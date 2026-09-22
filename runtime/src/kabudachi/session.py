"""One running worker's dealings with its own tasks: submitting them, running
what it claims, and delivering results only once the leader has certified them.

A result travels in two steps. The worker hands the client its bytes, which
are only provisional, and separately asks the leader to certify them by their
digest. The handle is settled when the leader's certification arrives and
matches the bytes the client holds; a result the leader refuses is never
delivered (README §8.5).
"""

import asyncio
import concurrent.futures
import contextvars
import functools
import hashlib
import inspect
import json
import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Protocol, runtime_checkable

from kabudachi.body import RunningBody, run_within, start_body
from kabudachi.composites import Composites, FlowHandle, GroupHandle
from kabudachi.config import Configuration
from kabudachi.errors import (
    CertificationError,
    RunStoppedError,
    RuntimeNotStartedError,
    TaskCancelledError,
    TaskDefinitionError,
    TaskExpiredError,
    TaskSupersededError,
    UnknownTaskError,
    interrupted,
)
from kabudachi.config import UNSET
from kabudachi.handle import TaskHandle, current_body, run_callback_inline
from kabudachi.lifecycle import RetryOutcome, TaskLifecycle
from kabudachi.options import SubmissionOptions
from kabudachi.registry import TaskDefinition, TaskKind, TaskRegistry
from kabudachi.serializers import SerializerRegistry

WAIT_POLL_SECONDS = 0.005
SLOW_DOWN_POLL_SECONDS = 0.05

_logger = logging.getLogger("kabudachi")


class Claim(Protocol):
    """A task run handed to this worker to execute."""

    task_id: str
    task_run_id: str
    definition_id: str
    source_version: int
    queue: str
    attempt_number: int
    serialized_input: bytes
    chain: list[bytes]


class Event(Protocol):
    """Something the runtime decided on its own, such as that a task expired."""

    kind: str
    task_id: str
    task_run_id: str
    was_running: bool
    superseded_by: str | None


class Certification(Protocol):
    """The leader's word that a run's result is the authoritative one."""

    task_id: str
    task_run_id: str
    result_digest: bytes


@runtime_checkable
class Runtime(Protocol):
    """What a session needs of the native runtime."""

    def submit(
        self,
        definition_id: str,
        source_version: int,
        serialized_input: bytes,
        queue: str,
        retries: int = 0,
        delay_ms: int | None = None,
        expires_in_ms: int | None = None,
        coalescing_key: str | None = None,
        drop_oldest: bool = False,
    ) -> str:
        ...

    async def claim_pending(self, limit: int) -> list[Claim]:
        ...

    async def next_events(self) -> list[Event]:
        ...

    def report_started(self, task_run_id: str) -> None:
        ...

    def cancel(self, task_id: str) -> str:
        """Cancels a task: `"cancelled"`, `"finished"` or `"unknown"`."""

    def fail(self, task_run_id: str, failure_kind: str) -> bool:
        """Reports that a running run failed, and whether it will be retried."""

    def complete(
        self, task_run_id: str, result_digest: bytes, continues: bool = False
    ) -> Certification:
        ...

    def end_continuation(self, task_id: str) -> bool:
        """Ends the continuation of a task that returned a step; whether there was one."""


def _digest(result: bytes) -> bytes:
    return hashlib.sha256(result).digest()


class _StartingContinuation:
    """Stands for a continuation between its run being certified and its first
    stage being started, so a cancel in that window is not lost."""

    def __init__(self) -> None:
        self.cancel_requested = False

    def cancel(self) -> bool:
        """Remembers the cancel, for the continuation to act on once it exists."""
        self.cancel_requested = True
        return True


class _BodyWaits:
    """Tracks when one running task body is waiting for another task, so the
    session can count it as not occupying a place while it does."""

    def __init__(self, session: "Session") -> None:
        self._session = session
        self._waits = 0

    def waiting_started(self) -> None:
        """The body has started waiting for a task; its place is given up while it does."""
        self._waits += 1
        if self._waits == 1:
            self._session._body_blocked()

    def waiting_finished(self) -> None:
        """The body has stopped waiting for a task; it takes its place back."""
        self._waits -= 1
        if self._waits == 0:
            self._session._body_unblocked()


class Session:
    """The tasks of one running worker: what it submitted and what it runs."""

    def __init__(
        self,
        runtime: Runtime,
        registry: TaskRegistry,
        serializers: SerializerRegistry,
        configuration: Configuration,
    ) -> None:
        self._runtime = runtime
        self._registry = registry
        self._serializers = serializers
        self._configuration = configuration
        self.concurrency: int = configuration.resolve("concurrency")
        self._lock = threading.Lock()
        # One lifecycle per task not yet forgotten: those submitted here, and
        # a detached one for a run whose task was settled before it began.
        self._tasks: dict[str, TaskLifecycle] = {}
        self._running: set[asyncio.Task[None]] = set()
        self._blocked = 0
        # The loop this runs on: known now if built inside it (as `run()` does),
        # otherwise once the worker loops start.
        self._loop: asyncio.AbstractEventLoop | None = self._running_loop()
        self._callback_tasks: set[asyncio.Task[None]] = set()
        self._callbacks_outstanding = 0
        self._flows_outstanding = 0
        # Set while memory use is below the soft limit, when bulk submission may go on.
        self._below_soft_limit = asyncio.Event()
        self._below_soft_limit.set()
        # Every flow and group started here, and what runs them.
        self._composites = Composites(self)
        self._slot_freed = asyncio.Event()
        self._stopping = False
        # As many threads as tasks may run at once, or synchronous tasks
        # would queue behind the event loop's small default pool.
        self._threads = ThreadPoolExecutor(
            max_workers=self.concurrency, thread_name_prefix="kabudachi-task"
        )

    @staticmethod
    def _running_loop() -> asyncio.AbstractEventLoop | None:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return None

    def submit(
        self,
        definition: TaskDefinition,
        argument: Any,
        options: SubmissionOptions | None = None,
    ) -> TaskHandle:
        """Queues `definition` to run on `argument` and returns its handle.

        Safe to call from any thread. Raises `SerializationError`, before
        anything is queued, if `argument` is not what the task takes, and
        `RunStoppedError` once the run is stopping.
        """
        serializer = self._serializers.get(definition.serializer)
        payload = serializer.encode(argument, definition.input_type)
        queue = self._configuration.resolve("queue", definition.queue)
        return self._submit_raw(
            definition.name,
            payload,
            queue,
            definition.version,
            definition.retries,
            options or SubmissionOptions(),
            coalescing=definition.kind is TaskKind.COALESCING,
        )

    def _submit_raw(
        self,
        definition_name: str,
        payload: bytes,
        queue: str,
        version: int = 0,
        retries: int = 0,
        options: SubmissionOptions = SubmissionOptions(),
        coalescing: bool = False,
    ) -> TaskHandle:
        # Held across submitting and recording, so a worker that claims the
        # task at once cannot look for its handle before it exists.
        with self._lock:
            if self._stopping:
                raise RunStoppedError("the run is stopping, so no more tasks are accepted")
            task_id = self._runtime.submit(
                definition_name,
                version,
                payload,
                queue,
                retries,
                options.delay_ms,
                options.expires_in_ms,
                # A coalescing task always has a key: the default is "".
                (options.key or "") if coalescing else None,
            )
            handle = TaskHandle(task_id, self._cancel, self._run_callback)
            self._tasks[task_id] = TaskLifecycle(handle, definition_name)
        return handle

    async def work(self) -> None:
        """Claims pending tasks and runs them, up to the concurrency limit,
        until the runtime shuts down or the caller cancels it.

        `stop_claiming` stops new tasks from starting but does not wake a
        claim already waiting, so the caller ends this by cancelling it.

        This must not be cancelled while it waits for work unless the runtime
        is shutting down: a claim that is cancelled after the native side
        made it leaves those tasks claimed and never run. If the runtime
        fails, the error ends this coroutine, and whoever awaits it sees it.
        """
        self._loop = asyncio.get_running_loop()
        while not self._stopping:
            free = self.concurrency - (len(self._running) - self._blocked)
            if free <= 0:
                # No await since `free` was computed, so a slot freed from now
                # on is not missed by clearing here.
                self._slot_freed.clear()
                await self._slot_freed.wait()
                continue
            for claim in await self._runtime.claim_pending(free):
                self._start(claim)

    def submit_flow(self, flow: Any, *arguments: Any) -> FlowHandle:
        """Starts `flow`, whose stages run one after another, and returns its
        handle. Safe to call from any thread once the run is serving; raises
        `RuntimeNotStartedError` before that and `RunStoppedError` once the
        run is stopping."""
        return self._composites.submit_flow(flow, arguments[0] if arguments else UNSET)

    def submit_group(self, group: Any, previous: Any = UNSET) -> GroupHandle:
        """Starts every member of `group` on `previous`, and returns the
        group's handle. Safe to call from any thread once the run is serving."""
        return self._composites.submit_group(group, previous)

    @property
    def stopping(self) -> bool:
        """Whether the run is stopping, so nothing new may be submitted."""
        return self._stopping

    @property
    def callback_runner(self) -> Callable[[Callable[[Any], Any], Any], None]:
        """What a handle made by this session runs its callbacks with."""
        return self._run_callback

    def has_room(self) -> bool:
        """Whether there is room to submit in bulk now: the scheduler has not
        said memory use is past its soft limit."""
        return self._below_soft_limit.is_set()

    async def wait_for_room(self) -> None:
        """Waits for room to submit in bulk, for one polling interval at most,
        so a caller submitting many tasks can look at its own reasons to give
        up between waits."""
        try:
            await asyncio.wait_for(self._below_soft_limit.wait(), SLOW_DOWN_POLL_SECONDS)
        except TimeoutError:
            pass

    def run_on_loop(self, coroutine: Any, when_done: Callable[[], None]) -> None:
        """Runs `coroutine` on the run's event loop, in a context of its own,
        and calls `when_done` when it ends, however it ends. Counted as
        outstanding from now, so finishing cannot miss it. Safe to call from
        any thread.

        Raises `RuntimeNotStartedError` if the run is not serving, and
        `RunStoppedError` once it is stopping; the coroutine is closed and
        `when_done` is not called in either case.
        """
        loop = self._loop
        if loop is None:
            # Called on the loop's own thread before the worker loops started.
            try:
                loop = self._loop = asyncio.get_running_loop()
            except RuntimeError:
                pass
        if loop is None or loop.is_closed():
            coroutine.close()
            raise RuntimeNotStartedError(
                "flows and groups can only be started while kabudachi.run() is serving"
            )
        with self._lock:
            if self._stopping:
                coroutine.close()
                raise RunStoppedError("the run is stopping, so no more flows are accepted")
            self._flows_outstanding += 1
        # Its own context, so awaiting its steps is not counted as the
        # calling task body waiting.
        try:
            loop.call_soon_threadsafe(self._spawn, coroutine, contextvars.Context(), when_done)
        except RuntimeError:
            coroutine.close()
            self._flow_done()
            raise RuntimeNotStartedError("the run's event loop has closed") from None

    def _spawn(
        self,
        coroutine: Any,
        context: contextvars.Context,
        when_done: Callable[[], None],
    ) -> None:
        task = asyncio.get_running_loop().create_task(coroutine, context=context)
        self._callback_tasks.add(task)
        task.add_done_callback(self._callback_tasks.discard)
        task.add_done_callback(lambda _: self._orchestration_ended(when_done))

    def _orchestration_ended(self, when_done: Callable[[], None]) -> None:
        """A flow or group's orchestration ended, however it did: its owner
        settles whatever it left unsettled, and it stops counting as
        outstanding whether that worked or not."""
        try:
            when_done()
        finally:
            self._flow_done()

    def _flow_done(self) -> None:
        with self._lock:
            self._flows_outstanding -= 1

    def _cancel(self, task_id: str) -> bool:
        """Asks the runtime to cancel `task_id`, which then tells this session
        through an event; says whether it was cancelled."""
        with self._lock:
            lifecycle = self._tasks.get(task_id)
            continuation = lifecycle.continuation if lifecycle is not None else None
            if continuation is None:
                # Under the lock, so a completion that carries a continuation
                # is either not accepted yet (this cancels the run) or already
                # has its continuation registered (found above).
                cancelled = self._runtime.cancel(task_id) == "cancelled"
                if cancelled and lifecycle is not None:
                    lifecycle.request_cancel()
                return cancelled
        # The run is certified and over; what can still be cancelled is its
        # continuation, outside the lock because cancelling it takes the lock.
        return continuation.cancel()

    async def watch_events(self) -> None:
        """Acts on what the runtime decides on its own: a task that expired
        fails its handle with `TaskExpiredError`. Runs until the runtime shuts
        down or the caller cancels it, which must only happen at shutdown
        because events the runtime has handed over but this has not acted on
        are lost. If the runtime fails, the error ends this coroutine."""
        self._loop = asyncio.get_running_loop()
        while True:
            for event in await self._runtime.next_events():
                if event.kind == "expired":
                    self._failed(
                        event.task_id,
                        TaskExpiredError(f"task {event.task_id} expired before it could start"),
                    )
                elif event.kind == "cancelled":
                    self._cancelled_by_leader(event.task_id)
                elif event.kind == "slow_down":
                    self._below_soft_limit.clear()
                elif event.kind == "slow_down_cleared":
                    self._below_soft_limit.set()
                elif event.kind == "superseded":
                    self._failed(
                        event.task_id,
                        TaskSupersededError(
                            f"task {event.task_id} was superseded by {event.superseded_by}",
                            event.superseded_by,
                        ),
                    )

    async def serve(self) -> None:
        """Runs `work` and `watch_events` together, until either ends. The
        error of whichever fails is raised, and the other is cancelled."""
        loops = [asyncio.ensure_future(self.work()), asyncio.ensure_future(self.watch_events())]
        try:
            await asyncio.wait(loops, return_when=asyncio.FIRST_COMPLETED)
            for loop in loops:
                if loop.done():
                    loop.result()
        finally:
            for loop in loops:
                loop.cancel()
            await asyncio.gather(*loops, return_exceptions=True)

    def _cancelled_by_leader(self, task_id: str) -> None:
        """The task was cancelled: fail its handle, and ask its body, if it is
        running here, to stop."""
        with self._lock:
            lifecycle = self._tasks.get(task_id)
            if lifecycle is not None:
                lifecycle.cancelled_by_leader()
        self._failed(task_id, TaskCancelledError(f"task {task_id} was cancelled"))
        body = lifecycle.body if lifecycle is not None else None
        if body is not None:
            body.outcome.cancel()

    def stop_claiming(self) -> None:
        """Stops the run: no more tasks start or are accepted, and every task
        that has not started fails with `RunStoppedError`, which also frees
        any running task that was waiting for one of them. Tasks already
        running are not interrupted."""
        with self._lock:
            self._stopping = True
            dropped = []
            for task_id, lifecycle in list(self._tasks.items()):
                if lifecycle.stop_unstarted():
                    dropped.append(lifecycle.handle)
                    self._retire(task_id, lifecycle)
        for handle in dropped:
            handle._fail(RunStoppedError("the run stopped before this task could start"))
        self._slot_freed.set()

    def close(self) -> None:
        """Releases the threads that ran synchronous tasks."""
        self._threads.shutdown(wait=False, cancel_futures=True)

    async def wait_until_running_finish(self) -> None:
        """Waits for every task that is running now to finish."""
        while self._running:
            await asyncio.wait(set(self._running))

    async def wait_until_idle(self) -> None:
        """Waits until every task submitted so far, including any submitted by
        those tasks, has finished, and so have their flows and callbacks."""
        while self._has_pending():
            await asyncio.sleep(WAIT_POLL_SECONDS)

    def _has_pending(self) -> bool:
        with self._lock:
            return (
                any(not lifecycle.settled for lifecycle in self._tasks.values())
                or self._callbacks_outstanding > 0
                or self._flows_outstanding > 0
            )

    def _run_callback(self, function: Any, value: Any) -> None:
        """Runs a task callback on the event loop, from whichever thread the
        task was settled on. Counted as outstanding from now, so finishing
        cannot miss one that has been asked for but not yet started."""
        loop = self._loop
        if loop is None or loop.is_closed():
            run_callback_inline(function, value)
            return
        with self._lock:
            self._callbacks_outstanding += 1
        try:
            loop.call_soon_threadsafe(self._start_callback, function, value)
        except RuntimeError:
            # The loop closed between looking and asking.
            self._callback_done()
            run_callback_inline(function, value)

    def _start_callback(self, function: Any, value: Any) -> None:
        task = asyncio.get_running_loop().create_task(self._invoke_callback(function, value))
        # Held here, so a callback whose handle was dropped still runs.
        self._callback_tasks.add(task)
        task.add_done_callback(self._callback_tasks.discard)

    def _callback_done(self) -> None:
        with self._lock:
            self._callbacks_outstanding -= 1

    async def _invoke_callback(self, function: Any, value: Any) -> None:
        try:
            if inspect.iscoroutinefunction(function):
                await function(value)
            else:
                # Off the loop, so a callback that blocks does not stall tasks.
                outcome = await asyncio.get_running_loop().run_in_executor(None, function, value)
                if inspect.isawaitable(outcome):
                    await outcome
        except Exception as error:
            _logger.warning("a task callback failed with %s", type(error).__name__)
        finally:
            self._callback_done()

    def _body_blocked(self) -> None:
        # A handle can be awaited from any event loop, so this can run off the
        # session's loop: count under the lock and wake the worker loop on its own.
        with self._lock:
            self._blocked += 1
        loop = self._loop
        if loop is None:
            self._slot_freed.set()
            return
        try:
            loop.call_soon_threadsafe(self._slot_freed.set)
        except RuntimeError:
            pass  # the loop is closed, so there is no worker loop to wake

    def _body_unblocked(self) -> None:
        with self._lock:
            self._blocked -= 1

    def _start(self, claim: Any) -> None:
        with self._lock:
            lifecycle = self._tasks.get(claim.task_id)
            if lifecycle is None or not lifecycle.claim():
                # Already settled and forgotten (by `stop_claiming`, or any
                # other route): its handle is settled, so running the body now
                # would contradict that.
                return
        running = asyncio.get_running_loop().create_task(self._run(claim))
        self._running.add(running)
        running.add_done_callback(self._finished)

    def _finished(self, running: asyncio.Task[None]) -> None:
        self._running.discard(running)
        # Whatever ended the task already reached its handle; retrieving it
        # here keeps asyncio from logging it as never retrieved.
        if not running.cancelled():
            running.exception()
        self._slot_freed.set()

    async def _run(self, claim: Any) -> None:
        """Runs one claimed task and settles its handle, whatever happens.

        A task that fails is reported to the leader, by its error's type,
        before its handle is failed with the error itself.
        """
        current_body.set(_BodyWaits(self))
        lifecycle, generation = self._begin_run(claim.task_id, claim.definition_id)
        started = False
        body: RunningBody | None = None
        try:
            # A retry does not run beside the abandoned body it replaces.
            previous = lifecycle.abandoned
            if previous is not None:
                await asyncio.wait({previous})
            definition = self._registry.get(claim.definition_id)
            if definition is None:
                raise UnknownTaskError(f"this process has no task named {claim.definition_id!r}")
            serializer = self._serializers.get(definition.serializer)
            self._runtime.report_started(claim.task_run_id)
            started = True
            argument = self._fold(definition, serializer, claim)
            body = start_body(definition, argument, self._threads)
            lifecycle.body = body
            value = await run_within(
                body,
                definition.timeout,
                self._configuration.resolve("cancel_grace", definition.cancel_grace),
            )
            if not lifecycle.outcome_counts:
                # Cancelled, though the body carried on and returned anyway.
                # The handle stays unsettled here on purpose: the leader's
                # `cancelled` event settles it, pushed in the same call that
                # answers "cancelled".
                return
            if definition.continues:
                self._continue(claim, lifecycle, serializer, value)
                return
            result = serializer.encode(value, definition.output_type)
            self._hold_provisionally(claim.task_id, result)
            certification = self._runtime.complete(claim.task_run_id, _digest(result))
            self._certified(certification)
        except asyncio.CancelledError as error:
            current = asyncio.current_task()
            if lifecycle.cancelled and current is not None and not current.cancelling():
                # Only the body was cancelled, because the task was: this is
                # not this run being interrupted, and its handle is settled.
                return
            self._interrupted(claim.task_id, error)
            raise
        except Exception as error:
            if not lifecycle.outcome_counts:
                # Cancelled, so its own failure no longer counts. The handle
                # stays unsettled here on purpose: the leader's `cancelled`
                # event settles it, pushed in the same call that answers
                # "cancelled".
                return
            _logger.warning("task %s failed with %s", claim.task_id, type(error).__name__)
            # Only here, at DEBUG, does the error's own message (which can hold
            # task input) reach the log; the warning above names its type only.
            _logger.debug("task %s failed", claim.task_id, exc_info=error)
            abandoned = body is not None and not body.exited.done()
            if abandoned:
                lifecycle.abandoned = body.exited
            if self._report_failure(claim, error, started):
                self._awaiting_retry(claim.task_id)
            else:
                self._failed(claim.task_id, error)
            if abandoned:
                # It keeps its place until it has really stopped.
                await asyncio.wait({body.exited})
                lifecycle.abandoned = None
        except BaseException as error:
            # Worse than cancelled: the task has no result, and whoever waits
            # for it must not wait forever.
            self._interrupted(claim.task_id, error)
            raise
        finally:
            # On the abandoned-body retry path a newer run of this task can
            # begin while this one still awaits `body.exited`. `generation`
            # is this run's own token, so if a newer run has already begun,
            # `end_run` is a no-op here and leaves the newer run's state alone.
            with self._lock:
                lifecycle.end_run(generation)
                self._retire(claim.task_id, lifecycle)

    def _interrupted(self, task_id: str, error: BaseException) -> None:
        self._failed(task_id, interrupted(f"task {task_id}", error))

    def _continue(
        self, claim: Any, lifecycle: TaskLifecycle, serializer: Any, step: Any
    ) -> None:
        """The task returned `step`: certify the run, by the digest of the step,
        and only then start the step as the task's continuation, so a run the
        leader does not certify leaves no continuation behind (README §3.4).
        The task is over, and its handle settled, when the continuation is."""
        if not getattr(step, "is_step", False):
            raise TaskDefinitionError(
                f"task {claim.definition_id} is declared to return a flow, group or bound task, "
                f"not {type(step).__name__}"
            )
        if step.needs_input:
            raise TaskDefinitionError(
                "a returned flow, group or bound task must not need an input: "
                "bind its first stage"
            )
        for definition in step.definitions():
            if self._registry.get(definition.name) is None:
                raise UnknownTaskError(f"this process has no task named {definition.name!r}")
        digest = _digest(json.dumps(step.describe(self._serializers), sort_keys=True).encode())
        starting = _StartingContinuation()
        with self._lock:
            self._runtime.complete(claim.task_run_id, digest, True)
            # Registered in the same step as the certification, so a cancel of
            # the task's handle always finds the continuation, or the run.
            lifecycle.continuation = starting
        # Certified: from here on nothing may fail the run, only the continuation.
        try:
            handle = step.start(self, UNSET)
        except Exception as error:
            self._continuation_over(claim.task_id, error, None)
            return
        with self._lock:
            lifecycle.continuation = handle
        if starting.cancel_requested:
            handle.cancel()  # cancelled while the first stage was being started

        def settled(outcome: "concurrent.futures.Future[Any]") -> None:
            error = outcome.exception()
            value = None if error is not None else outcome.result()
            self._continuation_over(claim.task_id, error, [value] if step.is_task else value)

        handle._outcome.add_done_callback(settled)

    def _continuation_over(self, task_id: str, error: BaseException | None, result: Any) -> None:
        """The continuation of `task_id` ended: end it for the leader too, and
        settle the task's handle with its results or its failure."""
        with self._lock:
            lifecycle = self._tasks.get(task_id)
            if lifecycle is not None:
                lifecycle.continuation = None
                # Forgets the task if this was the last thing keeping it: a
                # no-op while its run is still active, where `_run`'s
                # `finally` forgets it instead.
                self._retire(task_id, lifecycle)
        try:
            self._runtime.end_continuation(task_id)
        except Exception as refusal:
            _logger.warning(
                "the leader did not end the continuation of task %s: %s",
                task_id,
                type(refusal).__name__,
            )
        handle = self._settle(task_id)
        if handle is None:
            return
        if error is not None:
            handle._fail(error)
            return
        handle._resolve(result)

    @staticmethod
    def _fold(definition: TaskDefinition, serializer: Any, claim: Any) -> Any:
        """The input of the task: its own payload, folded onto the payloads of
        the generations it superseded, oldest first (README §3.2.1). Runs
        here, on the worker, because the leader never runs user code."""
        payloads = [*claim.chain, claim.serialized_input]
        values = [serializer.decode(payload, definition.input_type) for payload in payloads]
        if len(values) == 1:
            return values[0]
        merge = definition.merge or (lambda older, newer: newer)
        return functools.reduce(merge, values)

    def _report_failure(self, claim: Any, error: Exception, started: bool) -> bool:
        """Tells the leader the run failed and says whether it will be retried.
        Only the error's type name goes, never its message. A refusal is
        logged, counts as no retry, and does not replace `error`, which is
        what the handle's awaiter needs to see."""
        try:
            if not started:
                # A run has to be running before it can fail.
                self._runtime.report_started(claim.task_run_id)
            return self._runtime.fail(claim.task_run_id, type(error).__name__)
        except Exception as refusal:
            _logger.warning(
                "the leader did not record the failure of task %s: %s",
                claim.task_id,
                type(refusal).__name__,
            )
            return False

    def _awaiting_retry(self, task_id: str) -> None:
        """The task's next attempt is queued: its handle stays open, and it
        has not started again, so stopping fails it like any unstarted task."""
        with self._lock:
            lifecycle = self._tasks.get(task_id)
            if lifecycle is None:
                return
            outcome = lifecycle.retry_queued(self._stopping)
            if outcome is not RetryOutcome.STOPPED:
                return
            # `stop_claiming` had already failed everything not started,
            # and nothing will claim this retry.
            self._retire(task_id, lifecycle)
        lifecycle.handle._fail(
            RunStoppedError("the run stopped before this task's retry could start")
        )

    def _hold_provisionally(self, task_id: str, result: bytes) -> None:
        with self._lock:
            lifecycle = self._tasks.get(task_id)
            if lifecycle is not None:
                lifecycle.hold(result)

    def _certified(self, certification: Any) -> None:
        with self._lock:
            lifecycle = self._tasks.get(certification.task_id)
            if lifecycle is None or not lifecycle.settle():
                return
            self._retire(certification.task_id, lifecycle)
        result = lifecycle.provisional_result
        if result is None or _digest(result) != certification.result_digest:
            _logger.warning(
                "the leader certified a different result for %s", certification.task_id
            )
            lifecycle.handle._fail(
                CertificationError(
                    f"the leader certified a different result for task {certification.task_id}"
                )
            )
            return
        definition = self._registry.get(lifecycle.definition_name)
        try:
            serializer = self._serializers.get(definition.serializer)
            value = serializer.decode(result, definition.output_type)
        except Exception as error:
            lifecycle.handle._fail(error)
            return
        lifecycle.handle._resolve(value)

    def _failed(self, task_id: str, error: BaseException) -> None:
        handle = self._settle(task_id)
        if handle is not None:
            handle._fail(error)

    def _settle(self, task_id: str) -> TaskHandle | None:
        """Settles the task, once: returns its handle if this call did, so the
        caller can fail or resolve it outside the lock, and `None` if it was
        settled already or is not known."""
        with self._lock:
            lifecycle = self._tasks.get(task_id)
            if lifecycle is None or not lifecycle.settle():
                return None
            self._retire(task_id, lifecycle)
            return lifecycle.handle

    def _retire(self, task_id: str, lifecycle: TaskLifecycle) -> None:
        """Forgets a settled task nothing of which is still going. Call with the lock held."""
        if lifecycle.retirable:
            self._tasks.pop(task_id, None)

    def _begin_run(self, task_id: str, definition_name: str) -> tuple[TaskLifecycle, int]:
        """Marks a run of the task as in progress and returns its lifecycle
        and this run's generation token (pass it to `lifecycle.end_run`). A
        task settled between its claim and this run's first step is no longer
        known, so it is given a detached lifecycle, kept until the run ends, so
        a cancel notice for it still finds the running body."""
        with self._lock:
            lifecycle = self._tasks.get(task_id)
            if lifecycle is None:
                lifecycle = self._tasks[task_id] = TaskLifecycle.detached(definition_name)
            generation = lifecycle.begin_run()
            return lifecycle, generation


def validate_definitions(registry: TaskRegistry, serializers: SerializerRegistry) -> None:
    """Checks that every registered task can be run with the serializers this
    process has, so a mistake is reported before any task runs rather than by
    the first task that meets it.

    Raises `TaskDefinitionError` naming every task that cannot work.
    """
    problems = []
    for definition in registry.definitions():
        serializer = serializers.find(definition.serializer)
        if serializer is None:
            problems.append(
                f"{definition.name}: no serializer named {definition.serializer!r} is registered"
            )
        elif not serializer.available():
            problems.append(
                f"{definition.name}: the {definition.serializer!r} serializer cannot run here"
            )
        else:
            checked = [("input", definition.input_type)]
            if not definition.continues:  # a returned step is not encoded
                checked.append(("return", definition.output_type))
            for role, value_type in checked:
                if not serializer.supports(value_type):
                    problems.append(
                        f"{definition.name}: the {definition.serializer!r} serializer "
                        f"does not support the {role} type {value_type!r}"
                    )
    if problems:
        raise TaskDefinitionError("; ".join(problems))


_active_session: Session | None = None
_active_lock = threading.Lock()


def active_session() -> Session | None:
    """The session of the `run()` in progress in this process, if any."""
    return _active_session


def current_session() -> Session:
    """The session of the `run()` in progress. Raises `RuntimeNotStartedError`
    if none is running."""
    session = _active_session
    if session is None:
        raise RuntimeNotStartedError(
            "tasks can only be submitted while kabudachi.run() is running"
        )
    return session


def activate(session: Session) -> None:
    """Makes `session` the one tasks are submitted to. Only one can be active."""
    global _active_session
    with _active_lock:
        if _active_session is not None:
            raise RuntimeError("kabudachi.run() is already running in this process")
        _active_session = session


def deactivate(session: Session) -> None:
    """Ends `session` as the active session, if it still is."""
    global _active_session
    with _active_lock:
        if _active_session is session:
            _active_session = None
