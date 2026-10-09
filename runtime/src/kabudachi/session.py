"""One running worker's dealings with its own tasks: submitting them, running
what it claims, and delivering results only once the leader has certified them.

A result travels in two steps. The worker hands the client its bytes, which
are only provisional, and separately asks the leader to certify them by their
digest. The handle is settled when the leader's certification arrives and
matches the bytes the client holds; a result the leader refuses is never
delivered.

A coalescing key whose waiting payloads pile up past the soft memory limit
while one generation runs is compacted on a free worker place: the claim names
the payloads to fold, the executor folds them with the task's merge and hands the
folded payload to the leader, and no handle waits for it.

A worker of a networked shard runs whatever the shard's leader hands it,
whoever submitted the task, and its reports go to that leader: no
certification comes back, so its runs settle no handle. A task it submits is
forgotten once the leader has stored it; awaiting its handle raises
`RemoteResultUnavailableError`, and cancelling it asks the leader.
"""

import asyncio
import concurrent.futures
import functools
import inspect
import json
import logging
import math
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from kabudachi._native import EventKind
from kabudachi.body import RunningBody
from kabudachi.composites import Composites
from kabudachi.config import Configuration
from kabudachi.errors import (
    BackpressureError,
    CoalescedPayloadTooLargeError,
    RemoteResultUnavailableError,
    RuntimeNotStartedError,
    TaskBodyError,
    TaskDefinitionError,
    TaskExpiredError,
    TaskLostError,
    TaskRecordFullError,
    TaskSupersededError,
    UnknownTaskError,
    interrupted,
)
from kabudachi.config import UNSET
from kabudachi.execution import (
    CompactJob,
    Executor,
    InProcessExecutor,
    RunJob,
    TaskProcessLost,
    run_within,
)
from kabudachi.hosted_work import LoopHostedWork
from kabudachi.handle import TaskHandle, run_callback_inline
from kabudachi.lifecycle import default_hooks
from kabudachi.native_protocol import Runtime
from kabudachi.options import SubmissionOptions
from kabudachi.registry import TaskDefinition, TaskRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.task_table import Run, TaskTable, result_digest

WAIT_POLL_SECONDS = 0.005
SLOW_DOWN_POLL_SECONDS = 0.05

_logger = logging.getLogger("kabudachi")


@dataclass
class _HeldHere:
    """A run a networked worker holds for its shard, and the abort timers it is under."""

    run: Run
    cancel_grace: timedelta
    abort: list[asyncio.TimerHandle] = field(default_factory=list)


class Session:
    """The tasks of one running worker: what it submitted and what it runs."""

    def __init__(
        self,
        runtime: Runtime,
        registry: TaskRegistry,
        serializers: SerializerRegistry,
        configuration: Configuration,
        executor: Executor | None = None,
    ) -> None:
        self._runtime = runtime
        self._registry = registry
        self._serializers = serializers
        self._configuration = configuration
        self.concurrency: int = configuration.resolve("concurrency")
        self._tasks = TaskTable(runtime, self._run_callback, self._decode_result)
        self._hosted = LoopHostedWork()
        # Where bodies run, and the places they take: this process, unless
        # the caller hands over task processes.
        self._executor = (
            executor
            if executor is not None
            else InProcessExecutor(registry, serializers, self.concurrency, default_hooks())
        )
        self._executor.accept_nested_calls(self)
        # Every run and compaction this session sees through to its end.
        self._running: set[asyncio.Task[None]] = set()
        # Runs held for the shard, by run id: they settle no handle, so the
        # table does not keep them.
        self._here: dict[str, _HeldHere] = {}
        try:
            # Built inside the run's loop, as `run()` does.
            self._hosted.attach(asyncio.get_running_loop())
        except RuntimeError:
            pass
        # Set while memory use is below the soft limit, when bulk submission may go on.
        self._below_soft_limit = asyncio.Event()
        self._below_soft_limit.set()
        # Every flow and group started here, and what runs them.
        self._composites = Composites(self, self._hosted)

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
        return self._record(definition, payload, options or SubmissionOptions())

    def _record(
        self, definition: TaskDefinition, payload: bytes, options: SubmissionOptions
    ) -> TaskHandle:
        queue = self._configuration.resolve("queue", definition.queue)
        return self._tasks.submit(
            definition,
            payload,
            queue,
            options,
            reconnect_timeout_ms=self._reconnect_timeout_ms(definition, queue),
        )

    def _reconnect_timeout_ms(self, definition: TaskDefinition, queue: str) -> int | None:
        """The reconnect timeout a run of `definition` sent to `queue`
        carries, in whole milliseconds rounded up, and at least one, so it
        is never replayed early: the task's own, else the one configured for
        the queue. `None` leaves it to the shard."""
        timeout = definition.reconnect_timeout
        if timeout is None:
            seconds = self._configuration.resolve("reconnect_timeouts").get(queue)
            if seconds is None:
                return None
            timeout = timedelta(seconds=seconds)
        return max(1, math.ceil(timeout / timedelta(milliseconds=1)))

    def submit_serialized(
        self, definition_id: str, payload: bytes, options: SubmissionOptions
    ) -> TaskHandle:
        """Submits a task a body in a task process called, its input already
        encoded there. Raises as `submit` does, and `UnknownTaskError` for a
        task this process does not have."""
        definition = self._registry.get(definition_id)
        if definition is None:
            raise UnknownTaskError(f"this process has no task named {definition_id!r}")
        return self._record(definition, payload, options)

    def submit_composite(self, kind: str, composite: Any, previous: Any) -> TaskHandle:
        """Starts a flow (`kind` "flow") or group a body in a task process called."""
        if kind == "flow":
            return self._composites.submit_flow(composite, previous)
        return self._composites.submit_group(composite, previous)

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
        self._hosted.attach(asyncio.get_running_loop())
        while not self._tasks.stopping:
            free = self._executor.free()
            if free <= 0:
                await self._executor.wait_for_free()
                continue
            for claim in await self._runtime.claim_pending(free):
                self._start(claim)

    @property
    def runtime(self) -> Runtime:
        """The native runtime this session drives."""
        return self._runtime

    @property
    def stopping(self) -> bool:
        """Whether the run is stopping, so nothing new may be submitted."""
        return self._tasks.stopping

    @property
    def composites(self) -> Composites:
        """Every flow and group started here, and what runs them."""
        return self._composites

    @property
    def tasks(self) -> TaskTable:
        """The table of every task this session submitted and has not forgotten."""
        return self._tasks

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

    async def watch_events(self) -> None:
        """Acts on what the runtime decides on its own: a task that expired
        fails its handle with `TaskExpiredError`. Runs until the runtime shuts
        down or the caller cancels it, which must only happen at shutdown
        because events the runtime has handed over but this has not acted on
        are lost. If the runtime fails, the error ends this coroutine."""
        self._hosted.attach(asyncio.get_running_loop())
        while True:
            for event in await self._runtime.next_events():
                match event.kind:
                    case EventKind.EXPIRED:
                        self._tasks.failed(
                            event.task_id,
                            TaskExpiredError(
                                f"task {event.task_id} expired before it could start"
                            ),
                        )
                    case EventKind.CANCELLED:
                        here = self._here.get(event.task_run_id)
                        if here is None:
                            self._tasks.cancelled_by_leader(event.task_id)
                        else:
                            here.run.cancel_by_leader()
                            self._executor.cancel(event.task_run_id)
                    case EventKind.RECORD_FULL:
                        self._tasks.failed(
                            event.task_id,
                            TaskRecordFullError(
                                f"task {event.task_id} ended: its record had no room for another attempt"
                            ),
                        )
                    case EventKind.COALESCED_PAYLOAD_TOO_LARGE:
                        self._tasks.failed(
                            event.task_id,
                            CoalescedPayloadTooLargeError(
                                f"task {event.task_id}'s folded payloads grew too large to run"
                            ),
                        )
                    case EventKind.SLOW_DOWN if event.active:
                        self._below_soft_limit.clear()
                    case EventKind.SLOW_DOWN:
                        self._below_soft_limit.set()
                    case EventKind.SUPERSEDED:
                        self._tasks.failed(
                            event.task_id,
                            TaskSupersededError(
                                f"task {event.task_id} was superseded by {event.superseded_by}",
                                event.superseded_by,
                            ),
                        )
                    case EventKind.ACCEPTED:
                        # Stored, and its result never comes back here: its
                        # handle stays open for `cancel`, and nothing waits for it.
                        self._tasks.stored(event.task_id)
                    case EventKind.REFUSED:
                        self._tasks.failed(
                            event.task_id,
                            BackpressureError(
                                f"the leader refused task {event.task_id}: {event.reason}"
                            ),
                        )
                    case EventKind.ABORT:
                        self._abort_at(event.task_run_id, event.seconds_left)
                    case EventKind.ABORT_WITHDRAWN:
                        self._clear_abort(event.task_run_id)
                    case unknown:
                        raise RuntimeError(
                            f"the native runtime reported an event of unknown kind {unknown!r}"
                        )

    def _abort_at(self, run_id: str, seconds_left: float) -> None:
        """The run may be run again elsewhere `seconds_left` from now: its body
        is asked to stop its cancel grace before that, and its task process is
        killed then if it has not. A later deadline for the run replaces this one."""
        here = self._here.get(run_id)
        if here is None:
            return
        self._clear_abort(run_id)
        loop = asyncio.get_running_loop()
        stop_in = max(0.0, seconds_left - here.cancel_grace.total_seconds())
        here.abort = [
            loop.call_later(stop_in, self._abort, here, run_id),
            loop.call_later(seconds_left, self._executor.kill_host_of, run_id),
        ]

    def _abort(self, here: _HeldHere, run_id: str) -> None:
        here.run.abort()
        self._executor.cancel(run_id)

    def _clear_abort(self, run_id: str) -> None:
        """The leader hears this worker again: a pending abort is dropped. A
        body already asked to stop is not asked to go on."""
        here = self._here.get(run_id)
        if here is not None:
            for timer in here.abort:
                timer.cancel()
            here.abort = []

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

    def stop_claiming(self) -> None:
        """Stops the run: no more tasks start or are accepted, and every task
        that has not started fails with `RunStoppedError`, which also frees
        any running task that was waiting for one of them. Tasks already
        running are not interrupted."""
        self._tasks.stop_unstarted()
        self._executor.wake()

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
        return self._tasks.pending() or self._hosted.outstanding > 0

    def _run_callback(self, function: Any, value: Any) -> None:
        """Runs a task callback on the event loop, from whichever thread the
        task was settled on; inline if there is no loop left to run it."""
        if not self._hosted.spawn(invoke_callback(function, value)):
            run_callback_inline(function, value)

    def _start(self, claim: Any) -> None:
        if claim.compaction:
            # Internal: no handle waits for it, so it is not in the task table.
            job = CompactJob(claim.task_run_id, claim.definition_id, tuple(claim.chain))
            self._track(self._compact(claim, self._executor.compact(job)))
            return
        if self._runtime.delivers_results:
            run = self._tasks.claimed(claim.task_id)
            if run is None:
                # Already settled and forgotten (by `stop_claiming`, or any other
                # route): its handle is settled, so running the body now would
                # contradict that.
                return
        else:
            # The shard's leader handed this worker the run, whoever
            # submitted the task: it runs, and settles no handle here.
            run = self._tasks.detached(claim.task_id)
            self._here[claim.task_run_id] = _HeldHere(run, self._configuration.resolve("cancel_grace"))
        handed = self._hand_over(claim, run)
        if not isinstance(handed, BaseException) and claim.task_run_id in self._here:
            self._here[claim.task_run_id].cancel_grace = self._cancel_grace(handed[0])
        self._track(self._run(claim, run, handed))

    def _hand_over(
        self, claim: Any, run: Run
    ) -> "tuple[TaskDefinition, RunningBody, asyncio.Future[Any] | None] | BaseException":
        """Tells the leader the run started and hands it to the executor. Done
        here rather than in `_run`, which starts later, so the place the run
        takes is counted before the next claim asks for more. What went wrong
        instead is returned, for `_run` to settle. The future is the abandoned
        body this one waits for, if any."""
        after = run.previous_body_exited
        try:
            definition = self._registry.get(claim.definition_id)
            if definition is None:
                raise UnknownTaskError(f"this process has no task named {claim.definition_id!r}")
            self._runtime.report_started(claim.task_run_id)
            body = self._executor.run(
                RunJob(
                    run_id=claim.task_run_id,
                    definition_id=claim.definition_id,
                    source_version=claim.source_version,
                    serialized_input=claim.serialized_input,
                    chain=tuple(claim.chain),
                    queue=claim.queue,
                    attempt=claim.attempt_number,
                    cancel_grace=self._cancel_grace(definition),
                    # A retry does not run beside the abandoned body it replaces.
                    after=after,
                )
            )
        except BaseException as error:
            return error
        run.body_started(body)
        return definition, body, after

    def _track(self, coroutine: Any) -> None:
        task = asyncio.get_running_loop().create_task(coroutine)
        self._running.add(task)
        task.add_done_callback(self._running.discard)

    def _cancel_grace(self, definition: TaskDefinition) -> Any:
        return self._configuration.resolve("cancel_grace", definition.cancel_grace)

    async def _run(
        self, claim: Any, run: Run, handed: "tuple[TaskDefinition, RunningBody, asyncio.Future[Any] | None] | BaseException"
    ) -> None:
        """Sees one handed-over run to its end and settles its handle,
        whatever happens.

        A task that fails is reported to the leader, by its error's type,
        before its handle is failed with the error itself.
        """
        body: RunningBody | None = None
        try:
            if isinstance(handed, BaseException):
                raise handed
            definition, body, previous = handed
            # The body waits for an abandoned one of this task to exit before
            # it starts; its timeout counts from when it does.
            if previous is not None:
                await asyncio.wait({previous})
            outcome = await run_within(
                body,
                definition.timeout,
                self._cancel_grace(definition),
                functools.partial(self._executor.condemn_host_of, claim.task_run_id),
            )
            if not run.outcome_counts:
                # Cancelled, though the body carried on and returned anyway.
                # The handle stays unsettled here on purpose: the leader's
                # `cancelled` event settles it, pushed in the same call that
                # answers "cancelled".
                return
            if definition.continues:
                self._continue(claim, run, outcome)
                return
            if run.holds_handle:
                self._tasks.result_held(claim.task_id, outcome)
            certification = self._runtime.complete(claim.task_run_id, result_digest(outcome))
            if certification is not None:
                self._tasks.certified(certification)
        except TaskProcessLost:
            # Cancelled first, the leader's `cancelled` event settles the
            # handle, as for a cancelled body that failed.
            if run.outcome_counts:
                self._lost(claim, run)
        except asyncio.CancelledError as error:
            current = asyncio.current_task()
            cancelling = current is not None and current.cancelling()
            if run.aborted and not cancelling:
                # Another leader may run it again once the deadline passes:
                # it is lost only once its body has really stopped.
                if body is not None:
                    await asyncio.wait({body.exited})
                self._report_lost_quietly(claim)
                return
            if run.cancelled_by_leader and current is not None and not cancelling:
                # Only the body was cancelled, because the task was: this is
                # not this run being interrupted, and its handle is settled.
                return
            if run.holds_handle:
                self._interrupted(claim.task_id, error)
            raise
        except Exception as error:
            if not run.outcome_counts:
                # Cancelled, so its own failure no longer counts. The handle
                # stays unsettled here on purpose: the leader's `cancelled`
                # event settles it, pushed in the same call that answers
                # "cancelled".
                return
            kind = _kind_of(error)
            _logger.warning("task %s failed with %s", claim.task_id, kind)
            # Only here, at DEBUG, does the error's own message (which can hold
            # task input) reach the log; the warning above names its type only.
            _logger.debug("task %s failed", claim.task_id, exc_info=error)
            abandoned = body is not None and not body.exited.done()
            if abandoned:
                run.body_abandoned(body)
                if not run.holds_handle:
                    self._tasks.held_body_abandoned(claim.task_id, body.exited)
            retried = self._report_failure(claim, kind)
            if run.holds_handle:
                if retried:
                    self._tasks.retry_queued(claim.task_id)
                else:
                    self._tasks.failed(claim.task_id, error)
            if abandoned:
                # It keeps its place until it has really stopped.
                await asyncio.wait({body.exited})
                run.abandoned_body_exited()
        except BaseException as error:
            # Worse than cancelled: the task has no result, and whoever waits
            # for it must not wait forever.
            if run.holds_handle:
                self._interrupted(claim.task_id, error)
            raise
        finally:
            if run.holds_handle:
                # On the abandoned-body retry path a newer run of this task can
                # begin while this one still awaits `body.exited`; the table
                # leaves the newer run's state alone when this stale one ends.
                self._tasks.run_ended(run)
            else:
                self._clear_abort(claim.task_run_id)
                self._here.pop(claim.task_run_id, None)

    def _interrupted(self, task_id: str, error: BaseException) -> None:
        self._tasks.failed(task_id, interrupted(f"task {task_id}", error))

    def _continue(self, claim: Any, run: Run, step: Any) -> None:
        """The task returned `step`: certify the run, by the digest of the step,
        and only then start the step as the task's continuation, so a run the
        leader does not certify leaves no continuation behind.
        The task is over, and its handle settled, when the continuation is."""
        if not self._runtime.delivers_results:
            raise RemoteResultUnavailableError(
                f"task {claim.definition_id} returned a flow, group or bound task, whose stages "
                "need results this worker cannot deliver: results are not yet sent back across "
                "workers",
                claim.task_id,
            )
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
        description = json.dumps(step.describe(self._serializers), sort_keys=True)
        digest = result_digest(description.encode())
        self._tasks.complete_with_continuation(run, claim.task_run_id, digest)
        # Certified: from here on nothing may fail the run, only the continuation.
        try:
            handle = step.start(self, UNSET)
        except Exception as error:
            self._tasks.continuation_over(claim.task_id, error, None)
            return
        self._tasks.continuation_started(run, handle)

        def settled(outcome: "concurrent.futures.Future[Any]") -> None:
            error = outcome.exception()
            value = None if error is not None else outcome.result()
            self._tasks.continuation_over(
                claim.task_id, error, [value] if step.is_task else value
            )

        handle._outcome.add_done_callback(settled)

    async def _compact(self, claim: Any, body: RunningBody) -> None:
        """Waits for a compaction run's fold and hands the folded payload to the
        leader. A failure is reported by the error's type only, and the leader
        then leaves the chain as it was."""
        try:
            folded = await body.outcome
        except Exception as error:
            kind = _kind_of(error)
            _logger.warning("compaction of %s failed with %s", claim.definition_id, kind)
            _logger.debug("compaction of %s failed", claim.definition_id, exc_info=error)
            try:
                self._runtime.report_failure(claim.task_run_id, kind)
            except Exception as refusal:
                _logger.warning(
                    "the leader did not record the failed compaction of %s: %s",
                    claim.definition_id,
                    type(refusal).__name__,
                )
            return
        try:
            self._runtime.complete_compaction(claim.task_run_id, folded)
        except Exception as refusal:
            # The merge was fine: the leader refused to take its result (the
            # run is no longer this worker's), which is no failure to report.
            _logger.warning(
                "the leader did not take the fold of %s: %s",
                claim.definition_id,
                type(refusal).__name__,
            )

    def _lost(self, claim: Any, run: Run) -> None:
        """The process running the body died. The leader decides, as for a
        lost worker: a new attempt (the handle waits for it), or the task is
        over (an ephemeral one, or a non-retriable one that may have had its
        effects). A refused report ends the task here too; failing a handle
        the leader already settled changes nothing."""
        _logger.warning("task %s was lost with the process that ran it", claim.task_id)
        try:
            replayed = self._runtime.report_lost(claim.task_run_id)
        except Exception as refusal:
            _logger.warning(
                "the leader did not record the loss of task %s: %s",
                claim.task_id,
                type(refusal).__name__,
            )
            replayed = False
        if not run.holds_handle:
            return
        if replayed:
            self._tasks.retry_queued(claim.task_id)
        else:
            self._tasks.failed(
                claim.task_id,
                TaskLostError(f"task {claim.task_id} was lost with the process that ran it"),
            )

    def _report_lost_quietly(self, claim: Any) -> None:
        try:
            self._runtime.report_lost(claim.task_run_id)
        except Exception as refusal:
            _logger.warning(
                "the leader was not told task %s's run stopped at its abort deadline: %s",
                claim.task_id,
                type(refusal).__name__,
            )

    def _report_failure(self, claim: Any, kind: str) -> bool | None:
        """Tells the leader the run failed and says whether it will be retried.
        Only the failure's kind goes, never the error's message. A refusal is
        logged, counts as no retry, and does not replace the error, which is
        what the handle's awaiter needs to see."""
        try:
            return self._runtime.report_failure(claim.task_run_id, kind)
        except Exception as refusal:
            _logger.warning(
                "the leader did not record the failure of task %s: %s",
                claim.task_id,
                type(refusal).__name__,
            )
            return False

    def _decode_result(self, definition_name: str, payload: bytes) -> Any:
        """Decodes a certified result with the serializer and output type of the
        task named `definition_name`."""
        definition = self._registry.get(definition_name)
        serializer = self._serializers.get(definition.serializer)
        return serializer.decode(payload, definition.output_type)


def _kind_of(error: BaseException) -> str:
    """The type name a failure is reported and logged by: the original type
    for an error a task process could only describe."""
    return error.kind if isinstance(error, TaskBodyError) else type(error).__name__


async def invoke_callback(function: Any, value: Any) -> None:
    """Calls a task callback with `value`: an async one on the running loop, a
    synchronous one off it. A failure is logged, by type only."""
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
            for role, value_type in definition.types_unsupported_by(serializer):
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
