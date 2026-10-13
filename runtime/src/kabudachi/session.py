"""One running worker's dealings with its own tasks: submitting them, running
what it claims, and delivering results only once the leader has certified them.

A result travels in two steps. The worker hands the client its bytes, which
are only provisional, and separately asks the leader to certify them by their
digest. The handle is settled when the leader's certification arrives and
matches the bytes the client holds; a result the leader refuses is never
delivered.

A flow, group or map is run by the shard's leader, which starts each of its
stages or members as an ordinary task; this process submits it, runs the
members it claims on what the leader reads out for them, and settles its
handle when the leader says it is over.

A coalescing key whose waiting payloads pile up past the soft memory limit
while one generation runs is compacted on a free worker place: the claim names
the payloads to fold, the executor folds them with the task's merge and hands the
folded payload to the leader, and no handle waits for it.

A worker of a networked shard runs whatever the shard's leader hands it,
whoever submitted the task. It delivers each result to the task's client
before reporting the run to that leader, which certifies it to the client. A
task it submits names it as client: the result arrives as `RESULT`, the
certification as `CERTIFIED`, and an end without a result as `OUTCOME`, and
its handle settles as a one-node handle does.
"""

import asyncio
import functools
import inspect
import logging
import math
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from kabudachi import _native
from kabudachi._native import MAX_FAILURE_MESSAGE_BYTES, EventKind, RunState
from kabudachi.body import Given, RunningBody, decode_resolved
from kabudachi.composites import (
    MEMBER_BATCH_BYTES,
    Composites,
    _batches,
    composite_is_running,
    refuse_oversized_values,
)
from kabudachi.config import Configuration
from kabudachi.errors import (
    BackpressureError,
    CoalescedPayloadTooLargeError,
    ConfigurationError,
    PayloadTooLargeError,
    ResultTooLargeError,
    ResultUnavailableError,
    RunStoppedError,
    RuntimeNotStartedError,
    TaskBodyError,
    TaskCancelledError,
    TaskDefinitionError,
    TaskExpiredError,
    TaskFailure,
    TaskLostError,
    TaskOrphanedError,
    TaskRecordFullError,
    TaskSupersededError,
    TaskTimeoutError,
    UnknownTaskError,
    interrupted,
)
from kabudachi.config import Unset
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
from kabudachi.options import SubmissionOptions, SubmissionSettings
from kabudachi.registry import TaskDefinition, TaskKind, TaskRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.status import CompositeStatus, TaskStatus
from kabudachi.task_table import Run, TaskTable, result_digest

WAIT_POLL_SECONDS = 0.005

# How often a stream waiting out SlowDown asks the runtime again, so a change
# no event announced (a networked leader's, say) is still seen.
SLOW_DOWN_POLL_SECONDS = 0.05

_logger = logging.getLogger("kabudachi")

# How many held runs an abort deadline or a cancel can wait for: one that
# arrives after the run was claimed but before it started here.
_EARLY_LIMIT = 1024


@dataclass
class _HeldHere:
    """A run a networked worker holds for its shard, and the abort timers it
    is under. A compaction run has no `run`: it settles no task."""

    run: Run | None
    cancel_grace: timedelta
    abort: list[asyncio.TimerHandle] = field(default_factory=list)
    aborted: bool = False


@dataclass
class _Early:
    """An abort deadline (on the loop's clock) or a cancel that came for a
    held run before it started here."""

    deadline: float | None = None
    cancelled: bool = False


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
        # What came for held runs not started yet, by run id, oldest first.
        self._early: dict[str, _Early] = {}
        # This worker's shard name and id, read on the first run handed over.
        self._identity: tuple[str, str] | None = None
        try:
            # Built inside the run's loop, as `run()` does.
            self._hosted.attach(asyncio.get_running_loop())
        except RuntimeError:
            pass
        # Set when a SLOW_DOWN event arrives, to wake a stream waiting for room.
        self._slow_down_changed = asyncio.Event()
        # Every flow, group and map started here.
        self._composites = Composites(self, self._hosted)
        # Runs of composites' members, and expansions, under way here.
        self._members_running = 0
        # Finished composites whose results are being read to settle a handle.
        self._deliveries = 0
        # What this does with each kind of event the runtime decides on its own.
        self._event_handlers: dict[EventKind, Callable[[Any], None]] = {
            EventKind.EXPIRED: self._on_expired,
            EventKind.CANCELLED: self._on_cancelled,
            EventKind.RECORD_FULL: self._on_record_full,
            EventKind.COALESCED_PAYLOAD_TOO_LARGE: self._on_coalesced_payload_too_large,
            EventKind.SLOW_DOWN: self._on_slow_down,
            EventKind.SUPERSEDED: self._on_superseded,
            EventKind.ACCEPTED: self._on_accepted,
            EventKind.REFUSED: self._on_refused,
            EventKind.ABORT: self._on_abort,
            EventKind.ABORT_WITHDRAWN: self._on_abort_withdrawn,
            EventKind.RESULT: self._on_result,
            EventKind.CERTIFIED: self._on_certified,
            EventKind.OUTCOME: self._on_outcome,
            EventKind.COMPOSITE_FINISHED: self._on_composite_finished,
        }

    def submit(
        self,
        definition: TaskDefinition,
        argument: Any,
        options: SubmissionOptions | None = None,
    ) -> TaskHandle:
        """Queues `definition` to run on `argument` and returns its handle.

        Safe to call from any thread. Raises `SerializationError`, before
        anything is queued, if `argument` is not what the task takes, and
        `PayloadTooLargeError` if the encoded `argument` alone is larger than
        `memory_hard_limit`, `RunStoppedError` once the run is stopping, and
        `ConfigurationError` if the router returns something that is not a
        queue name or `None`.
        """
        serializer = self._serializers.get(definition.serializer)
        payload = serializer.encode(argument, definition.input_type)
        return self._record(definition, payload, options or SubmissionOptions(), lambda: argument)

    def _record(
        self,
        definition: TaskDefinition,
        payload: bytes,
        options: SubmissionOptions,
        value: Callable[[], Any],
    ) -> TaskHandle:
        limit = self._runtime.payload_limit
        if limit is not None and len(payload) > limit:
            raise PayloadTooLargeError(
                f"task {definition.name}'s input is {len(payload)} bytes, past the {limit} this process can hold",
                len(payload),
                limit,
            )
        queue = self._queue_of(definition, value)
        settings = SubmissionSettings(queue, self._reconnect_timeout_ms(definition, queue))
        return self._tasks.submit(definition, payload, options, settings)

    def _queue_of(self, definition: TaskDefinition, value: Callable[[], Any]) -> str:
        """The queue a task of `definition` goes to: its own, else the one
        the router picks for its argument (`value()`, called only when a
        router runs), else `queue`."""
        if not isinstance(definition.queue, Unset):
            return definition.queue
        router = self._configuration.resolve("router")
        if router is not None:
            chosen = router(definition.name, value())
            if chosen is not None:
                if not isinstance(chosen, str) or not chosen.strip():
                    raise ConfigurationError(
                        f"the router returned {chosen!r} for {definition.name}; "
                        "it must return a queue name or None"
                    )
                return chosen
        return self._configuration.resolve("queue")

    def _reconnect_timeout_ms(self, definition: TaskDefinition, queue: str) -> int | None:
        """The reconnect timeout a run of `definition` sent to `queue`
        carries, in whole milliseconds rounded up, and at least one, so it
        is never replayed early: the task's own, else its queue's. `None`
        leaves it to the shard."""
        timeout = definition.reconnect_timeout
        if timeout is None:
            options = self._configuration.resolve("queue_options").get(queue)
            timeout = None if options is None else options.reconnect_timeout
            if timeout is None:
                return None
        return max(1, math.ceil(timeout / timedelta(milliseconds=1)))

    def submission_settings(self, definition: TaskDefinition) -> dict[str, Any]:
        """What a member of a composite carries to the leader for `definition`,
        resolved here as a submit resolves it. The queue is the task's own,
        else `queue`: a router picks by an argument that a composite's member
        does not have yet."""
        queue = definition.queue
        if isinstance(queue, Unset):
            queue = self._configuration.resolve("queue")
        return {
            "queue": queue,
            "retries": definition.retries,
            "ephemeral": definition.kind is TaskKind.EPHEMERAL,
            "non_retriable": not definition.retriable,
            "reconnect_timeout_ms": self._reconnect_timeout_ms(definition, queue),
        }

    def submit_serialized(
        self, definition_id: str, payload: bytes, options: SubmissionOptions
    ) -> TaskHandle:
        """Submits a task a body in a task process called, its input already
        encoded there. Raises as `submit` does, and `UnknownTaskError` for a
        task this process does not have."""
        definition = self._registry.get(definition_id)
        if definition is None:
            raise UnknownTaskError(f"this process has no task named {definition_id!r}")
        serializer = self._serializers.get(definition.serializer)
        return self._record(
            definition, payload, options, lambda: serializer.decode(payload, definition.input_type)
        )

    def submit_composite(self, step: Any, previous: Any) -> TaskHandle:
        """Submits a flow, group or map a body in a task process called."""
        return self._composites.submit(step, previous)

    def cancel_task(self, task_id: str) -> bool:
        """Cancels a task a body in a task process called, by its id, as its
        handle's `cancel` does. The pool asks this only for a cancel that
        raced the task's outcome settling, after it dropped the handle."""
        return self._tasks.cancel(task_id)

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
    def serializers(self) -> SerializerRegistry:
        """The serializers this session encodes and decodes tasks' values with."""
        return self._serializers

    @property
    def stopping(self) -> bool:
        """Whether the run is stopping, so nothing new may be submitted."""
        return self._tasks.stopping

    @property
    def composites(self) -> Composites:
        """Every flow, group and map started here."""
        return self._composites

    @property
    def tasks(self) -> TaskTable:
        """The table of every task this session submitted and has not forgotten."""
        return self._tasks

    @property
    def callback_runner(self) -> Callable[[Callable[[Any], Any], Any], None]:
        """What a handle made by this session runs its callbacks with."""
        return self._run_callback

    async def task_status(self, task_id: str) -> TaskStatus | CompositeStatus:
        """How a task, flow, group or map submitted here stands: a composite
        from its leader's record of it, a task from its shard's."""
        composite = self._composites.status_of(task_id)
        if composite is not None:
            return await composite
        return await self._tasks.status(task_id)

    def has_room(self) -> bool:
        """Whether there is room to submit in bulk now: the runtime has not
        said memory use is past its soft limit. Asked of the runtime, not
        inferred from events, so a late event can never leave a stream paused."""
        return not self._runtime.slow_down()

    async def wait_for_room(self) -> None:
        """Waits for a SLOW_DOWN event or `SLOW_DOWN_POLL_SECONDS`, whichever
        comes first. The caller asks `has_room` again."""
        self._slow_down_changed.clear()
        try:
            await asyncio.wait_for(self._slow_down_changed.wait(), SLOW_DOWN_POLL_SECONDS)
        except asyncio.TimeoutError:
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
                handler = self._event_handlers.get(event.kind)
                if handler is None:
                    raise RuntimeError(
                        f"the native runtime reported an event of unknown kind {event.kind!r}"
                    )
                handler(event)

    def _on_expired(self, event: Any) -> None:
        self._tasks.failed(
            event.task_id,
            TaskExpiredError(f"task {event.task_id} expired before it could start"),
        )

    def _on_cancelled(self, event: Any) -> None:
        here = self._here.get(event.task_run_id)
        if here is None:
            self._tasks.cancelled_by_leader(event.task_id)
            self._hold_early(event.task_run_id).cancelled = True
        else:
            self._cancel_held(here, event.task_run_id)

    def _on_record_full(self, event: Any) -> None:
        self._tasks.failed(
            event.task_id,
            TaskRecordFullError(
                f"task {event.task_id} ended: its record had no room for another attempt"
            ),
        )

    def _on_coalesced_payload_too_large(self, event: Any) -> None:
        self._tasks.failed(
            event.task_id,
            CoalescedPayloadTooLargeError(
                f"task {event.task_id}'s folded payloads grew too large to run"
            ),
        )

    def _on_slow_down(self, event: Any) -> None:
        self._slow_down_changed.set()

    def _on_superseded(self, event: Any) -> None:
        self._tasks.failed(
            event.task_id,
            TaskSupersededError(
                f"task {event.task_id} was superseded by {event.superseded_by}",
                event.superseded_by,
            ),
        )

    def _on_accepted(self, event: Any) -> None:
        # Stored on a quorum: its handle is `REPLICATED`.
        if not self._composites.accepted(event.task_id):
            self._tasks.replicated(event.task_id)

    def _on_refused(self, event: Any) -> None:
        error: Exception = BackpressureError(
            f"task {event.task_id} was refused: {event.reason}"
        )
        if event.reason == "TASK_REJECT_PAYLOAD_TOO_LARGE":
            error = PayloadTooLargeError(
                "the input is past the hard memory limit of the shard's leader"
            )
        if not self._composites.refused(event.task_id, error):
            self._tasks.refused(event.task_id, error)

    def _on_abort(self, event: Any) -> None:
        self._abort_at(event.task_run_id, event.seconds_left)

    def _on_abort_withdrawn(self, event: Any) -> None:
        self._clear_abort(event.task_run_id)

    def _on_result(self, event: Any) -> None:
        self._tasks.result_held(event.task_id, event.task_run_id, event.payload)

    def _on_certified(self, event: Any) -> None:
        self._tasks.certified(event)

    def _on_composite_finished(self, event: Any) -> None:
        self._composites.finished(event)

    def continuation_finished(self, event: Any) -> None:
        """The composite a task's run returned as its step is over: settles
        the task's handle as the composite's would be."""
        handle = self._tasks.continuation_over(event.parent_task_id)
        if handle is not None:
            self.settle_from_composite(handle, event)

    def settle_from_composite(self, handle: TaskHandle, event: Any) -> None:
        """Settles `handle`, of a composite or of the task that returned it as
        its step, by how the composite `event` names ended: with its results,
        with the failure that ended it, or as cancelled."""
        match event.state:
            case "succeeded":
                self._deliveries += 1
                if not self._hosted.spawn(self._deliver(handle, event.composite_id)):
                    self._deliveries -= 1
                    handle._fail(RunStoppedError("the run ended before the results could be read"))
            case "failed":
                handle._fail(self._failure_of(handle.task_id, event))
            case _:
                handle._fail(TaskCancelledError(f"{handle.task_id} was cancelled"))
        self._release_member_errors()

    def _awaits_composites(self) -> bool:
        """Whether anything here still waits for a composite, so may be given
        the error a member of it failed with."""
        return bool(
            self._composites.pending() or self._tasks.continuations() or self._deliveries > 0
        )

    def _release_member_errors(self) -> None:
        """Drops the errors kept for members once nothing here awaits a
        composite: those that were not taken, such as a failed sibling of the
        member that ended its composite, will not be asked for."""
        if not self._awaits_composites():
            self._tasks.drop_member_errors()

    async def _deliver(self, handle: TaskHandle, composite_id: str) -> None:
        """Resolves `handle` with the results of the finished composite."""
        try:
            await self._resolve_with_results(handle, composite_id)
        finally:
            self._deliveries -= 1
            self._release_member_errors()

    async def _resolve_with_results(self, handle: TaskHandle, composite_id: str) -> None:
        try:
            resolved = await self._runtime.composite_results(composite_id)
        except Exception as error:
            handle._fail(
                ResultUnavailableError(
                    f"{handle.task_id} succeeded, but its results can no longer be read: {error}",
                    handle.task_id,
                )
            )
            return
        try:
            value = self.decode_resolved(resolved, own_errors=True)
        except Exception as error:
            handle._fail(error)
            return
        handle._resolve(value)

    def _failure_of(self, subject: str, event: Any) -> BaseException:
        """What a handle fails with when the composite `event` names failed:
        the error of the member that ended it if this process ran that member,
        else an error of the kind and message the leader gives."""
        kind, message = event.failure_kind, event.failure_message
        held = self._tasks.member_error(event.failure_task_id)
        if held is not None:
            return held
        known = _KNOWN_FAILURES.get(kind)
        if known is not None:
            return known(message or f"{subject} failed with {kind}")
        return TaskBodyError(message or f"{subject} failed with {kind}", kind)

    def decode_resolved(self, resolved: Any, *, own_errors: bool = False) -> Any:
        """The value a stage's `resolved` result stands for, each value decoded
        by the serializer of the task that produced it. A failure is a
        `TaskFailure`, or, with `own_errors`, the error this process's run of
        that member failed with, when it still has it."""
        failure = self._own_failure if own_errors else TaskFailure
        return decode_resolved(self._registry, self._serializers, resolved, failure)

    def _own_failure(self, kind: str, message: str, task_id: str | None) -> BaseException:
        return self._tasks.member_error(task_id) or TaskFailure(kind, message)

    def _on_outcome(self, event: Any) -> None:
        if event.state is None:
            # The shard does not know the task: nothing says it was ever stored.
            self._tasks.failed(event.task_id, _outcome_error(event))
            return
        self._tasks.concluded(event.task_id, _outcome_error(event))

    def _hold_early(self, run_id: str) -> _Early:
        """What came for `run_id` before it started here, kept for `_start`
        on a networked worker, which may have claimed it already. The oldest
        is dropped past `_EARLY_LIMIT`: most are for runs held elsewhere."""
        early = self._early.get(run_id)
        if early is not None:
            return early
        early = _Early()
        if self._runtime.certifies_locally or not run_id:
            return early
        if len(self._early) >= _EARLY_LIMIT:
            del self._early[next(iter(self._early))]
        self._early[run_id] = early
        return early

    def _apply_early(self, run_id: str) -> None:
        """Applies what came for a held run before it started here."""
        early = self._early.pop(run_id, None)
        here = self._here.get(run_id)
        if early is None or here is None:
            return
        if early.cancelled:
            self._cancel_held(here, run_id)
        elif early.deadline is not None:
            self._abort_at(run_id, early.deadline - asyncio.get_running_loop().time())

    def _cancel_held(self, here: _HeldHere, run_id: str) -> None:
        if here.run is not None:
            here.run.cancel_by_leader()
        self._executor.cancel(run_id)

    def _abort_at(self, run_id: str, seconds_left: float) -> None:
        """The run may be run again elsewhere `seconds_left` from now: its body
        is asked to stop its cancel grace before that, and its task process is
        killed then if it has not. A later deadline for the run replaces this one."""
        loop = asyncio.get_running_loop()
        here = self._here.get(run_id)
        if here is None:
            self._hold_early(run_id).deadline = loop.time() + seconds_left
            return
        self._clear_abort(run_id)
        stop_in = max(0.0, seconds_left - here.cancel_grace.total_seconds())
        here.abort = [
            loop.call_later(stop_in, self._abort, here, run_id),
            loop.call_later(seconds_left, self._executor.kill_host_of, run_id),
        ]

    def _abort(self, here: _HeldHere, run_id: str) -> None:
        here.aborted = True
        if here.run is not None:
            here.run.abort()
        self._executor.cancel(run_id)

    def _clear_abort(self, run_id: str) -> None:
        """The leader hears this worker again: a pending abort is dropped. A
        body already asked to stop is not asked to go on."""
        early = self._early.get(run_id)
        if early is not None:
            early.deadline = None
            if not early.cancelled:
                del self._early[run_id]
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

    async def stop_claiming(self) -> None:
        """Stops the run: no more tasks start or are accepted, and every task
        that has not started fails with `RunStoppedError`, which also frees
        any running task that was waiting for one of them. Tasks already
        running are not interrupted."""
        self._tasks.stop_unstarted()
        self._executor.wake()
        if self._members_running == 0:
            await self._fail_stalled()

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
        return (
            self._tasks.pending()
            or self._composites.pending()
            or self._members_running > 0
            or self._hosted.outstanding > 0
        )

    async def _fail_stalled(self) -> None:
        """The run is stopping and no member runs here: a composite still
        running can never go on, and neither can a task waiting for the one it
        returned. They fail with `RunStoppedError`, unless the leader has just
        finished them, when their own finish settles them."""
        await self._composites.stopped()
        for task_id, composite_id in self._tasks.continuations():
            if await composite_is_running(self._runtime, composite_id):
                self._tasks.failed(
                    task_id, RunStoppedError("the run stopped before this task's step could finish")
                )
        self._release_member_errors()

    def _run_callback(self, function: Any, value: Any) -> None:
        """Runs a task callback on the event loop, from whichever thread the
        task was settled on; inline if there is no loop left to run it."""
        if not self._hosted.spawn(invoke_callback(function, value)):
            run_callback_inline(function, value)

    def _start(self, claim: Any) -> None:
        if claim.compaction:
            # Internal: no handle waits for it, so it is not in the task table.
            # Held for the shard, it is under an abort deadline like any run.
            if not self._runtime.certifies_locally:
                definition = self._registry.get(claim.definition_id)
                grace = (
                    self._cancel_grace(definition)
                    if definition is not None
                    else self._configuration.resolve("cancel_grace")
                )
                self._here[claim.task_run_id] = _HeldHere(None, grace)
            job = CompactJob(claim.task_run_id, claim.definition_id, tuple(claim.chain))
            self._track(self._compact(claim, self._executor.compact(job)))
            self._apply_early(claim.task_run_id)
            return
        if claim.expansion:
            self._members_running += 1
            self._track(self._expand(claim))
            return
        if self._runtime.certifies_locally and claim.composite_id is None:
            run = self._tasks.claimed(claim.task_id)
            if run is None:
                # Already settled and forgotten (by `stop_claiming`, or any other
                # route): its handle is settled, so running the body now would
                # contradict that.
                return
        else:
            # The shard's leader handed this worker the run, whoever
            # submitted the task, or the leader made it a member of a
            # composite: it runs, and settles no handle here.
            run = self._tasks.detached(claim.task_id)
            self._here[claim.task_run_id] = _HeldHere(run, self._configuration.resolve("cancel_grace"))
        if claim.composite_id is not None:
            self._members_running += 1
        handed = self._hand_over(claim, run)
        if not isinstance(handed, BaseException) and claim.task_run_id in self._here:
            self._here[claim.task_run_id].cancel_grace = self._cancel_grace(handed[0])
        self._track(self._run(claim, run, handed))
        self._apply_early(claim.task_run_id)

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
            shard, worker_id = self._shard_and_worker()
            given = self._read_out(claim)
            body = self._executor.run(
                RunJob(
                    run_id=claim.task_run_id,
                    task_id=claim.task_id,
                    definition_id=claim.definition_id,
                    source_version=claim.source_version,
                    serialized_input=claim.serialized_input,
                    chain=tuple(claim.chain),
                    queue=claim.queue,
                    attempt=claim.attempt_number,
                    shard=shard,
                    worker_id=worker_id,
                    cancel_grace=self._cancel_grace(definition),
                    # A retry does not run beside the abandoned body it replaces.
                    after=after,
                    given=given,
                )
            )
        except BaseException as error:
            return error
        run.body_started(body)
        return definition, body, after

    def _read_out(self, claim: Any) -> "asyncio.Future[Given] | None":
        """Starts reading what the leader holds for the member `claim` is a run
        of, if it is one: its bound fields and the results it takes. The body
        starts once they are read, and fails if they cannot be."""
        if claim.composite_id is None:
            return None
        reading = asyncio.get_running_loop().create_task(self._given(claim))
        # A run cancelled before it needed them leaves a failure nobody reads.
        reading.add_done_callback(lambda done: done.cancelled() or done.exception())
        return reading

    async def _given(self, claim: Any) -> Given:
        fields, resolved = await self._runtime.resolve_input(claim.task_run_id)
        return Given(tuple(fields), resolved)

    def _shard_and_worker(self) -> tuple[str, str]:
        if self._identity is None:
            self._identity = (self._runtime.shard_name(), self._runtime.worker_id())
        return self._identity

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

        A task that fails is reported to the leader, by its error's type and
        message, before its handle is failed with the error itself.
        """
        body: RunningBody | None = None
        try:
            if isinstance(handed, BaseException):
                raise handed
            definition, body, previous = handed
            await self._run_body(claim, run, definition, body, previous)
        except TaskProcessLost:
            self._lost(claim, run)
        except asyncio.CancelledError as error:
            if not await self._cancellation_ends_here(claim, run, body, error):
                raise
        except Exception as error:
            await self._run_failed(claim, run, body, error)
        except BaseException as error:
            # Worse than cancelled: the task has no result, and whoever waits
            # for it must not wait forever.
            self._fail_held(claim, run, error)
            raise
        finally:
            self._run_ended(claim, run)

    async def _run_body(
        self,
        claim: Any,
        run: Run,
        definition: TaskDefinition,
        body: RunningBody,
        previous: "asyncio.Future[Any] | None",
    ) -> None:
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
        await self._complete(claim, run, outcome)

    async def _complete(self, claim: Any, run: Run, outcome: Any) -> None:
        limit = self._runtime.payload_limit
        if limit is not None and len(outcome) > limit:
            raise ResultTooLargeError(
                f"task {claim.definition_id} returned {len(outcome)} bytes, past the {limit} this process can hold"
            )
        if claim.composite_id is not None:
            # A member's result goes to the shard as a blob, stored before the
            # run is reported, so the stage after it can always read it.
            digest = await self._runtime.store_result(claim.task_id, outcome)
            if digest is None:
                # The shard could not take the result in time (it has no
                # leader, say): the run is lost, not failed, so the leader
                # replays it as it replays a run lost with its worker.
                self._lost(claim, run)
                return
            self._runtime.complete(claim.task_run_id, digest)
            return
        if run.holds_handle:
            self._tasks.result_held(claim.task_id, claim.task_run_id, outcome)
            certification = self._runtime.complete(claim.task_run_id, result_digest(outcome))
        else:
            # Held for the shard: the result goes to the task's client first.
            certification = self._runtime.complete(
                claim.task_run_id, result_digest(outcome), result=outcome
            )
        if certification is not None:
            self._tasks.certified(certification)

    async def _cancellation_ends_here(
        self,
        claim: Any,
        run: Run,
        body: RunningBody | None,
        error: asyncio.CancelledError,
    ) -> bool:
        """Whether the run's cancellation ends here instead of going on to
        cancel the coroutine running it: the run was aborted at its deadline,
        or only its body was cancelled because the leader cancelled the task.
        Otherwise the run's handle is failed, and the caller re-raises."""
        current = asyncio.current_task()
        cancelling = current is not None and current.cancelling()
        if run.aborted and not cancelling:
            # Another leader may run it again once the deadline passes:
            # it is lost only once its body has really stopped.
            if body is not None:
                await asyncio.wait({body.exited})
            self._report_lost_quietly(claim)
            return True
        if run.cancelled_by_leader and current is not None and not cancelling:
            # Only the body was cancelled, because the task was: this is
            # not this run being interrupted, and its handle is settled.
            return True
        self._fail_held(claim, run, error)
        return False

    async def _run_failed(
        self, claim: Any, run: Run, body: RunningBody | None, error: Exception
    ) -> None:
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
        message = _message_of(error)
        retried = self._report_failure(claim, kind, message)
        if claim.composite_id is not None and not retried and self._awaits_composites():
            # Kept before anything else is heard of the member, for whoever
            # awaits its composite.
            self._tasks.member_failed(claim.task_id, error)
        if run.holds_handle:
            if retried:
                self._tasks.retry_queued(claim.task_id)
            else:
                self._tasks.failed(claim.task_id, error)
        if abandoned:
            # It keeps its place until it has really stopped.
            await asyncio.wait({body.exited})
            run.abandoned_body_exited()

    def _fail_held(self, claim: Any, run: Run, error: BaseException) -> None:
        if run.holds_handle:
            self._interrupted(claim.task_id, error)

    def _run_ended(self, claim: Any, run: Run) -> None:
        if claim.composite_id is not None:
            self._member_ended()
        if run.holds_handle:
            # On the abandoned-body retry path a newer run of this task can
            # begin while this one still awaits `body.exited`; the table
            # leaves the newer run's state alone when this stale one ends.
            self._tasks.run_ended(run)
        else:
            self._forget_held(claim.task_run_id)

    def _member_ended(self) -> None:
        self._members_running -= 1
        if self._members_running == 0 and self._tasks.stopping:
            # Their state is read on the run's loop, counted as work until done.
            self._hosted.spawn(self._fail_stalled())

    def _interrupted(self, task_id: str, error: BaseException) -> None:
        self._tasks.failed(task_id, interrupted(f"task {task_id}", error))

    def _continue(self, claim: Any, run: Run, step: Any) -> None:
        """The task returned `step`: certify the run, by the digest of the
        step, which the leader then runs as the task's continuation. The task
        is over, and its handle settled, when the continuation is."""
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
        tree = step.describe(self)
        if step.is_task:
            tree = ("flow", "fail_fast", [tree])
        refuse_oversized_values(self._runtime, tree)
        self._runtime.complete(claim.task_run_id, continuation=tree)
        if run.holds_handle:
            self._tasks.continuing(claim.task_id, _native.continuation_of(claim.task_run_id))

    async def _expand(self, claim: Any) -> None:
        """Splits the list an expansion run names into the map it feeds: each
        item encoded as the map's task takes it and sent in batches, waiting
        out SlowDown, then the run completes, which seals the map. The leader
        never decodes the list."""
        try:
            request = _native.read_expansion(claim.serialized_input)
            items = self.decode_resolved(await self._runtime.resolve_expansion(claim.task_run_id))
            template = self._registry.get(request.definition_id)
            if template is None:
                raise UnknownTaskError(f"this process has no task named {request.definition_id!r}")
            serializer = self._serializers.get(template.serializer)
            self._runtime.report_started(claim.task_run_id)
            encoded = (serializer.encode(item, template.input_type) for item in items)
            start = 0
            for batch in _batches(encoded, self._composites.batch_items, MEMBER_BATCH_BYTES):
                while not self.has_room():
                    if self.stopping:
                        raise RunStoppedError("the run stopped while this was being sent")
                    await self.wait_for_room()
                if await self._runtime.submit_members(claim.composite_id, start, batch, False) != "accepted":
                    # The map ended without all its items (a member failed, or
                    # it was cancelled), which cancels this run: nothing is left
                    # to complete or report.
                    return
                start += len(batch)
            self._runtime.complete(claim.task_run_id, result_digest(b""))
        except Exception as error:
            kind, message = _kind_of(error), _message_of(error)
            if not self._report_failure(claim, kind, message) and self._awaits_composites():
                self._tasks.member_failed(claim.task_id, error)
        finally:
            self._member_ended()

    def _forget_held(self, run_id: str) -> None:
        self._clear_abort(run_id)
        self._here.pop(run_id, None)
        self._early.pop(run_id, None)

    async def _compact(self, claim: Any, body: RunningBody) -> None:
        """Waits for a compaction run's fold and hands the folded payload to the
        leader. A failure is reported by the error's type only, and the leader
        then leaves the chain as it was."""
        try:
            await self._fold(claim, body)
        finally:
            self._forget_held(claim.task_run_id)

    async def _fold(self, claim: Any, body: RunningBody) -> None:
        try:
            folded = await self._folded(claim, body)
        except Exception as error:
            self._compaction_failed(claim, error)
            return
        if folded is not None:
            self._hand_fold_to_leader(claim, folded)

    async def _folded(self, claim: Any, body: RunningBody) -> bytes | None:
        """The folded payload, or `None` if the leader cancelled the
        compaction, which leaves nothing to report. A compaction stopped at
        its abort deadline raises `TaskProcessLost` once its body has really
        stopped, as one lost with its process does."""
        try:
            return await body.outcome
        except asyncio.CancelledError:
            here = self._here.get(claim.task_run_id)
            current = asyncio.current_task()
            if here is None or (current is not None and current.cancelling()):
                raise
            if not here.aborted:
                return None
            await asyncio.wait({body.exited})
            raise TaskProcessLost("the compaction was stopped at its abort deadline") from None

    def _compaction_failed(self, claim: Any, error: Exception) -> None:
        kind = _kind_of(error)
        _logger.warning("compaction of %s failed with %s", claim.definition_id, kind)
        _logger.debug("compaction of %s failed", claim.definition_id, exc_info=error)
        try:
            self._runtime.report_failure(claim.task_run_id, kind, _message_of(error))
        except Exception as refusal:
            _logger.warning(
                "the leader did not record the failed compaction of %s: %s",
                claim.definition_id,
                type(refusal).__name__,
            )

    def _hand_fold_to_leader(self, claim: Any, folded: bytes) -> None:
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
        """The run cannot be seen through: the process running the body died,
        or the shard could not take its result. The leader decides, as for a
        lost worker: a new attempt (the handle waits for it), or the task is
        over (an ephemeral one, or a non-retriable one that may have had its
        effects). A refused report ends the task here too; failing a handle
        the leader already settled changes nothing."""
        if not run.outcome_counts:
            # Cancelled first, the leader's `cancelled` event settles the
            # handle, as for a cancelled body that failed.
            return
        _logger.warning("task %s was reported lost", claim.task_id)
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
            self._tasks.failed(claim.task_id, self._loss_error(claim))

    def _loss_error(self, claim: Any) -> BaseException:
        """How a run lost here with no new attempt ended for its handle:
        orphaned if the leader orphaned it (a task not safe to run twice),
        lost otherwise, or if the runtime has shut down. Only a run that
        holds a handle gets here, and only a runtime that certifies locally
        hands out such runs, so the runtime has `task_run_state`."""
        try:
            state = self._runtime.task_run_state(claim.task_run_id)
        except RuntimeError as closed:
            # The runtime shut down: the leader's word is gone with it.
            _logger.debug(
                "the state of task %s's lost run could not be read: %s",
                claim.task_id,
                type(closed).__name__,
            )
            state = None
        if state == RunState.ORPHANED:
            return TaskOrphanedError(
                f"task {claim.task_id} was running when the process that ran it died; whether it "
                "took effect is unknown"
            )
        return TaskLostError(f"task {claim.task_id} was lost with the process that ran it")

    def _report_lost_quietly(self, claim: Any) -> None:
        try:
            self._runtime.report_lost(claim.task_run_id)
        except Exception as refusal:
            _logger.warning(
                "the leader was not told task %s's run stopped at its abort deadline: %s",
                claim.task_id,
                type(refusal).__name__,
            )

    def _report_failure(self, claim: Any, kind: str, message: str) -> bool | None:
        """Tells the leader the run failed and says whether it will be retried.
        The failure's kind and message go, never its traceback. A refusal is
        logged, counts as no retry, and does not replace the error, which is
        what the handle's awaiter needs to see."""
        try:
            return self._runtime.report_failure(claim.task_run_id, kind, message)
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


# Failure kinds a leader reports that name kabudachi's own errors, including
# the kinds the leader itself gives a task it ended.
_KNOWN_FAILURES: dict[str, type[Exception]] = {
    "ResultTooLargeError": ResultTooLargeError,
    "TaskTimeoutError": TaskTimeoutError,
    "HistoryTooLarge": TaskRecordFullError,
    "CoalescedPayloadTooLarge": CoalescedPayloadTooLargeError,
    # A claimant whose hard memory limit the input alone is past.
    "PayloadTooLarge": PayloadTooLargeError,
}

# How a task ended without a result, for the endings that carry nothing more
# than their state: the error it raises and what its message says.
_PLAIN_ENDINGS: dict[Any, tuple[type[Exception], str]] = {
    RunState.LOST: (TaskLostError, "was lost with its worker and is not run again"),
    RunState.ORPHANED: (
        TaskOrphanedError,
        "was running when its worker was lost; whether it took effect is unknown",
    ),
    RunState.CANCELLED: (TaskCancelledError, "was cancelled"),
    RunState.EXPIRED: (TaskExpiredError, "expired before it could start"),
}


def _outcome_error(event: Any) -> BaseException:
    """The error a handle fails with when its task ended without a result
    here, by what its leader said."""
    task_id = event.task_id
    if event.state is None:
        return ResultUnavailableError(
            f"the shard no longer knows task {task_id}: it never stored it, or forgot it after "
            "its result's time to live",
            task_id,
        )
    if event.state == RunState.FAILED:
        return _failure_error(task_id, event.failure_kind)
    if event.state == RunState.SUPERSEDED:
        return TaskSupersededError(
            f"task {task_id} was superseded by {event.superseded_by}", event.superseded_by
        )
    plain = _PLAIN_ENDINGS.get(event.state)
    if plain is None:
        return ResultUnavailableError(f"task {task_id} ended as {event.state!r} with no result", task_id)
    error_type, ending = plain
    return error_type(f"task {task_id} {ending}")


def _failure_error(task_id: str, kind: str) -> BaseException:
    """The error of a run that failed elsewhere, of which only the kind
    arrives: kabudachi's own kinds as their classes."""
    known = _KNOWN_FAILURES.get(kind)
    if known is not None:
        return known(f"task {task_id} failed with {kind}")
    return TaskBodyError(f"task {task_id} failed with {kind}", kind)


def _kind_of(error: BaseException) -> str:
    """The type name a failure is reported and logged by: the original type
    for an error a task process could only describe."""
    return error.kind if isinstance(error, TaskBodyError) else type(error).__name__


def _message_of(error: BaseException) -> str:
    """What a failure is recorded with besides its type: the error's own
    message, never its traceback, cut to what the leader keeps. It is stored
    in the task's record, so it is what the error says, as `str(error)` gives
    it. An error that cannot be turned into text has no message."""
    try:
        text = str(error)
    except Exception:
        text = ""
    encoded = text.encode("utf-8", "replace")[:MAX_FAILURE_MESSAGE_BYTES]
    return encoded.decode("utf-8", "ignore")


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
