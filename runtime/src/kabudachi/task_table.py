"""Every task this process submitted, from its submission until its handle is
settled.

A task has one handle, settled once, but its runs come and go: a run can be
cancelled while it still executes, fail and be queued again, or be certified as
the step of a continuation. `TaskTable` holds one record per task and the
transition rules, behind events named for what happened to the task. `Session`
reports each event; the `CancelledError` handling and the order of the calls it
makes stay in `Session`.

One lock guards the table. The native calls whose outcome the bookkeeping must
agree with (submit and register, cancel routing, complete with a continuation)
are made under it. Handles are resolved and failed outside it, because a
callback on a handle may re-enter the table.
"""

import asyncio
import hashlib
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from kabudachi.body import RunningBody
from kabudachi.errors import (
    CertificationError,
    RunStoppedError,
    TaskCancelledError,
)
from kabudachi.handle import TaskHandle
from kabudachi.native_protocol import Certification, Runtime
from kabudachi.options import SubmissionOptions
from kabudachi.registry import TaskDefinition, TaskKind

CallbackRunner = Callable[[Callable[[Any], Any], Any], None]
ResultDecoder = Callable[[str, bytes], Any]
"""Decodes a certified result, given the name of the task that produced it."""

_logger = logging.getLogger("kabudachi")


def result_digest(result: bytes) -> bytes:
    """The digest a run's result is certified by (sha256)."""
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


@dataclass
class _Record:
    """The state of one task in this process."""

    handle: TaskHandle
    definition_name: str
    # Claimed here and not queued again for a retry.
    started: bool = False
    # The result of the last run, held until the leader certifies it.
    provisional_result: bytes | None = None
    # A run of this task is in progress here.
    active: bool = False
    # The leader told this process the task was cancelled while a run was in progress.
    cancelled: bool = False
    # This process asked for the cancel and the leader accepted it. Its notice
    # may not have arrived yet, so a run that fails meanwhile is not a failure.
    cancel_requested: bool = False
    settled: bool = False
    # The running body, so a cancelled task's can be asked to stop.
    body: RunningBody | None = None
    # A body that outlived its hard limit, and that a retry waits for.
    abandoned: "asyncio.Future[None] | None" = None
    # The continuation of a task that returned a step, until it is over.
    continuation: Any = None
    # Bumped by each claim, so a retry's run cannot be undone by an abandoned
    # earlier run ending later (both await the same future).
    generation: int = 0


class Run:
    """One run of a task in this process, from its claim until
    `TaskTable.run_ended`. Used on the run's event loop only."""

    def __init__(self, task_id: str, record: _Record, generation: int) -> None:
        self.task_id = task_id
        self._record = record
        self._generation = generation

    @property
    def previous_body_exited(self) -> "asyncio.Future[None] | None":
        """Done once the body of an earlier run of this task, which outlived
        its hard limit, has stopped; a retry starts no body before that."""
        return self._record.abandoned

    def body_started(self, body: RunningBody) -> None:
        self._record.body = body

    def body_abandoned(self, body: RunningBody) -> None:
        """The body outlived its hard limit: a retry waits for `body.exited`."""
        self._record.abandoned = body.exited

    def abandoned_body_exited(self) -> None:
        self._record.abandoned = None

    @property
    def outcome_counts(self) -> bool:
        """Whether what this run ends with still matters: not once the task
        was cancelled, by this process or by the leader."""
        return not (self._record.cancelled or self._record.cancel_requested)

    @property
    def cancelled_by_leader(self) -> bool:
        """The leader cancelled the task while this run was in progress, so
        its body was cancelled on purpose."""
        return self._record.cancelled


class TaskTable:
    """Every task this process submitted, from submission until its handle is
    settled once. One lock guards the table. The native calls whose outcome
    its bookkeeping must agree with are made under that lock. Handles are
    resolved and failed outside it."""

    def __init__(
        self, runtime: Runtime, run_callback: CallbackRunner, decode_result: ResultDecoder
    ) -> None:
        self._runtime = runtime
        self._run_callback = run_callback
        self._decode_result = decode_result
        self._lock = threading.Lock()
        self._records: dict[str, _Record] = {}
        self._stopping = False

    # submitted

    def submit(
        self, definition: TaskDefinition, payload: bytes, queue: str, options: SubmissionOptions
    ) -> TaskHandle:
        """Refuses with `RunStoppedError` once stopping; otherwise submits to
        the native runtime and registers the handle under one lock, so a
        worker that claims the task at once finds it."""
        with self._lock:
            if self._stopping:
                raise RunStoppedError("the run is stopping, so no more tasks are accepted")
            task_id = self._runtime.submit(
                definition.name,
                definition.version,
                payload,
                queue,
                definition.retries,
                options.delay_ms,
                options.expires_in_ms,
                # A coalescing task always has a key: the default is "".
                (options.key or "") if definition.kind is TaskKind.COALESCING else None,
            )
            handle = TaskHandle(task_id, self.cancel, self._run_callback)
            self._records[task_id] = _Record(handle, definition.name)
        return handle

    # the handle's canceller

    def cancel(self, task_id: str) -> bool:
        """Asks the runtime to cancel `task_id`, which then tells this process
        through an event; says whether it was cancelled."""
        with self._lock:
            record = self._records.get(task_id)
            continuation = record.continuation if record is not None else None
            if continuation is None:
                # Under the lock, so a completion that carries a continuation
                # is either not accepted yet (this cancels the run) or already
                # has its continuation registered (found above).
                cancelled = self._runtime.cancel(task_id) == "cancelled"
                if cancelled and record is not None:
                    record.cancel_requested = True
                return cancelled
            if isinstance(continuation, _StartingContinuation):
                # Under the lock, so `continuation_started` cannot swap in the
                # real handle before it sees this cancel.
                return continuation.cancel()
        # The run is certified and over; what can still be cancelled is its
        # continuation, outside the lock because cancelling it takes the lock.
        return continuation.cancel()

    # claimed, run ended

    def claimed(self, task_id: str) -> Run | None:
        """A worker claimed a run of this task and the run begins now. `None`
        if the task is settled or forgotten: the run must not start."""
        with self._lock:
            record = self._records.get(task_id)
            if record is None or record.settled:
                return None
            record.started = True
            record.active = True
            record.generation += 1
            return Run(task_id, record, record.generation)

    def run_ended(self, run: Run) -> None:
        """The run ended, however it did. A stale run (a retry began after it)
        changes nothing but may let a settled task be forgotten."""
        with self._lock:
            record = run._record
            if run._generation == record.generation:
                record.active = False
                record.cancelled = False
                record.body = None
            self._retire(run.task_id, record)

    # result held, certified

    def result_held(self, task_id: str, result: bytes) -> None:
        """Keeps a run's result until the leader certifies it, while the task is open."""
        with self._lock:
            record = self._records.get(task_id)
            if record is not None and not record.settled:
                record.provisional_result = result

    def certified(self, certification: Certification) -> None:
        """Settles the task with the held result if the leader certified that
        result's digest; otherwise fails it with `CertificationError`."""
        with self._lock:
            record = self._records.get(certification.task_id)
            if record is None or record.settled:
                return
            record.settled = True
            self._retire(certification.task_id, record)
        result = record.provisional_result
        if result is None or result_digest(result) != certification.result_digest:
            _logger.warning(
                "the leader certified a different result for %s", certification.task_id
            )
            record.handle._fail(
                CertificationError(
                    f"the leader certified a different result for task {certification.task_id}"
                )
            )
            return
        try:
            value = self._decode_result(record.definition_name, result)
        except Exception as error:
            record.handle._fail(error)
            return
        record.handle._resolve(value)

    # complete with continuation, continuation started / over

    def complete_with_continuation(self, run: Run, task_run_id: str, digest: bytes) -> None:
        """Certifies the run's step (native `complete(..., continues=True)`)
        and records the continuation as starting, under one lock. Raises what
        the runtime raises; nothing is recorded then."""
        with self._lock:
            self._runtime.complete(task_run_id, digest, True)
            # Registered in the same step as the certification, so a cancel of
            # the task's handle always finds the continuation, or the run.
            run._record.continuation = _StartingContinuation()

    def continuation_started(self, run: Run, handle: TaskHandle) -> None:
        """Cancels `handle` at once if a cancel arrived while it was starting."""
        with self._lock:
            starting = run._record.continuation
            run._record.continuation = handle
        if getattr(starting, "cancel_requested", False):
            handle.cancel()  # cancelled while the first stage was being started

    def continuation_over(self, task_id: str, error: BaseException | None, result: Any) -> None:
        """Ends the continuation for the leader (a refusal is logged), then
        settles the task with `result` or `error`."""
        with self._lock:
            record = self._records.get(task_id)
            if record is not None:
                record.continuation = None
                # Forgets the task if this was the last thing keeping it: a
                # no-op while its run is still active, where `run_ended`
                # forgets it instead.
                self._retire(task_id, record)
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

    # failed with retry, failed (final, expired, superseded, interrupted)

    def retry_queued(self, task_id: str) -> None:
        """The task's next attempt is queued: its handle stays open, and it
        has not started again, so stopping fails it like any unstarted task."""
        with self._lock:
            record = self._records.get(task_id)
            if record is None or record.settled:
                return
            if not self._stopping:
                record.started = False
                record.provisional_result = None
                return
            # `stop_unstarted` had already failed everything not started, and
            # nothing will claim this retry.
            record.settled = True
            self._retire(task_id, record)
        record.handle._fail(RunStoppedError("the run stopped before this task's retry could start"))

    def failed(self, task_id: str, error: BaseException) -> None:
        """Settles the task, once, by failing its handle with `error`."""
        handle = self._settle(task_id)
        if handle is not None:
            handle._fail(error)

    # cancelled by leader

    def cancelled_by_leader(self, task_id: str) -> None:
        """Fails the handle with `TaskCancelledError` and cancels the running
        body, if a run of the task is in progress here."""
        with self._lock:
            record = self._records.get(task_id)
            if record is not None and record.active:
                record.cancelled = True
            body = record.body if record is not None else None
        self.failed(task_id, TaskCancelledError(f"task {task_id} was cancelled"))
        if body is not None:
            body.outcome.cancel()

    # stop unstarted

    def stop_unstarted(self) -> None:
        """From now on nothing is accepted, and every open task that has not
        started (a queued retry included) fails with `RunStoppedError`."""
        with self._lock:
            self._stopping = True
            dropped = []
            for task_id, record in list(self._records.items()):
                if not record.started and not record.settled:
                    record.settled = True
                    dropped.append(record.handle)
                    self._retire(task_id, record)
        for handle in dropped:
            handle._fail(RunStoppedError("the run stopped before this task could start"))

    @property
    def stopping(self) -> bool:
        return self._stopping

    # empty or pending

    def pending(self) -> bool:
        """Whether any task is not settled yet."""
        with self._lock:
            return any(not record.settled for record in self._records.values())

    def is_empty(self) -> bool:
        """Whether the table remembers no task at all: every task is settled
        and nothing of it (a run, a continuation) is still going."""
        with self._lock:
            return not self._records

    def _settle(self, task_id: str) -> TaskHandle | None:
        """Settles the task, once: returns its handle if this call did, so the
        caller can fail or resolve it outside the lock, and `None` if it was
        settled already or is not known."""
        with self._lock:
            record = self._records.get(task_id)
            if record is None or record.settled:
                return None
            record.settled = True
            self._retire(task_id, record)
            return record.handle

    def _retire(self, task_id: str, record: _Record) -> None:
        """Forgets a settled task nothing of which is still going. Call with the lock held."""
        if record.settled and not record.active and record.continuation is None:
            self._records.pop(task_id, None)
