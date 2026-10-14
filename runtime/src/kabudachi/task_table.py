"""Every task this process submitted, from its submission until its handle is
settled.

A task has one handle, settled once, but its runs come and go: a run can be
cancelled while it still executes, fail and be queued again, or be certified as
the step of a continuation. `TaskTable` holds one record per task and the
transition rules, behind events named for what happened to the task. `Session`
reports each event; the `CancelledError` handling and the order of the calls it
makes stay in `Session`.

One lock guards the table. The native calls whose outcome the bookkeeping must
agree with (submit and register, cancel routing) are made under it. Handles are
resolved and failed outside it, because a callback on a handle may re-enter the
table. A networked task's record is kept until a leader's word settles it, as a
one-node task's is.
"""

import asyncio
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from kabudachi import _native
from kabudachi._native import CancelOutcome
from kabudachi.body import RunningBody
from kabudachi.errors import (
    CertificationError,
    ResultUnavailableError,
    RunStoppedError,
    TaskCancelledError,
)
from kabudachi.handle import TaskHandle
from kabudachi.native_protocol import Certification, Runtime, SubmitArguments
from kabudachi.options import SubmissionOptions, SubmissionSettings
from kabudachi.registry import TaskDefinition, TaskKind
from kabudachi.status import TaskStatus

CallbackRunner = Callable[[Callable[[Any], Any], Any], None]
ResultDecoder = Callable[[str, bytes], Any]
"""Decodes a certified result, given the name of the task that produced it."""

_logger = logging.getLogger("kabudachi")

def result_digest(result: bytes) -> bytes:
    """The digest a run's result is certified by (BLAKE3)."""
    return _native.result_digest(result)


def _run_stopped(handle: TaskHandle, unstarted: str) -> RunStoppedError:
    """Why stopping the run failed `handle` before its task's result arrived:
    a networked handle's task may be stored and running on another worker, so
    it is not said not to have started."""
    if handle._replicable:
        return RunStoppedError(
            "the run stopped before this task's result arrived here; the task may still run "
            "elsewhere"
        )
    return RunStoppedError(f"the run stopped before {unstarted}")


@dataclass
class _Record:
    """The state of one task in this process."""

    handle: TaskHandle
    definition_name: str
    # Claimed here and not queued again for a retry.
    started: bool = False
    # Results delivered for runs of this task, by run id, until the leader certifies one.
    provisional: dict[str, bytes] = field(default_factory=dict)
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
    # The id of the composite a task that returned a step now runs as, until it is over.
    continuation: str | None = None
    # Bumped by each claim, so a retry's run cannot be undone by an abandoned
    # earlier run ending later (both await the same future).
    generation: int = 0


class Run:
    """One run of a task in this process, from its claim until
    `TaskTable.run_ended`. Used on the run's event loop only."""

    def __init__(
        self, task_id: str, record: _Record, generation: int, *, holds_handle: bool = True
    ) -> None:
        self.task_id = task_id
        self._record = record
        self._generation = generation
        self._holds_handle = holds_handle
        self._aborted = False

    @property
    def holds_handle(self) -> bool:
        """Whether this run settles a handle of this process. A run a shard's
        leader handed a networked worker settles none, whoever submitted its
        task: its result goes to the task's client, whose handle settles
        once the leader certifies it."""
        return self._holds_handle

    def abort(self) -> None:
        """The run's abort deadline is near: it is being stopped, and is
        reported lost once its body has exited."""
        self._aborted = True

    @property
    def aborted(self) -> bool:
        """Whether the run is being stopped before its abort deadline."""
        return self._aborted

    def cancel_by_leader(self) -> None:
        """The leader cancelled this run: nothing it ends with counts."""
        self._record.cancelled = True

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
        # The bodies of runs a shard's leader handed this worker that outlived
        # their hard limit, by task id, until each exits: a retry of the task
        # handed back here waits for it.
        self._held_abandoned: dict[str, asyncio.Future[None]] = {}
        # Errors of members that failed here for good, by the member's task id.
        self._member_errors: dict[str, BaseException] = {}

    # submitted

    def submit(
        self,
        definition: TaskDefinition,
        payload: bytes,
        options: SubmissionOptions,
        settings: SubmissionSettings,
        source_version: int,
    ) -> TaskHandle:
        """Refuses with `RunStoppedError` once stopping; otherwise submits to
        the native runtime and registers the handle under one lock, so a
        worker that claims the task at once finds it. `source_version` is the
        version of the schema `payload` is at."""
        with self._lock:
            if self._stopping:
                raise RunStoppedError("the run is stopping, so no more tasks are accepted")
            task_id = self._runtime.submit(
                SubmitArguments(
                    definition.name,
                    source_version,
                    payload,
                    definition.kind.value,
                    # A coalescing task always has a key: the default is "".
                    (options.key or "") if definition.kind is TaskKind.COALESCING else None,
                    options,
                    settings,
                    retries=settings.retries,
                    drop_oldest=definition.drop_oldest,
                    non_retriable=not definition.retriable,
                    result_ttl_ms=settings.result_ttl_ms,
                )
            )
            handle = TaskHandle(
                task_id,
                self.cancel,
                self._run_callback,
                shard_id=self._runtime.shard_id(),
                replicable=not self._runtime.certifies_locally,
                status_reader=self.status,
            )
            self._records[task_id] = _Record(handle, definition.name)
        return handle

    @staticmethod
    def _cancelled(outcome: CancelOutcome) -> bool:
        match outcome:
            case CancelOutcome.CANCELLED:
                return True
            case CancelOutcome.ALREADY_FINISHED | CancelOutcome.UNKNOWN_TASK:
                return False
            case unknown:
                raise RuntimeError(f"the native runtime answered a cancel with {unknown!r}")

    # the handle's canceller

    def cancel(self, task_id: str) -> bool:
        """Asks the runtime to cancel `task_id`, which then tells this process
        through an event; says whether it was cancelled. A task that returned
        a step is cancelled by cancelling the continuation it runs as, which
        the leader does."""
        with self._lock:
            record = self._records.get(task_id)
            outcome = self._runtime.cancel(task_id)
            # `None`: sent to a leader elsewhere, which decides.
            cancelled = outcome is None or self._cancelled(outcome)
            if cancelled and record is not None:
                record.cancel_requested = True
            return cancelled

    # replicated, refused, concluded, status

    def replicated(self, task_id: str) -> None:
        """A networked leader stored the task on a quorum of its shard."""
        with self._lock:
            record = self._records.get(task_id)
        if record is not None:
            record.handle._mark_stored()

    def refused(self, task_id: str, error: BaseException) -> None:
        """The leader refused the task for good: it was never stored."""
        with self._lock:
            record = self._records.get(task_id)
        if record is not None:
            record.handle._mark_stored(error)
        self.failed(task_id, error)

    def concluded(self, task_id: str, error: BaseException) -> None:
        """A networked leader decided the task ended without a result here,
        so it was stored, and its handle fails with `error`."""
        self.replicated(task_id)
        self.failed(task_id, error)

    async def status(self, task_id: str) -> TaskStatus:
        """How the task stands, by the newest record its runtime reaches."""
        return TaskStatus.of(task_id, await self._runtime.task_record(task_id))

    # detached

    def detached(self, task_id: str) -> Run:
        """A run of `task_id` that a shard's leader handed this worker, which
        settles no handle and is not kept in the table: its record's handle
        is one nobody holds. It starts no body before an abandoned one of an
        earlier run of the task here has exited."""
        record = _Record(TaskHandle(task_id), "")
        with self._lock:
            record.abandoned = self._held_abandoned.get(task_id)
        return Run(task_id, record, 1, holds_handle=False)

    def held_body_abandoned(self, task_id: str, exited: "asyncio.Future[None]") -> None:
        """The body of a run handed this worker outlived its hard limit: a
        later run of the task here waits for `exited`."""
        with self._lock:
            self._held_abandoned[task_id] = exited

        def forget(_: object) -> None:
            with self._lock:
                if self._held_abandoned.get(task_id) is exited:
                    del self._held_abandoned[task_id]

        exited.add_done_callback(forget)

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

    def released(self, run: Run) -> None:
        """The claim was given back to the leader without running: the task
        is queued again, not started, and its handle waits for whatever the
        leader decides (another worker's run, or the end of the task).

        A task the leader already ended is left as it is. Giving a claim back
        reports it to the leader before this is called, and a dropped task's
        event can settle the handle in that same call, so the task may be
        settled by the time its run is released."""
        with self._lock:
            record = run._record
            if not record.settled and run._generation == record.generation:
                record.started = False

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

    def result_held(self, task_id: str, run_id: str, result: bytes) -> None:
        """Keeps the result of the run `run_id` until the leader certifies a
        run, while the task is open."""
        with self._lock:
            record = self._records.get(task_id)
            if record is not None and not record.settled:
                record.provisional[run_id] = result

    def certified(self, certification: Certification) -> None:
        """Settles the task with the result held for the certified run if the
        leader certified that result's digest. Fails it with
        `ResultUnavailableError` if no result of that run reached this
        process, and with `CertificationError` if the one held differs. A
        certification of a task already settled changes nothing."""
        with self._lock:
            record = self._records.get(certification.task_id)
            if record is None or record.settled:
                return
            record.settled = True
            self._retire(certification.task_id, record)
        # A leader decided it, so it was stored: said before the handle settles.
        record.handle._mark_stored()
        result = record.provisional.get(certification.task_run_id)
        if result is None:
            record.handle._fail(
                ResultUnavailableError(
                    f"task {certification.task_id} succeeded, but its result did not reach this "
                    "process",
                    certification.task_id,
                )
            )
            return
        if result_digest(result) != certification.result_digest:
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

    # continuing, continuation over

    def continuing(self, task_id: str, composite_id: str) -> None:
        """The task's run was certified as the step it returned, which now
        runs as the composite `composite_id`: its handle waits for that."""
        with self._lock:
            record = self._records.get(task_id)
            if record is not None:
                record.continuation = composite_id

    def continuation_over(self, task_id: str) -> TaskHandle | None:
        """The task's continuation is over: settles the task, once, and
        returns its handle for the caller to resolve or fail with the
        continuation's end, or `None` if it was settled already."""
        with self._lock:
            record = self._records.get(task_id)
            if record is not None:
                record.continuation = None
        return self._settle(task_id)

    def continuations(self) -> list[tuple[str, str]]:
        """The (task id, composite id) of every task still waiting for its continuation."""
        with self._lock:
            return [
                (task_id, record.continuation)
                for task_id, record in self._records.items()
                if record.continuation is not None and not record.settled
            ]

    # members that failed here

    def member_failed(self, task_id: str, error: BaseException) -> None:
        """A run of the member task `task_id` of a composite failed here for
        good with `error`. Kept, until taken or dropped, for whoever awaits the
        composite to be given it as it was."""
        with self._lock:
            self._member_errors[task_id] = error

    def member_error(self, task_id: str | None) -> BaseException | None:
        """The error the run of member `task_id` failed with here, taken;
        `None` if no run of it failed here, or it was taken or dropped."""
        if task_id is None:
            return None
        with self._lock:
            return self._member_errors.pop(task_id, None)

    def drop_member_errors(self) -> None:
        """Nobody awaits a composite any more: the errors kept for their
        members will not be asked for."""
        with self._lock:
            self._member_errors.clear()

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
                record.provisional.clear()
                return
            # `stop_unstarted` had already failed everything not started, and
            # nothing will claim this retry.
            record.settled = True
            self._retire(task_id, record)
        record.handle._fail(_run_stopped(record.handle, "this task's retry could start"))

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
            handle._fail(_run_stopped(handle, "this task could start"))

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
