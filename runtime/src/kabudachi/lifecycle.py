"""What one task is going through in this process, from its submission until
its handle is settled.

A task has one handle, settled once, but its runs come and go: a run can be
cancelled while it still executes, fail and be queued again, or be certified as
the step of a continuation. `Session` holds one `TaskLifecycle` per task it
submitted and holds here the state, and the transition rules that need no I/O.
The `CancelledError` handling and the order in which calls are made stay in
`Session`.

This does no I/O, takes no lock and settles no handle: `Session` calls its
methods under its own lock and does what they decide.
"""

import enum
from dataclasses import dataclass
from typing import Any

from kabudachi.handle import TaskHandle


class RetryOutcome(enum.Enum):
    """What a queued retry means for the task's handle."""

    WAIT = "wait"
    STOPPED = "stopped"
    SETTLED = "settled"


@dataclass
class TaskLifecycle:
    """The state of one task in this process."""

    # A detached lifecycle has no handle and is always settled, so the handle is
    # never dereferenced for it.
    handle: TaskHandle | None
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
    body: Any = None
    # A body that outlived its hard limit, and that a retry waits for.
    abandoned: Any = None
    # The continuation of a task that returned a step, until it is over.
    continuation: Any = None
    # Bumped by each `begin_run`, so a retry's `end_run` cannot be undone by an
    # abandoned earlier run finishing later (both await the same future).
    _generation: int = 0

    @classmethod
    def detached(cls, definition_name: str = "") -> "TaskLifecycle":
        """A settled lifecycle with no handle, for a run whose task was settled
        before the run began."""
        return cls(None, definition_name, settled=True)

    def claim(self) -> bool:
        """A worker loop claimed a run of this task. Says whether it may
        start: it may not if the handle is settled already."""
        if self.settled:
            return False
        self.started = True
        return True

    def begin_run(self) -> int:
        """A run of this task began here. Returns a generation token: pass it
        to `end_run` so an abandoned earlier run, finishing after a retry has
        already begun a new one, cannot clear the new run's state."""
        self.active = True
        self._generation += 1
        return self._generation

    def end_run(self, generation: int) -> None:
        """The run that `begin_run` gave `generation` for has ended, however
        it did. A stale generation (a later run has already begun) is a no-op:
        that run's own `end_run` owns the cleanup."""
        if generation != self._generation:
            return
        self.active = False
        self.cancelled = False
        self.body = None

    def request_cancel(self) -> None:
        """The leader accepted a cancel this process asked for."""
        self.cancel_requested = True

    def cancelled_by_leader(self) -> None:
        """The leader said the task was cancelled. A run in progress no longer
        counts: its outcome is suppressed."""
        if self.active:
            self.cancelled = True

    @property
    def outcome_counts(self) -> bool:
        """Whether what a run ends with still matters, as it does not once the
        task is cancelled."""
        return not (self.cancelled or self.cancel_requested)

    def stop_unstarted(self) -> bool:
        """The run is stopping. Says whether this task must fail now, which it
        must if it is open and has not started, including while it waits for a
        retry."""
        if self.started or self.settled:
            return False
        self.settled = True
        return True

    def retry_queued(self, stopping: bool) -> RetryOutcome:
        """The leader queued the task's next attempt. `WAIT`: the handle stays
        open until it runs. `STOPPED`: the run is stopping and nothing will
        claim it, so the handle must fail. `SETTLED`: nothing to do."""
        if self.settled:
            return RetryOutcome.SETTLED
        if stopping:
            self.settled = True
            return RetryOutcome.STOPPED
        self.started = False
        self.provisional_result = None
        return RetryOutcome.WAIT

    def settle(self) -> bool:
        """The handle is being settled. Says whether this is the first time,
        so a task is settled once however many things try."""
        if self.settled:
            return False
        self.settled = True
        return True

    def hold(self, result: bytes) -> None:
        """Keeps a run's result until the leader certifies it, while the task is open."""
        if not self.settled:
            self.provisional_result = result

    @property
    def retirable(self) -> bool:
        """Whether `Session` can forget this task: it is settled and neither a
        run nor a continuation of it is still going."""
        return self.settled and not self.active and self.continuation is None
