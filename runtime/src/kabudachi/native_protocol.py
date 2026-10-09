"""What a session needs of the native runtime, and what the runtime hands it.

These are protocols, so the runtime's `NativeRuntime` and a test double both
satisfy them. They live apart from `session` so that `task_table` can name them
without importing `session`, which imports `task_table`.
"""

from typing import Literal, Protocol, runtime_checkable

from kabudachi._native import CancelOutcome, EventKind


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
    compaction: bool
    """A compaction run: `chain` holds the payloads to fold, oldest first;
    `serialized_input` is empty."""
    reconnect_timeout_ms: int
    """The run's reconnect timeout, resolved: its task's own, or the shard's."""


class Event(Protocol):
    """Something the runtime decided on its own, such as that a task expired."""

    kind: EventKind
    task_id: str
    task_run_id: str
    was_running: bool
    superseded_by: str | None
    active: bool
    """For `EventKind.SLOW_DOWN`: whether it was raised (`True`) or cleared."""
    reason: str | None
    """For `EventKind.REFUSED`: why the leader refused the submission."""
    seconds_left: float
    """For `EventKind.ABORT`: seconds until the run may be run again elsewhere."""


class Certification(Protocol):
    """The leader's word that a run's result is the authoritative one."""

    task_id: str
    task_run_id: str
    result_digest: bytes


@runtime_checkable
class Runtime(Protocol):
    """What a session needs of the native runtime."""

    delivers_results: bool
    """Whether certifications come back to this process, so a handle can be
    settled with its run's result. `False` for a worker of a networked shard,
    whose reports go to a leader that may be another process: the reports
    below then return `None`, and its runs settle no handle here."""

    def shard_id(self) -> str:
        """The incarnation of the shard this runtime founded at start."""
        ...

    def submit(
        self,
        definition_id: str,
        source_version: int,
        serialized_input: bytes,
        queue: str,
        kind: Literal["task", "ephemeral", "coalescing"],
        key: str | None,
        *,
        retries: int = 0,
        delay_ms: int | None = None,
        expires_in_ms: int | None = None,
        drop_oldest: bool = False,
        reconnect_timeout_ms: int | None = None,
    ) -> str:
        """Records a new task. `kind` is its `TaskKind` value; a coalescing
        task always has a `key` (its default is ""), and no other kind has
        one. Raises `ValueError` if `kind` and `key` disagree, or
        `reconnect_timeout_ms` is 0, and `BackpressureError` past the hard
        memory limit. `reconnect_timeout_ms`, when given, is the run's own
        reconnect timeout."""

    async def claim_pending(self, limit: int) -> list[Claim]:
        ...

    async def next_events(self) -> list[Event]:
        ...

    def report_started(self, task_run_id: str) -> None:
        ...

    def cancel(self, task_id: str) -> CancelOutcome | None:
        """Cancels a task, and says how that ended. `None`: the cancel was
        sent, and the leader decides."""

    def report_failure(self, task_run_id: str, failure_kind: str) -> bool | None:
        """Reports that a claimed run failed, started or not; whether a retry
        is now queued. `None` when the leader decides elsewhere."""

    def report_lost(self, task_run_id: str) -> bool | None:
        """Reports that a claimed run was lost with the process that ran it;
        whether a new attempt now waits. Raises `RuntimeError` if the run is
        not this worker's to lose. `None` when the leader decides elsewhere."""

    def complete(
        self, task_run_id: str, result_digest: bytes, continues: bool = False
    ) -> Certification | None:
        """Reports that a running run succeeded; its certification. `None`
        when the leader decides elsewhere."""

    def complete_compaction(self, task_run_id: str, folded: bytes) -> bool | None:
        """Reports the fold of a compaction run; whether the leader applied
        it. `None` when the leader decides elsewhere."""

    def end_continuation(self, task_id: str) -> bool:
        """Ends the continuation of a task that returned a step; whether there was one."""
