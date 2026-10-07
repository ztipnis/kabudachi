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


class Event(Protocol):
    """Something the runtime decided on its own, such as that a task expired."""

    kind: EventKind
    task_id: str
    task_run_id: str
    was_running: bool
    superseded_by: str | None
    active: bool
    """For `EventKind.SLOW_DOWN`: whether it was raised (`True`) or cleared."""


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
        kind: Literal["task", "ephemeral", "coalescing"],
        key: str | None,
        *,
        retries: int = 0,
        delay_ms: int | None = None,
        expires_in_ms: int | None = None,
        drop_oldest: bool = False,
    ) -> str:
        """Records a new task. `kind` is its `TaskKind` value; a coalescing
        task always has a `key` (its default is ""), and no other kind has
        one. Raises `ValueError` if they disagree, and `BackpressureError`
        past the hard memory limit."""

    async def claim_pending(self, limit: int) -> list[Claim]:
        ...

    async def next_events(self) -> list[Event]:
        ...

    def report_started(self, task_run_id: str) -> None:
        ...

    def cancel(self, task_id: str) -> CancelOutcome:
        """Cancels a task, and says how that ended."""

    def report_failure(self, task_run_id: str, failure_kind: str) -> bool:
        """Reports that a claimed run failed, started or not; whether a retry
        is now queued."""

    def complete(
        self, task_run_id: str, result_digest: bytes, continues: bool = False
    ) -> Certification:
        ...

    def complete_compaction(self, task_run_id: str, folded: bytes) -> bool:
        """Reports the fold of a compaction run; whether the leader applied it."""

    def end_continuation(self, task_id: str) -> bool:
        """Ends the continuation of a task that returned a step; whether there was one."""
