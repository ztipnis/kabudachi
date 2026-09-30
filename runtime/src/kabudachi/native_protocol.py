"""What a session needs of the native runtime, and what the runtime hands it.

These are protocols, so the runtime's `NativeRuntime` and a test double both
satisfy them. They live apart from `session` so that `task_table` can name them
without importing `session`, which imports `task_table`.
"""

from typing import Protocol, runtime_checkable


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
