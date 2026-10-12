"""How durable a submitted task is, and how a task, flow or group stands."""

import enum
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from kabudachi._native import RunState


class Durability(enum.IntEnum):
    """Where a submitted task is kept, from least to most durable.

    `IN_MEMORY`: only this process holds it (the one-node runtime, or a
    networked submission no leader has stored yet). `REPLICATED`: a networked
    leader stored it on a quorum of its shard. `DR_STORED`: copied to a
    disaster-recovery store, which no runtime offers yet.
    """

    IN_MEMORY = 1
    REPLICATED = 2
    DR_STORED = 3


_RUN_STATE_NAMES = {
    getattr(RunState, name): name.lower()
    for name in (
        "SCHEDULED",
        "QUEUED",
        "CLAIMED",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
        "EXPIRED",
        "SUPERSEDED",
        "CANCELLED",
        "LOST",
        "ORPHANED",
    )
}
_PENDING = {RunState.SCHEDULED, RunState.QUEUED, RunState.CLAIMED}


def _moment(unix_millis: int) -> datetime | None:
    if unix_millis == 0:
        return None
    return datetime.fromtimestamp(unix_millis / 1000, tz=timezone.utc)


@dataclass(frozen=True)
class TaskStatus:
    """How a task stood when its record was read. Read from the record its
    shard holds, not from the leader, so a change the leader has just made
    may not show yet.

    `state` is "unknown" (no record was found), "pending" (waiting to run or
    claimed), "running", or how its newest run ended ("succeeded",
    "failed", "expired", "superseded", "cancelled", "lost", "orphaned").
    `run_state` is the newest run's state, `attempt` its attempt number
    (1 for the first), and `delivered` whether its result reached the worker
    awaiting it. Times are UTC, by the clocks of the workers that stamped
    them.
    """

    task_id: str
    state: str
    run_state: RunState | None
    attempt: int
    submitted_at: datetime | None
    updated_at: datetime | None
    delivered: bool

    @classmethod
    def of(cls, task_id: str, record: Any) -> "TaskStatus":
        """The status `record`, a native task record or `None`, shows."""
        if record is None or not record.runs:
            return cls(task_id, "unknown", None, 0, None, None, False)
        run = record.runs[-1]
        state = "pending" if run.state in _PENDING else _RUN_STATE_NAMES[run.state]
        return cls(
            task_id,
            state,
            run.state,
            run.attempt_number,
            _moment(record.submitted_at_ms),
            _moment(run.updated_at_ms),
            run.delivered,
        )


@dataclass(frozen=True)
class CompositeStatus:
    """How a flow or group stood when it was asked. `state` is "running",
    "succeeded", "failed" or "cancelled"; `counts` its stages or members
    by "pending" (not started), "running", "succeeded", "failed" and
    "cancelled", only those with any; `stage`, for a flow, the index of the
    stage it is at or ended on."""

    composite_id: str
    kind: str
    state: str
    counts: Mapping[str, int]
    stage: int | None = None
