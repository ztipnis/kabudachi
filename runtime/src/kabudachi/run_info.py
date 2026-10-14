"""The run a task body belongs to, as context variables set where the body
runs: in its asyncio task for an async body, and in its thread for a
synchronous one, which starts with a copy of that context."""

import contextvars
from dataclasses import dataclass


@dataclass(frozen=True)
class RunInfo:
    """The run of a task whose body is running now."""

    task_id: str
    run_id: str
    task_name: str
    attempt: int
    """1 for a task's first run, then one more for each retry."""
    shard: str
    """The name of the shard the worker serves (`local` for a shard of one)."""
    worker_id: str
    trace_id: str = ""
    """Empty: no trace is recorded yet."""
    span_id: str = ""
    """Empty: no trace is recorded yet."""


_current: contextvars.ContextVar["RunInfo | None"] = contextvars.ContextVar(
    "kabudachi_current_run", default=None
)


def current_context() -> RunInfo | None:
    """The run of the task body calling this, with its hooks; `None` outside one."""
    return _current.get()


def run_context(info: RunInfo) -> None:
    """Makes `info` the current run, in the context this is called in."""
    _current.set(info)
