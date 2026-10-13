"""The exceptions kabudachi raises on purpose."""

import signal
import threading
from typing import Any

# Defined by the native module, which raises BackpressureError itself;
# re-exported so callers catch them as kabudachi.errors.*.
from kabudachi._native import BackpressureError as BackpressureError
from kabudachi._native import KabudachiError as KabudachiError


class ConfigurationError(KabudachiError, ValueError):
    """A setting is unknown, or its value is not acceptable."""


class TaskDefinitionError(KabudachiError, ValueError):
    """A function cannot be a task, or an option of the decorator is invalid."""


class DuplicateTaskError(TaskDefinitionError):
    """Two tasks were given the same name."""


class ReduceEmptyError(KabudachiError, TypeError):
    """A reduce of no items was given no initial value, so it has no result.
    A `TypeError`, as `functools.reduce` raises for the same case."""


class UnknownSerializerError(KabudachiError, LookupError):
    """No serializer is registered under the requested name."""


class DuplicateSerializerError(KabudachiError, ValueError):
    """Two serializers were registered under the same name."""


class SerializerUnavailableError(KabudachiError, RuntimeError):
    """A serializer is registered but cannot run here, for example because
    the library it needs is not installed."""


class SerializationError(KabudachiError, ValueError):
    """A value cannot be encoded, or bytes cannot be decoded, as the type
    that was asked for."""


class PayloadTooLargeError(SerializationError):
    """A task's encoded input is larger than the hard memory limit of the
    process that must hold it, so it was not submitted: this one's
    `memory_hard_limit`, or the shard leader's. Submitting it again is
    refused again. `size` and `limit` are in bytes, `None` where not known
    here (a leader's refusal names neither)."""

    def __init__(self, message: str, size: int | None = None, limit: int | None = None) -> None:
        super().__init__(message)
        self.size = size
        self.limit = limit

    def __reduce__(self) -> tuple[Any, tuple[str, int | None, int | None]]:
        # Crosses the pipe from a task process with its sizes.
        return (PayloadTooLargeError, (str(self), self.size, self.limit))


class RuntimeNotStartedError(KabudachiError, RuntimeError):
    """A task was submitted while `kabudachi.run()` was not running."""


class UnknownTaskError(KabudachiError, LookupError):
    """A worker was given a task that no task in this process is named for."""


class CertificationError(KabudachiError, RuntimeError):
    """The leader certified a result that is not the one this process holds,
    so the result is not delivered."""


class RunStoppedError(KabudachiError, RuntimeError):
    """A task could not start, or cannot be submitted, because the run that
    would have executed it is stopping."""


class TaskInterruptedError(KabudachiError, RuntimeError):
    """A task's body was interrupted, for example cancelled, before it
    finished, so it has no result."""


class TaskExpiredError(KabudachiError, RuntimeError):
    """A task was still waiting to start when its expiry passed, so it was
    never run."""


class TaskRecordFullError(KabudachiError, RuntimeError):
    """A task's run ended and no further attempt was made, because the
    task's record had no room left for one."""


class CoalescedPayloadTooLargeError(KabudachiError, RuntimeError):
    """A coalescing task's superseded payloads, folded together, grew larger
    than one worker can be sent, so its newest generation can never run."""


class TaskTimeoutError(KabudachiError, TimeoutError):
    """A task ran past its timeout, so its run failed. The body was asked to
    stop, and if it did not stop within the cancel grace it was abandoned:
    nothing it does afterwards counts."""


class TaskCancelledError(KabudachiError, RuntimeError):
    """A task was cancelled before it finished, so it has no result."""


class TaskSupersededError(KabudachiError, RuntimeError):
    """A coalescing task was still pending when a newer generation with the
    same key replaced it, so it never ran: its payload was folded into
    `superseded_by`, the newer generation's task ID."""

    def __init__(self, message: str, superseded_by: str) -> None:
        super().__init__(message)
        self.superseded_by = superseded_by

    def __reduce__(self) -> tuple[Any, tuple[str, str]]:
        return (TaskSupersededError, (str(self), self.superseded_by))


class StartupError(KabudachiError, RuntimeError):
    """The worker's task processes could not start: one could not import the
    task modules, did not find the same tasks as the worker, or exited first."""


class TaskOutcomeError(KabudachiError, RuntimeError):
    """A task ended without a result for a reason outside its own body: its
    run was lost, or its outcome is unknown."""


class TaskLostError(TaskOutcomeError):
    """The process running a task died before it finished, and the task is
    not run again: an ephemeral task is never replayed."""


class TaskOrphanedError(TaskOutcomeError):
    """A task that is not safe to run twice was running when its worker was
    lost, so whether its effects happened is unknown, and it is not run
    again."""


class TaskBodyError(KabudachiError, RuntimeError):
    """A task body in a task process raised an error that could not be sent
    back as it was. `kind` is the type name of the original error; the
    message is its text."""

    def __init__(self, message: str, kind: str) -> None:
        super().__init__(message)
        self.kind = kind

    def __reduce__(self) -> tuple[Any, tuple[str, str]]:
        return (TaskBodyError, (str(self), self.kind))


class TaskFailure(KabudachiError):
    """A member of a `collect_all` group or map that failed: its entry in the
    group's results, and what the next stage is given in its place. `kind`
    is the error's type name and `message` its message, empty when the run
    was remote and gave none."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(f"{kind}: {message}" if message else kind)
        self.kind = kind
        self.message = message

    def __reduce__(self) -> tuple[Any, tuple[str, str]]:
        # Crosses the pipe from a task process.
        return (TaskFailure, (self.kind, self.message))


class ResultUnavailableError(KabudachiError, RuntimeError):
    """A task's outcome is known to its shard but its result cannot be had
    here: it never reached this process (its worker could not reach this
    one), or the shard no longer knows the task. `task_id` names it."""

    def __init__(self, message: str, task_id: str) -> None:
        super().__init__(message)
        self.task_id = task_id

    def __reduce__(self) -> tuple[Any, tuple[str, str]]:
        # Crosses the pipe from a task process with its task id.
        return type(self), (str(self), self.task_id)


class ResultTooLargeError(KabudachiError, RuntimeError):
    """A task's result is larger than the hard memory limit of the process that
    must hold it whole: the worker that ran it (the run failed) or this one,
    awaiting it."""


class DurabilityUnavailableError(KabudachiError, RuntimeError):
    """A task cannot reach the durability asked for here: the one-node
    runtime keeps tasks only in memory, and no disaster-recovery store
    exists."""


def interrupted(subject: str, error: BaseException) -> TaskInterruptedError:
    """The error that settles a task, flow or group which something other than
    its own failure ended: a cancellation, an interrupt, anything worse than
    an exception. `subject` names what was interrupted, and only the type name
    of `error` is repeated, never its message, which can hold task input."""
    return TaskInterruptedError(f"{subject} was interrupted by {type(error).__name__}")


def body_error(error: BaseException) -> TaskBodyError:
    """`error`, which is not an `Exception`, as a `TaskBodyError` of its kind
    that a run can fail with: raised as it is, it would end whoever awaits
    the run instead."""
    kind = type(error).__name__
    return TaskBodyError(f"{kind}: {error}" if str(error) else kind, kind)


def may_be_ctrl_c() -> bool:
    """Whether a `KeyboardInterrupt` raised here can be a real Ctrl-C: Python
    raises one only on the main thread, and never while SIGINT is ignored."""
    return (
        threading.current_thread() is threading.main_thread()
        and signal.getsignal(signal.SIGINT) is not signal.SIG_IGN
    )
