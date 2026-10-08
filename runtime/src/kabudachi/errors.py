"""The exceptions kabudachi raises on purpose."""

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


class TaskLostError(KabudachiError, RuntimeError):
    """The process running a task died before it finished, and the task is
    not run again: an ephemeral task is never replayed, and a non-retriable
    one may already have had its effects."""


class TaskBodyError(KabudachiError, RuntimeError):
    """A task body in a task process raised an error that could not be sent
    back as it was. `kind` is the type name of the original error; the
    message is its text."""

    def __init__(self, message: str, kind: str) -> None:
        super().__init__(message)
        self.kind = kind

    def __reduce__(self) -> tuple[Any, tuple[str, str]]:
        return (TaskBodyError, (str(self), self.kind))


def interrupted(subject: str, error: BaseException) -> TaskInterruptedError:
    """The error that settles a task, flow or group which something other than
    its own failure ended: a cancellation, an interrupt, anything worse than
    an exception. `subject` names what was interrupted, and only the type name
    of `error` is repeated, never its message, which can hold task input."""
    return TaskInterruptedError(f"{subject} was interrupted by {type(error).__name__}")
