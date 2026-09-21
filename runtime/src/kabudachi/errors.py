"""The exceptions kabudachi raises on purpose."""


class KabudachiError(Exception):
    """Base class of every exception kabudachi raises on purpose."""


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


class BackpressureError(KabudachiError, RuntimeError):
    """A task was not submitted because the scheduler holds as much pending
    work as its hard memory limit allows. Nothing was queued; submit again
    once running tasks have finished."""
