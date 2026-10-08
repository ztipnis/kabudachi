"""The frames a worker and its task processes send each other.

Both ends are this package and the pipe never leaves the machine, so frames
are pickled by the pipe itself. A task's input and result travel inside them
as the bytes its serializer made; only control data, continuation steps and
the values and errors of nested calls are pickled as they are. An error or
value that would not arrive whole is replaced by a `TaskBodyError` naming its
type.
"""

import logging
import pickle
import traceback
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

from kabudachi.errors import TaskBodyError
from kabudachi.options import SubmissionOptions

_logger = logging.getLogger("kabudachi")

# The worker to a task process.


@dataclass(frozen=True)
class Run:
    """Run a body on this input, folded onto the superseded payloads in `chain`."""

    run_id: str
    definition_id: str
    source_version: int
    serialized_input: bytes
    chain: tuple[bytes, ...]
    queue: str
    """The queue the task was sent to, which picks the run hooks."""
    attempt: int
    """1 for the task's first run, then one more for each retry."""


@dataclass(frozen=True)
class Compact:
    """Fold these payloads with the task's merge."""

    run_id: str
    definition_id: str
    payloads: tuple[bytes, ...]


@dataclass(frozen=True)
class Cancel:
    """Ask this run's body to stop."""

    run_id: str


@dataclass(frozen=True)
class Drain:
    """No more runs will come: exit once every body has exited."""


@dataclass(frozen=True)
class Reply:
    """The answer to a `Submit` (the new task's id and shard id) or to a
    `CancelTask` (whether it cancelled), or the error it raised."""

    request: int
    value: Any = None
    error: BaseException | None = None


@dataclass(frozen=True)
class WaitDone:
    """A task a body here submitted has its outcome."""

    task_id: str
    value: Any = None
    error: BaseException | None = None


# A task process to the worker.


@dataclass(frozen=True)
class Ready:
    """The tasks (name to version) and serializers this process found, or
    why it could not import its modules."""

    definitions: dict[str, int]
    serializers: tuple[str, ...]
    error: str | None = None
    hooks: tuple[str, ...] = ()
    """The lifecycle hooks this process found, each as `<kind> hook <name>`."""


@dataclass(frozen=True)
class Result:
    """A run's encoded result, or the pickled step a task that continues returned."""

    run_id: str
    payload: bytes | None
    step: bytes | None = None


@dataclass(frozen=True)
class Failed:
    """A run's body, or a compaction's fold, raised `error`."""

    run_id: str
    error: BaseException
    traceback: str


@dataclass(frozen=True)
class Importing:
    """About to import `module`; the last one sent names where a process
    that never became ready was stuck."""

    module: str


@dataclass(frozen=True)
class Exited:
    """A run's body has really stopped; always after its `Result` or `Failed`."""

    run_id: str


@dataclass(frozen=True)
class Waiting:
    """A run's body is waiting for tasks it called, and gives its place back meanwhile."""

    run_id: str


@dataclass(frozen=True)
class Recycle:
    """An `after_run` hook raised, so this process may hold what it failed
    to clean up: send it no new runs; it exits once its runs finish."""


@dataclass(frozen=True)
class Resumed:
    """A run's body stopped waiting and takes its place back."""

    run_id: str


@dataclass(frozen=True)
class Submit:
    """Submit a task for a body here (`definition_id`, `payload`, `options`),
    or a flow or group (`composite` is "flow" or "group"; `step` and
    `previous` are pickled)."""

    request: int
    definition_id: str | None = None
    payload: bytes | None = None
    options: SubmissionOptions | None = None
    composite: str | None = None
    step: bytes | None = None
    previous: bytes | None = None


@dataclass(frozen=True)
class CancelTask:
    """Cancel a task a body here submitted."""

    request: int
    task_id: str


@dataclass(frozen=True)
class Compacted:
    """A compaction's folded payload."""

    run_id: str
    payload: bytes


def read_frames(connection: Any, deliver: Callable[[Any], None]) -> None:
    """Delivers every frame read from `connection`, in order, and returns once
    the other end has closed or the pipe has broken, a frame cut short
    included."""
    while True:
        try:
            frame = connection.recv()
        except (EOFError, OSError):
            return
        except Exception as error:
            # A frame that cannot be unpickled: nothing after it can be trusted.
            _logger.warning(
                "a task process pipe carried a frame that could not be read: %s",
                type(error).__name__,
            )
            return
        deliver(frame)


def portable(error: BaseException) -> BaseException:
    """`error` if it survives pickling whole, else a `TaskBodyError` with its
    type name and text."""
    try:
        pickle.loads(pickle.dumps(error))
    except Exception:
        return TaskBodyError(f"{type(error).__name__}: {error}", type(error).__name__)
    return error


def failed(run_id: str, error: BaseException) -> Failed:
    return Failed(run_id, portable(error), "".join(traceback.format_exception(error)))


def outcome_of(task_id: str, outcome: "Future[Any]") -> WaitDone:
    """The `WaitDone` for a settled handle's outcome."""
    error = outcome.exception()
    if error is not None:
        return WaitDone(task_id, error=portable(error))
    value = outcome.result()
    try:
        pickle.loads(pickle.dumps(value))
    except Exception as unsendable:
        kind = type(unsendable).__name__
        return WaitDone(
            task_id,
            error=TaskBodyError(
                f"the result of task {task_id} cannot be sent to a task process: {kind}", kind
            ),
        )
    return WaitDone(task_id, value)
