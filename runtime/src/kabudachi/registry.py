"""Every task this process knows, by its stable name."""

import enum
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from kabudachi.config import UNSET, Unset
from kabudachi.errors import DuplicateTaskError, UnknownTaskError
from kabudachi.serializers import Serializer


class TaskKind(enum.Enum):
    """The delivery semantics a task was declared with."""

    TASK = "task"
    EPHEMERAL = "ephemeral"
    COALESCING = "coalescing"


@dataclass(frozen=True)
class ReducerShape:
    """How a task of two parameters `(item, previous)` folds a list: the
    definition's `input_type` is the item's type and its `output_type` the
    type of the result so far."""

    previous_type: Any
    """The type of the second parameter, which is also what the task returns."""
    associative: bool
    """Whether the fold may be evaluated as a tree: items and results are of
    one type, and the combine of two adjacent results is the reducer itself."""
    tree: bool
    """Whether a reduce folds chunks of the list side by side and combines
    adjacent results, rather than one item after another."""
    merge: Callable[[Any, Any], Any] | None
    """For a tree that is not associative: combines the results of two
    adjacent branches, `merge(left, right)`."""


@dataclass(frozen=True)
class TaskDefinition:
    """What a task is, fixed when its module is imported. The callable never
    leaves this process; everything else identifies the task to others."""

    name: str
    func: Callable[..., Any]
    kind: TaskKind
    serializer: str
    queue: str | Unset
    input_type: Any
    output_type: Any
    is_async: bool
    module: str
    """The module that declared the task, which a task process imports to
    find it; empty if the callable names none."""
    retries: int | Unset = UNSET
    """How many times a failed run is replaced by a new attempt; `UNSET`
    takes the process's `retries`."""
    retriable: bool = True
    """Whether a run that was running when its worker or task process was
    lost may be run again. `False` for a task not safe to run twice: such a
    run is orphaned instead, and the task is over."""
    timeout: timedelta | None | Unset = UNSET
    """How long a run may take before it is asked to stop. `None` is no
    limit; `UNSET` takes the process's `timeout`."""
    result_ttl: timedelta | None | Unset = UNSET
    """How long the task is kept once finished: `None` keeps no result,
    `UNSET` takes its queue's `result_ttl`, else the process's."""
    cancel_grace: timedelta | Unset = UNSET
    """How long a run has to stop once asked, or `UNSET` for the process's."""
    continues: bool = False
    """Whether the task returns a flow, group or bound task, which runs as its
    continuation (an implicit flow) instead of being its result."""
    merge: Callable[[Any, Any], Any] | None = None
    """For a coalescing task: combines (older, newer) payloads; `None` keeps
    the newest."""
    drop_oldest: bool = False
    """For a coalescing task: whether a submission that would pass the hard
    memory limit drops this key's oldest retained payloads to fit, instead of
    being refused."""
    recycle_process: bool = False
    """Whether a task process that runs this task takes no more runs after
    it and is replaced by a fresh one once its runs finish: for a body that
    leaves its process unfit for more work."""
    reconnect_timeout: timedelta | None = None
    """How long a run may go without its worker being heard, past the time
    it takes to suspect a silent worker, before the leader replays it.
    `None` leaves it to the queue's setting, else the shard's."""
    reducer: ReducerShape | None = None
    """For a task of two parameters `(item, previous)`: how it folds a list.
    `None` for a task of one."""
    raise_on_failed_migration: bool = False
    """Whether a task whose input this worker cannot migrate (a migration
    raised, or a bump is declared incompatible) is left for another worker
    instead of dropped. Left, it fails with `MigrationError` after
    `version_block_ttl`; dropped, it fails with `MigrationError` at once and
    the worker logs a warning."""

    def __reduce__(self) -> tuple[Any, tuple[str]]:
        # Sent between a worker and its task processes by name: both imported
        # the same task, and its function is code, not data.
        return (registered_definition, (self.name,))

    def types_unsupported_by(self, serializer: Serializer) -> list[tuple[str, Any]]:
        """The ("input" | "previous" | "return", type) pairs `serializer`
        cannot encode. A task that returns a step has its return left out: a
        returned step is never encoded."""
        checked = [("input", self.input_type)]
        if self.reducer is not None:
            checked.append(("previous", self.reducer.previous_type))
        if not self.continues:
            checked.append(("return", self.output_type))
        return [
            (role, value_type) for role, value_type in checked if not serializer.supports(value_type)
        ]


class TaskRegistry:
    """Tasks by name, in the order they were registered."""

    def __init__(self) -> None:
        self._definitions: dict[str, TaskDefinition] = {}

    def register(self, definition: TaskDefinition) -> None:
        """Adds a task. Raises `DuplicateTaskError` if the name is taken, and
        then keeps the task that was there."""
        if definition.name in self._definitions:
            raise DuplicateTaskError(f"a task named {definition.name!r} is already registered")
        self._definitions[definition.name] = definition

    def get(self, name: str) -> TaskDefinition | None:
        """The task registered under `name`, or `None` if there is none."""
        return self._definitions.get(name)

    def definitions(self) -> list[TaskDefinition]:
        """Every registered task, in the order they were registered."""
        return list(self._definitions.values())

    def __contains__(self, name: object) -> bool:
        return name in self._definitions


_default_registry = TaskRegistry()


def default_registry() -> TaskRegistry:
    """The registry the task decorators add to."""
    return _default_registry


def registered_definition(name: str) -> TaskDefinition:
    """The task this process registered as `name`. Raises `UnknownTaskError`
    if there is none."""
    definition = default_registry().get(name)
    if definition is None:
        raise UnknownTaskError(f"this process has no task named {name!r}")
    return definition
