"""Every task this process knows, by its stable name."""

import enum
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from kabudachi.config import UNSET, Unset
from kabudachi.errors import DuplicateTaskError
from kabudachi.serializers import Serializer


class TaskKind(enum.Enum):
    """The delivery semantics a task was declared with."""

    TASK = "task"
    EPHEMERAL = "ephemeral"
    COALESCING = "coalescing"


@dataclass(frozen=True)
class TaskDefinition:
    """What a task is, fixed when its module is imported. The callable never
    leaves this process; everything else identifies the task to others."""

    name: str
    func: Callable[..., Any]
    kind: TaskKind
    serializer: str
    version: int
    queue: str | Unset
    input_type: Any
    output_type: Any
    is_async: bool
    retries: int = 0
    """How many times a failed run is replaced by a new attempt."""
    timeout: timedelta | None = None
    """How long a run may take before it is asked to stop. `None` is no limit."""
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

    def types_unsupported_by(self, serializer: Serializer) -> list[tuple[str, Any]]:
        """The ("input" | "return", type) pairs `serializer` cannot encode. A
        task that returns a step has its return left out: a returned step is
        never encoded."""
        checked = [("input", self.input_type)]
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
