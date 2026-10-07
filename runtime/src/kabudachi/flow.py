"""Fixed inputs and sequential composition: `.bind()` and `flow`.

Only the sequencing lives here: each stage is an ordinary task submitted when
its predecessor has finished, so retries, timeouts and queues stay per stage
and nothing is inherited between stages.
"""

from typing import Any

from kabudachi.composites import FlowHandle, GroupHandle
from kabudachi.config import UNSET
from kabudachi.errors import TaskDefinitionError
from kabudachi.handle import TaskHandle
from kabudachi.registry import TaskDefinition
from kabudachi.session import Session, current_session


def _message_fields(input_type: Any) -> dict[str, Any]:
    """The fields of a protobuf message type, or `TypeError` if `input_type`
    is not one."""
    descriptor = getattr(input_type, "DESCRIPTOR", None)
    if not isinstance(input_type, type) or descriptor is None:
        raise TypeError(
            "fields can only be bound on a task whose input is a protobuf message, "
            f"not {input_type!r}"
        )
    return descriptor.fields_by_name


def _check_arguments(name: str, needs_input: bool, arguments: tuple[Any, ...]) -> None:
    wanted = 1 if needs_input else 0
    if len(arguments) != wanted:
        raise TypeError(f"{name} takes {wanted} argument(s) once bound, not {len(arguments)}")


class BoundTask:
    """A task with some or all of its input fixed, or a plain task as a stage
    of a flow. Calling it submits the task.

    Fully bound (`task.bind(value)`): the input is `value`, so it is called
    with no argument. Overlaid (`task.bind(field=value, ...)`): it is called
    with a message, which gets those fields set on a copy.
    """

    is_step = True
    is_task = True
    """Its handle gives the task's result itself, not a list of results."""

    def __init__(
        self,
        definition: TaskDefinition,
        value: Any = UNSET,
        fields: dict[str, Any] | None = None,
    ) -> None:
        self.definition = definition
        self._value = value
        self._fields = fields or {}

    @property
    def needs_input(self) -> bool:
        """Whether a call, or the previous stage of a flow, has to supply an input."""
        return self._value is UNSET

    @property
    def output_type(self) -> Any:
        """What the step gives the next one; unknown for a task that returns a
        step, whose handle resolves to the continuation's list of results."""
        return None if self.definition.continues else self.definition.output_type

    def input_requirements(self) -> list[tuple[str, Any]]:
        """The (name, type) of each task that takes the input of this step."""
        return [(self.definition.name, self.definition.input_type)] if self.needs_input else []

    def definitions(self) -> list[TaskDefinition]:
        """Every task this step runs."""
        return [self.definition]

    def describe(self, serializers: Any) -> Any:
        """This step as plain data, the same every time it is built the same
        way, so the leader can certify it by digest."""
        if not self.needs_input:
            serializer = serializers.get(self.definition.serializer)
            payload = serializer.encode(self._value, self.definition.input_type).hex()
            return {"task": self.definition.name, "value": payload}
        fields = {name: repr(value) for name, value in sorted(self._fields.items())}
        return {"task": self.definition.name, "fields": fields}

    def __call__(self, *arguments: Any) -> TaskHandle:
        self.check_arguments(arguments)
        return self.start(current_session(), arguments[0] if arguments else UNSET)

    def check_arguments(self, arguments: tuple[Any, ...]) -> None:
        """Raises `TypeError` unless `arguments` is what the bound task takes.

        That is one input, or none if the task is fully bound.
        """
        _check_arguments(self.definition.name, self.needs_input, arguments)

    def start(self, session: Session, previous: Any) -> TaskHandle:
        """Submits the task on `previous`, the input it was given, which a
        fully bound task ignores."""
        return session.submit(self.definition, self._input(previous))

    def _input(self, previous: Any) -> Any:
        if not self.needs_input:
            return self._value
        if not self._fields:
            return previous
        merged = type(previous)()
        merged.CopyFrom(previous)
        for name, value in self._fields.items():
            setattr(merged, name, value)
        return merged


def bind_task(
    definition: TaskDefinition, serializers: Any, arguments: tuple[Any, ...], fields: dict[str, Any]
) -> BoundTask:
    """Checks a `.bind()` and builds the bound task. `serializers` is where
    the task's serializer is looked up to check a bound value."""
    if arguments and fields:
        raise TypeError("bind a value or fields, not both")
    if len(arguments) > 1:
        raise TypeError("bind takes at most one value, the whole input")
    if not arguments and not fields:
        raise TypeError("bind needs a value, or fields to set")
    if arguments:
        serializer = serializers.get(definition.serializer)
        serializer.encode(arguments[0], definition.input_type)
        return BoundTask(definition, value=arguments[0])
    known = _message_fields(definition.input_type)
    for name, value in fields.items():
        field = known.get(name)
        if field is None:
            raise ValueError(f"{definition.input_type.__name__} has no field {name!r}")
        if field.is_repeated or field.cpp_type == field.CPPTYPE_MESSAGE:
            raise TypeError(f"only scalar fields can be bound, and {name!r} is not one")
        definition.input_type(**{name: value})  # refuses a value of the wrong type
    return BoundTask(definition, fields=dict(fields))


class Flow:
    """Steps that run one after another, each on the output of the one before."""

    is_step = True
    is_task = False

    def __init__(self, stages: list[Any]) -> None:
        self.stages = stages

    @property
    def needs_input(self) -> bool:
        return self.stages[0].needs_input

    @property
    def output_type(self) -> Any:
        """Unknown: a flow resolves to the list of its stages' results."""
        return None

    def input_requirements(self) -> list[tuple[str, Any]]:
        """The (name, type) of each task the flow's input goes to.

        Used to type-check the step before it.
        """
        return self.stages[0].input_requirements()

    def definitions(self) -> list[TaskDefinition]:
        return [definition for stage in self.stages for definition in stage.definitions()]

    def describe(self, serializers: Any) -> Any:
        """The flow as plain data, the same every time it is built the same way."""
        return {"flow": [stage.describe(serializers) for stage in self.stages]}

    def check_arguments(self, arguments: tuple[Any, ...]) -> None:
        """Raises `TypeError` unless `arguments` is what the flow takes."""
        _check_arguments("this flow", self.needs_input, arguments)

    def __call__(self, *arguments: Any) -> FlowHandle:
        self.check_arguments(arguments)
        return current_session().composites.submit_flow(
            self, arguments[0] if arguments else UNSET
        )

    def start(self, session: Session, previous: Any) -> FlowHandle:
        return session.composites.submit_flow(self, previous if self.needs_input else UNSET)


ON_ERROR_POLICIES = ("fail_fast", "collect_all")


def _policy(on_error: str) -> str:
    if on_error not in ON_ERROR_POLICIES:
        raise ValueError(f"on_error must be one of {ON_ERROR_POLICIES}, not {on_error!r}")
    return on_error


class Group:
    """Steps that run side by side on the same input. Awaiting the group gives
    the list of their results in member order. With `on_error="fail_fast"`
    the first failure is raised; with `"collect_all"` a failure is an entry of
    the list, as with `asyncio.gather(return_exceptions=True)`."""

    is_step = True
    is_task = False

    def __init__(self, members: list[Any], on_error: str) -> None:
        self.members = members
        self.on_error = on_error

    @property
    def needs_input(self) -> bool:
        return any(member.needs_input for member in self.members)

    @property
    def output_type(self) -> Any:
        """Unknown: a group resolves to the list of its members' results."""
        return None

    def input_requirements(self) -> list[tuple[str, Any]]:
        """The (name, type) of each member task that takes the group's input."""
        return [need for member in self.members for need in member.input_requirements()]

    def definitions(self) -> list[TaskDefinition]:
        return [definition for member in self.members for definition in member.definitions()]

    def describe(self, serializers: Any) -> Any:
        """The group as plain data, the same every time it is built the same way."""
        members = [member.describe(serializers) for member in self.members]
        return {"group": members, "on_error": self.on_error}

    def check_arguments(self, arguments: tuple[Any, ...]) -> None:
        """Raises `TypeError` unless `arguments` is what the group takes."""
        _check_arguments("this group", self.needs_input, arguments)

    def __call__(self, *arguments: Any) -> GroupHandle:
        self.check_arguments(arguments)
        return current_session().composites.submit_group(self, arguments[0] if arguments else UNSET)

    def start(self, session: Session, previous: Any) -> GroupHandle:
        return session.composites.submit_group(self, previous)


class MapStep:
    """A task run on every item of a list, as a group of independent tasks
    whose results keep the order of the items. Called with the
    list, or used as a flow stage that takes the prior stage's list."""

    is_step = True
    is_task = False
    needs_input = True
    output_type = None

    def __init__(self, definition: TaskDefinition, serializers: Any) -> None:
        self.definition = definition
        self._serializers = serializers

    def input_requirements(self) -> list[tuple[str, Any]]:
        """The (name, type) of the task, whose input must be a list of what it takes."""
        return [(self.definition.name, list[self.definition.input_type])]

    def definitions(self) -> list[TaskDefinition]:
        return [self.definition]

    def describe(self, serializers: Any) -> Any:
        """The map as plain data, the same every time it is built the same way."""
        return {"map": self.definition.name}

    def check_arguments(self, arguments: tuple[Any, ...]) -> None:
        """Raises `TypeError` unless `arguments` is the one list map takes."""
        _check_arguments(f"{self.definition.name}.map", True, arguments)

    def __call__(self, inputs: Any, *, on_error: str = "fail_fast") -> GroupHandle:
        return current_session().composites.submit_group(self._group(inputs, on_error), UNSET)

    def start(self, session: Session, previous: Any) -> GroupHandle:
        return session.composites.submit_group(self._group(previous, "fail_fast"), UNSET)

    def _group(self, inputs: Any, on_error: str) -> "Group":
        """A group with one fully bound task per item. Every item is checked
        before any is submitted, so a bad one leaves nothing half done."""
        if not isinstance(inputs, (list, tuple)):
            raise TypeError(f"map takes a list of inputs, not {inputs!r}")
        serializer = self._serializers.get(self.definition.serializer)
        for item in inputs:
            serializer.encode(item, self.definition.input_type)
        return Group([BoundTask(self.definition, value=item) for item in inputs], _policy(on_error))


def _steps(kind: str, steps: tuple[Any, ...]) -> list[Any]:
    """`steps` as steps: a plain task becomes an unbound one."""
    converted = []
    for step in steps:
        if getattr(step, "is_step", False):
            pass
        elif isinstance(getattr(step, "definition", None), TaskDefinition):
            step = BoundTask(step.definition)  # a plain task is an unbound stage
        else:
            raise TypeError(f"a {kind} takes tasks, bound tasks, flows and groups, not {step!r}")
        converted.append(step)
    return converted


def _fits(produced: Any, wanted: Any) -> bool:
    if produced == wanted:
        return True
    return isinstance(produced, type) and isinstance(wanted, type) and issubclass(produced, wanted)


def flow(*stages: Any) -> Flow:
    """A flow of `stages`, which are tasks, bound tasks, groups or flows: the
    first is called with the flow's argument, each later one with the output
    of the one before, and calling the flow gives a handle that resolves to
    the list of their results, in order (a group or flow stage gives a nested
    list).

    A stage that is fully bound ignores the output before it. Raises
    `TypeError` for a stage that is not a step, `ValueError` for no stages,
    and `TaskDefinitionError` if a task's output type does not fit the next
    task's input.
    """
    if not stages:
        raise ValueError("a flow needs at least one stage")
    steps = _steps("flow", stages)
    for before, after in zip(steps, steps[1:]):
        produced = before.output_type
        if produced is None:
            continue  # a group or flow gives a list whose members are not checked
        for name, wanted in after.input_requirements():
            if not _fits(produced, wanted):
                raise TaskDefinitionError(
                    f"{before.definition.name} returns {produced!r}, "
                    f"which {name} cannot take as its input {wanted!r}"
                )
    return Flow(steps)


def group(*members: Any, on_error: str = "fail_fast") -> Group:
    """Steps that run side by side, each on the same input; awaiting the group
    gives the list of their results in member order.

    `on_error="fail_fast"` (the default) raises the first failure, like
    `asyncio.gather`: the group is then failed and, like `gather`, the other
    members are left to finish. `"collect_all"` returns a list in which a
    failed member's entry is its error. Cancelling a group cancels every
    member. Raises `ValueError` for no members or an unknown policy.
    """
    if not members:
        raise ValueError("a group needs at least one member")
    return Group(_steps("group", members), _policy(on_error))


STEP_TYPES = (BoundTask, Flow, Group)


def is_step_type(annotation: Any) -> bool:
    """Whether a task declared to return `annotation` returns a step, which
    continues as an implicit flow."""
    return isinstance(annotation, type) and issubclass(annotation, STEP_TYPES)
