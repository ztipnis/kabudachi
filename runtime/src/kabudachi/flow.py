"""Fixed inputs and sequential composition: `.bind()` and `flow`.

A step describes itself as a tree the shard's leader runs: each stage is an
ordinary task the leader submits when its predecessor has finished, so
retries, timeouts and queues stay per stage and nothing is inherited between
stages.
"""

import types
import typing
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from kabudachi import schemas
from kabudachi.composites import FlowHandle, GroupHandle, ReduceHandle
from kabudachi.config import UNSET
from kabudachi.errors import ReduceEmptyError, TaskFailure
from kabudachi.handle import TaskHandle
from kabudachi.registry import TaskDefinition, TaskKind
from kabudachi.schemas import UNKNOWN, Each, Either, Items, OutputShape, Value
from kabudachi.serializers import process_serializers
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


def _field_kind(input_type: Any, name: str) -> str:
    """The kind a description gives the bound field `name` of `input_type`."""
    field = _message_fields(input_type)[name]
    if field.cpp_type in (field.CPPTYPE_INT32, field.CPPTYPE_INT64, field.CPPTYPE_ENUM):
        return "int"
    if field.cpp_type in (field.CPPTYPE_UINT32, field.CPPTYPE_UINT64):
        return "uint"
    if field.cpp_type in (field.CPPTYPE_FLOAT, field.CPPTYPE_DOUBLE):
        return "double"
    if field.cpp_type == field.CPPTYPE_BOOL:
        return "bool"
    return "bytes" if field.type == field.TYPE_BYTES else "string"


def _refuse_coalescing(definition: TaskDefinition) -> None:
    if definition.kind is TaskKind.COALESCING:
        raise TypeError(
            "a coalescing task cannot be a stage of a flow, group or map: "
            "its generations would replace each other across them"
        )


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

    def output_shape(self) -> OutputShape:
        """What the step gives the next one; unknown for a task that returns a
        step, whose handle resolves to the continuation's list of results."""
        if self.definition.continues:
            return UNKNOWN
        return Value(self.definition.output_type, self.definition.serializer)

    def check_types(self) -> None:
        """Nothing: a task's own output is not next to any other step, so the
        checks happen between the stages of the flow or group that holds it."""

    def input_requirements(self) -> list[tuple[str, Any, str]]:
        """The (name, type, serializer name) of each task that takes the input of this step."""
        if not self.needs_input:
            return []
        return [(self.definition.name, self.definition.input_type, self.definition.serializer)]

    def definitions(self) -> list[TaskDefinition]:
        """Every task this step runs."""
        return [self.definition]

    def describe(self, session: Session, previous: Any = UNSET) -> tuple:
        """This step as the tree `_native.describe_step` encodes: built from
        values and field values only, never from `repr`, so the same step
        always encodes to the same bytes. `previous`, when given, is the
        input this step takes, encoded here as its whole input."""
        definition = self.definition
        serializer = session.serializers.get(definition.serializer)
        value: bytes | None = None
        if not self.needs_input:
            value = serializer.encode(self._value, definition.input_type)
        elif previous is not UNSET:
            value = serializer.encode(self._input(previous), definition.input_type)
        fields = []
        if value is None:
            fields = [
                (name, _field_kind(definition.input_type, name), field_value)
                for name, field_value in sorted(self._fields.items())
            ]
        settings = session.submission_settings(definition)
        version = session.schemas.version_of(definition.input_type)
        return ("task", definition.name, version, settings, value, fields)

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
        return _bind_value(definition, serializers, arguments[0])
    return _bind_fields(definition, fields)


def _bind_value(definition: TaskDefinition, serializers: Any, value: Any) -> BoundTask:
    serializer = serializers.get(definition.serializer)
    serializer.encode(value, definition.input_type)
    return BoundTask(definition, value=value)


def _bind_fields(definition: TaskDefinition, fields: dict[str, Any]) -> BoundTask:
    known = _message_fields(definition.input_type)
    for name, value in fields.items():
        _check_bound_field(definition, known.get(name), name, value)
    return BoundTask(definition, fields=dict(fields))


def _check_bound_field(definition: TaskDefinition, field: Any, name: str, value: Any) -> None:
    if field is None:
        raise ValueError(f"{definition.input_type.__name__} has no field {name!r}")
    if field.is_repeated or field.cpp_type == field.CPPTYPE_MESSAGE:
        raise TypeError(f"only scalar fields can be bound, and {name!r} is not one")
    definition.input_type(**{name: value})  # refuses a value of the wrong type


class Flow:
    """Steps that run one after another, each on the output of the one before."""

    is_step = True
    is_task = False
    handle_type = FlowHandle

    def __init__(self, stages: list[Any]) -> None:
        self.stages = stages

    @property
    def needs_input(self) -> bool:
        return self.stages[0].needs_input

    def output_shape(self) -> OutputShape:
        """A flow resolves to the list of its stages' results, in order."""
        return Items(tuple(stage.output_shape() for stage in self.stages))

    def check_types(self) -> None:
        """Raises `TypeError` unless every stage's output fits the stage after
        it, here and in the steps nested inside."""
        for before, after in zip(self.stages, self.stages[1:]):
            _check_adjacent(before, after)
        for stage in self.stages:
            stage.check_types()

    def input_requirements(self) -> list[tuple[str, Any, str]]:
        """The (name, type, serializer name) of each task the flow's input goes to.

        Used to type-check the step before it.
        """
        return self.stages[0].input_requirements()

    def definitions(self) -> list[TaskDefinition]:
        return [definition for stage in self.stages for definition in stage.definitions()]

    def describe(self, session: Session, previous: Any = UNSET) -> tuple:
        """The flow as a tree, as `BoundTask.describe` has a task; only its
        first stage is bound to `previous`."""
        first, *rest = self.stages
        stages = [first.describe(session, previous)]
        stages.extend(stage.describe(session) for stage in rest)
        return ("flow", "fail_fast", stages)

    def check_arguments(self, arguments: tuple[Any, ...]) -> None:
        """Raises `TypeError` unless `arguments` is what the flow takes."""
        _check_arguments("this flow", self.needs_input, arguments)

    def __call__(self, *arguments: Any) -> FlowHandle:
        self.check_arguments(arguments)
        return self.start(current_session(), arguments[0] if arguments else UNSET)

    def start(self, session: Session, previous: Any) -> FlowHandle:
        return session.composites.submit(self, previous if self.needs_input else UNSET)


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
    handle_type = GroupHandle

    def __init__(self, members: list[Any], on_error: str) -> None:
        self.members = members
        self.on_error = on_error

    @property
    def needs_input(self) -> bool:
        return any(member.needs_input for member in self.members)

    def output_shape(self) -> OutputShape:
        """A group resolves to the list of its members' results, in order,
        each of which is the failure under `collect_all` if the member failed."""
        return Items(tuple(_result_shape(member.output_shape(), self.on_error) for member in self.members))

    def check_types(self) -> None:
        """Raises `TypeError` unless the steps nested in the members fit together."""
        for member in self.members:
            member.check_types()

    def input_requirements(self) -> list[tuple[str, Any, str]]:
        """The (name, type, serializer name) of each member task that takes the group's input."""
        return [need for member in self.members for need in member.input_requirements()]

    def definitions(self) -> list[TaskDefinition]:
        return [definition for member in self.members for definition in member.definitions()]

    def describe(self, session: Session, previous: Any = UNSET) -> tuple:
        """The group as a tree, as `BoundTask.describe` has a task; every
        member that takes an input is bound to `previous`."""
        members = [member.describe(session, previous) for member in self.members]
        return ("group", self.on_error, members)

    def describe_streamed(self) -> tuple:
        """The head of the group as it is streamed: its members follow in
        batches."""
        return ("group", self.on_error, [], "streamed")

    def members_described(self, session: Session, previous: Any) -> list[tuple]:
        """Each member as a tree, bound to `previous` as `describe` binds it.
        All at once, so a `previous` that does not fit raises here, and later
        changes to it do not reach members not yet sent."""
        return [member.describe(session, previous) for member in self.members]

    def check_arguments(self, arguments: tuple[Any, ...]) -> None:
        """Raises `TypeError` unless `arguments` is what the group takes."""
        _check_arguments("this group", self.needs_input, arguments)

    def __call__(self, *arguments: Any) -> GroupHandle:
        self.check_arguments(arguments)
        return self.start(current_session(), arguments[0] if arguments else UNSET)

    def start(self, session: Session, previous: Any) -> GroupHandle:
        return session.composites.submit(self, previous)


@dataclass(frozen=True)
class MappedList:
    """A map called with its list: `definition` run on every item, in order."""

    definition: TaskDefinition
    items: list[Any]
    on_error: str


class MapStep:
    """A task run on every item of a list, as a group of independent tasks
    whose results keep the order of the items. Called with the
    list, or used as a flow stage that takes the prior stage's list."""

    is_step = True
    is_task = False
    handle_type = GroupHandle
    needs_input = True

    def __init__(self, definition: TaskDefinition, serializers: Any, on_error: str = "fail_fast") -> None:
        _refuse_coalescing(definition)
        self.definition = definition
        self._serializers = serializers
        self._on_error = on_error

    def output_shape(self) -> OutputShape:
        """A list whose every item is the task's result, or its failure under `collect_all`."""
        return Each(_result_shape(Value(self.definition.output_type, self.definition.serializer), self._on_error))

    def check_types(self) -> None:
        """Nothing: a map's items are all its one task's, so there is no
        adjacency inside it; what feeds it is checked by the enclosing flow."""

    def input_requirements(self) -> list[tuple[str, Any, str]]:
        """The (name, type, serializer name) of the task, whose input must be a list of what it takes."""
        return [(self.definition.name, list[self.definition.input_type], self.definition.serializer)]

    def definitions(self) -> list[TaskDefinition]:
        return [self.definition]

    def describe(self, session: Session, previous: Any = UNSET) -> tuple:
        """The map as a tree, as `BoundTask.describe` has a task: one bound
        member per item of `previous`, or, with none, the one unbound member
        that is expanded once the stage before it has produced its list."""
        if previous is UNSET:
            members = [BoundTask(self.definition)]
        else:
            members = self._members(previous)
        return ("map", self._on_error, [member.describe(session) for member in members])

    def check_arguments(self, arguments: tuple[Any, ...]) -> None:
        """Raises `TypeError` unless `arguments` is the one list map takes."""
        _check_arguments(f"{self.definition.name}.map", True, arguments)

    def __call__(self, inputs: Any, *, on_error: str = "fail_fast") -> GroupHandle:
        return self.start(current_session(), inputs, _policy(on_error))

    def start(self, session: Session, previous: Any, on_error: str = "fail_fast") -> GroupHandle:
        """Submits the map over the list `previous`, streaming its items."""
        return session.composites.submit(
            MappedList(self.definition, self._checked(previous), _policy(on_error)), UNSET
        )

    def _checked(self, inputs: Any) -> list[Any]:
        """The items of `inputs`, each checked before any is submitted, so a
        bad one leaves nothing half done. Each is encoded here only to prove
        it can be, and the bytes are dropped: the stream encodes an item again
        as its batch is built, so the encoded items of a long list are never
        held at once, outside the payload memory the stream waits on, and the
        failure of a bad item stays a synchronous error rather than a failed
        handle."""
        if not isinstance(inputs, (list, tuple)):
            raise TypeError(f"map takes a list of inputs, not {inputs!r}")
        serializer = self._serializers.get(self.definition.serializer)
        for item in inputs:
            serializer.encode(item, self.definition.input_type)
        return list(inputs)

    def _members(self, inputs: Any) -> list[BoundTask]:
        """One fully bound task per item."""
        return [BoundTask(self.definition, value=item) for item in self._checked(inputs)]


DEFAULT_CHUNK_SIZE = 64
"""Items per chunk of a tree reduce, unless the caller says otherwise."""


class ReduceStep:
    """A reducer folded over a list, left to right, as one composite the
    shard runs: `await task.reduce(items)` gives the folded value. Also a flow
    stage that folds the list the stage before it gives: `task.reduce`, or
    `task.reduce.options(initial=..., chunk_size=...)`.
    """

    is_step = True
    is_task = False
    needs_input = True

    def __init__(
        self,
        definition: TaskDefinition,
        serializers: Any,
        initial: Any = UNSET,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or not 1 <= chunk_size < 2**32:
            raise ValueError(f"chunk_size must be a positive integer, not {chunk_size!r}")
        self.definition = definition
        self._serializers = serializers
        self._initial = initial
        self._chunk_size = chunk_size
        if initial is not UNSET:
            serializers.get(definition.serializer).encode(initial, definition.output_type)

    def output_shape(self) -> OutputShape:
        """The reducer's result, which is what the reduce gives."""
        return Value(self.definition.output_type, self.definition.serializer)

    def check_types(self) -> None:
        """A reduce joins nothing to check."""

    def input_requirements(self) -> list[tuple[str, Any, str]]:
        """The (name, type, serializer name) of the reducer, whose input must be a list of its items."""
        return [(self.definition.name, list[self.definition.input_type], self.definition.serializer)]

    def definitions(self) -> list[TaskDefinition]:
        return [self.definition]

    def options(self, *, initial: Any = UNSET, chunk_size: int = DEFAULT_CHUNK_SIZE) -> "ReduceStep":
        """This reduce seeded with `initial` (which may be `None`; leaving it
        out makes the first item the start) and with tree chunks of
        `chunk_size` items, for use as a flow stage."""
        return ReduceStep(self.definition, self._serializers, initial, chunk_size)

    def __call__(
        self, items: Any, *, initial: Any = UNSET, chunk_size: int = DEFAULT_CHUNK_SIZE
    ) -> ReduceHandle:
        """Folds `items` and returns the handle of the folded value. The items
        go to the shard in batches, however many there are. Raises
        `ReduceEmptyError` for no items and no `initial`."""
        return self.options(initial=initial, chunk_size=chunk_size).start(current_session(), items)

    def start(self, session: Session, previous: Any) -> ReduceHandle:
        """Submits the reduce of the list `previous`, streaming its items."""
        if not isinstance(previous, (list, tuple)):
            raise TypeError(f"reduce takes a list of items, not {previous!r}")
        if not previous and self._initial is UNSET:
            raise ReduceEmptyError("a reduce of no items needs an initial value")
        return session.composites.stream_reduce(self, previous)

    def describe(self, session: Session, previous: Any = UNSET) -> tuple:
        """This reduce as a flow stage, the tree `_native.describe_step`
        encodes: one stage that takes the list it is given."""
        if previous is not UNSET:
            raise TypeError("a reduce's items stream in; describe it with no list")
        return ("reduce", "fail_fast", [self._reducer(session)], self._described_options(session))

    def describe_streamed(self, session: Session) -> tuple:
        """The head of a reduce whose items stream in after it."""
        return ("reduce", "fail_fast", [], self._described_options(session), "streamed")

    def _reducer(self, session: Session) -> tuple:
        definition = self.definition
        settings = session.submission_settings(definition)
        version = session.schemas.version_of(definition.input_type)
        return ("task", definition.name, version, settings, None, [])

    def _described_options(self, session: Session) -> dict[str, Any]:
        definition = self.definition
        serializer = session.serializers.get(definition.serializer)
        initial = None if self._initial is UNSET else serializer.encode(self._initial, definition.output_type)
        return {
            "reducer": self._reducer(session),
            "tree": definition.reducer.tree,
            "chunk_size": self._chunk_size,
            "initial": initial,
        }


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
        if isinstance(step, BoundTask):
            _refuse_coalescing(step.definition)
        converted.append(step)
    return converted


def _result_shape(shape: OutputShape, on_error: str) -> OutputShape:
    """`shape`, or under `collect_all` either it or the failure in its place."""
    if on_error != "collect_all":
        return shape
    return Either((shape, Value(TaskFailure, "")))


def _type_text(annotation: Any) -> str:
    """A type as a reader writes it: `Greeting`, `list[Greeting | TaskFailure]`."""
    if annotation is type(None):
        return "None"
    origin = typing.get_origin(annotation)
    if origin is None:
        return annotation.__name__ if isinstance(annotation, type) else repr(annotation)
    arguments = typing.get_args(annotation)
    if origin in (typing.Union, types.UnionType):
        return " | ".join(_type_text(argument) for argument in arguments)
    return f"{getattr(origin, '__name__', repr(origin))}[{', '.join(_type_text(a) for a in arguments)}]"


def _shape_text(shape: OutputShape) -> str:
    """A shape as the type of what it gives the next step."""
    match shape:
        case Value(value_type, _):
            return _type_text(value_type)
        case Items(entries):
            return f"list[{_union_text(entries)}]"
        case Each(item):
            return f"list[{_shape_text(item)}]"
        case Either(options):
            return _union_text(options)
        case _:
            return "an unknown type"


def _union_text(shapes: tuple[OutputShape, ...]) -> str:
    texts = dict.fromkeys(_shape_text(shape) for shape in shapes)
    return " | ".join(texts)


def _describe(step: Any) -> str:
    """The tasks of a step as the caller wrote it: `to_receipt`, `group(shout, to_receipt)`."""
    if isinstance(step, Flow):
        return f"flow({', '.join(_describe(stage) for stage in step.stages)})"
    if isinstance(step, Group):
        return f"group({', '.join(_describe(member) for member in step.members)})"
    if isinstance(step, MapStep):
        return f"{step.definition.name}.map"
    return step.definition.name


def _check_adjacent(before: Any, after: Any) -> None:
    """Raises `TypeError` unless what `before` gives fits every task of `after`
    that takes it."""
    shape = before.output_shape()
    serializers = process_serializers()
    for name, wanted, serializer in after.input_requirements():
        if schemas.fits(shape, wanted, serializer, serializers) is False:
            raise TypeError(
                f"{_describe(before)} gives {_shape_text(shape)}, "
                f"which {name} cannot take as its input {_type_text(wanted)}"
            )


def flow(*stages: Any) -> Flow:
    """A flow of `stages`, which are tasks, bound tasks, groups or flows: the
    first is called with the flow's argument, each later one with the output
    of the one before, and calling the flow gives a handle that resolves to
    the list of their results, in order (a group or flow stage gives a nested
    list).

    A stage that is fully bound ignores the output before it. Raises
    `TypeError` for a stage that is not a step or whose output does not fit
    the input of the stage after it (a task's by its type, a group's or
    flow's by the types of its entries), and `ValueError` for no stages.
    """
    if not stages:
        raise ValueError("a flow needs at least one stage")
    steps = _steps("flow", stages)
    if isinstance(steps[0], ReduceStep):
        raise TypeError(
            f"{steps[0].definition.name}.reduce cannot be the first stage of a flow: "
            f"give it its items with {steps[0].definition.name}.reduce(items), "
            "or put it after a stage that returns a list"
        )
    built = Flow(steps)
    built.check_types()
    return built


def group(*members: Any, on_error: str = "fail_fast") -> Group:
    """Steps that run side by side, each on the same input; awaiting the group
    gives the list of their results in member order.

    `on_error="fail_fast"` (the default) raises the first failure, like
    `asyncio.gather`: the group is then failed and, like `gather`, the other
    members are left to finish. `"collect_all"` returns a list in which a
    failed member's entry is its error. Cancelling a group cancels every
    member. Raises `ValueError` for no members or an unknown policy, and
    `TypeError` for a member that is not a step or holds stages that do not
    fit together.
    """
    if not members:
        raise ValueError("a group needs at least one member")
    built = Group(_steps("group", members), _policy(on_error))
    built.check_types()
    return built


STEP_TYPES = (BoundTask, Flow, Group)


def is_step_type(annotation: Any) -> bool:
    """Whether a task declared to return `annotation` returns a step, which
    continues as an implicit flow."""
    return isinstance(annotation, type) and issubclass(annotation, STEP_TYPES)
