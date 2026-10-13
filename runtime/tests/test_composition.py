"""Steps that run one after another or side by side: `.bind()` fixes inputs,
`flow()` runs stages in sequence, `group()` runs members together, and a task
that returns a step runs it as its continuation after its own run is certified,
the original handle resolving to the continuation's results. Python-side
properties are checked too: payloads are folded in order whatever the
compaction points, and the stage after a group runs exactly once."""

import asyncio
import dataclasses
import functools
import pickle
import threading
from types import SimpleNamespace

import pydantic
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from kabudachi import _native, schemas
from kabudachi import serializers as serializer_registry
from kabudachi._native import KabudachiError
from kabudachi.body import fold_payloads
from kabudachi.composites import INLINE_VALUE_BYTES, _batches
from kabudachi.errors import (
    BackpressureError,
    PayloadTooLargeError,
    ReduceEmptyError,
    RunStoppedError,
    RuntimeNotStartedError,
    SerializationError,
    TaskBodyError,
    TaskCancelledError,
    TaskDefinitionError,
    TaskFailure,
    UnknownTaskError,
)
from kabudachi.flow import BoundTask, Flow, Group, MapStep, flow, group
from kabudachi.registry import TaskKind, TaskRegistry
from kabudachi.schemas import SchemaRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.session import Session
from kabudachi.tasks import Task
from proto_messages import Greeting, Receipt
from session_world import WAIT, Pickle, World, activated, run, with_events


def shout(request: Greeting) -> Greeting:
    return Greeting(text=request.text.upper(), times=request.times)


def wrap(request: Greeting) -> Greeting:
    return Greeting(text=f"[{request.text}]", times=request.times)


def declare(function):
    return Task(
        function,
        registry=TaskRegistry(),
        serializers=SerializerRegistry.with_defaults(),
        name=f"tests.{function.__name__}",
    )


def to_receipt(request: Greeting) -> Receipt:
    return Receipt(ok=request.times > 0)


def numbers(request: list[Greeting]) -> list[Greeting]:
    return request


# --- bind ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("bind", "needs_input"),
    [
        pytest.param(lambda: declare(shout).bind(Greeting(text="x")), False, id="a value binds the whole input"),
        pytest.param(lambda: declare(shout).bind(text="fixed"), True, id="fields overlay the input"),
    ],
)
def test_a_bound_task_takes_an_input_only_when_the_binding_leaves_fields_open(bind, needs_input):
    bound = bind()

    assert bound.needs_input is needs_input
    with pytest.raises(TypeError):
        bound(Greeting()) if not needs_input else bound()
    with pytest.raises(RuntimeNotStartedError):
        bound(Greeting(times=3)) if needs_input else bound()


def test_a_bound_task_is_checked_when_it_is_bound():
    task = declare(shout)

    with pytest.raises(ValueError, match="nope"):
        task.bind(nope=1)
    with pytest.raises(SerializationError):
        task.bind("not a greeting")
    with pytest.raises(TypeError):
        task.bind()
    with pytest.raises(TypeError):
        task.bind(Greeting(), text="x")
    with pytest.raises(TypeError):
        task.bind(Greeting(), Greeting())
    with pytest.raises(TypeError):
        declare(numbers).bind(text="x")
    with pytest.raises((TypeError, ValueError)):
        task.bind(times="many")


# --- describe -----------------------------------------------------------


def test_a_step_describes_the_same_bytes_every_time_and_a_bound_field_keeps_its_type():
    world = World(shout, wrap)
    inner = group(world.tasks["shout"].bind(times=0, text="b"), world.tasks["wrap"])
    pipeline = flow(world.tasks["shout"].bind(Greeting(text="a")), inner)
    first = _native.describe_step(pipeline.describe(world.session))
    again = _native.describe_step(
        flow(
            world.tasks["shout"].bind(Greeting(text="a")),
            group(world.tasks["shout"].bind(text="b", times=0), world.tasks["wrap"]),
        ).describe(world.session)
    )
    assert first == again, "field order and object identity do not change the description"
    [_, _, [bound, nested]] = pipeline.describe(world.session)
    assert bound[4] == Greeting(text="a").SerializeToString()
    [_, _, [overlaid, _]] = nested
    assert overlaid[5] == [("text", "string", "b"), ("times", "int", 0)]


COALESCING_STAGE = (
    "a coalescing task cannot be a stage of a flow, group or map: "
    "its generations would replace each other across them"
)


def test_a_coalescing_task_is_refused_as_a_stage_member_or_map_when_the_step_is_composed():
    registry, serializers = TaskRegistry(), SerializerRegistry.with_defaults()

    def refresh(request: Greeting) -> Greeting:
        return request

    coalescing = Task(
        refresh, registry=registry, serializers=serializers, name="tests.refresh", kind=TaskKind.COALESCING
    )
    plain = declare(shout)
    for compose in (
        lambda: flow(plain, coalescing),
        lambda: flow(coalescing.bind(Greeting(text="a"))),
        lambda: group(plain, coalescing),
        lambda: coalescing.map,
    ):
        with pytest.raises(TypeError) as refused:
            compose()
        assert str(refused.value) == COALESCING_STAGE


def test_a_task_name_cannot_take_the_reserved_prefix():
    def refresh(request: Greeting) -> Greeting:
        return request

    with pytest.raises(TaskDefinitionError, match="kabudachi."):
        Task(
            refresh,
            registry=TaskRegistry(),
            serializers=SerializerRegistry.with_defaults(),
            name="kabudachi.expand",
        )


def test_a_task_named_by_default_cannot_land_in_the_reserved_prefix():
    def refresh(request: Greeting) -> Greeting:
        return request

    refresh.__module__ = "kabudachi.extras"
    refresh.__qualname__ = "refresh"

    with pytest.raises(TaskDefinitionError, match="kabudachi."):
        Task(refresh, registry=TaskRegistry(), serializers=SerializerRegistry.with_defaults())


def test_a_task_failure_keeps_its_kind_and_message_across_the_task_process_pipe():
    failure = pickle.loads(pickle.dumps(TaskFailure("ValueError", "bad input")))

    assert isinstance(failure, KabudachiError)
    assert (failure.kind, failure.message) == ("ValueError", "bad input")


# --- flow definition ----------------------------------------------------


def test_a_flow_needs_at_least_one_stage_and_only_takes_tasks():
    with pytest.raises(ValueError):
        flow()
    with pytest.raises(TypeError):
        flow(shout)
    with pytest.raises(TypeError):
        flow("not a task")


class OrderV1(pydantic.BaseModel):
    text: str


class OrderV2(pydantic.BaseModel):
    text: str
    priority: int = 0


class OrderV3(pydantic.BaseModel):
    text: str


class RenamedV4(pydantic.BaseModel):
    title: str


class RushV1(OrderV1):
    pass


class ParcelModel(pydantic.BaseModel):
    weight: int


@dataclasses.dataclass
class ParcelRecord:
    weight: int


def order_schemas() -> SchemaRegistry:
    """Four classes as versions of one family, with an incompatible bump into the last."""
    registry = SchemaRegistry()
    registry.declare(OrderV1, version=1, family="shop.Order")
    registry.declare(OrderV2, version=2, family="shop.Order")
    registry.declare(OrderV3, version=3, family="shop.Order")
    registry.declare(RenamedV4, version=4, family="shop.Order", migration=None)
    registry.declare(ParcelModel, version=1, family="shop.Parcel")
    registry.declare(ParcelRecord, version=2, family="shop.Parcel")
    return registry


def greetings_of(request: Greeting) -> list[Greeting]:
    return [request]


def count(request: list[Greeting]) -> Receipt:
    return Receipt(ok=bool(request))


def tally(request: list[Receipt]) -> Receipt:
    return Receipt(ok=bool(request))


def anything(request: list) -> Receipt:
    return Receipt(ok=bool(request))


def outcomes(request: list[Greeting | TaskFailure]) -> Receipt:
    return Receipt(ok=bool(request))


def numbers_of(request: Greeting) -> list[Receipt]:
    return [Receipt(ok=True)]


def make_v1(request: OrderV1) -> OrderV1:
    return request


def take_v2(request: OrderV2) -> OrderV2:
    return request


def take_v3(request: OrderV3) -> OrderV3:
    return request


def take_v4(request: RenamedV4) -> RenamedV4:
    return request


def make_rush(request: OrderV1) -> RushV1:
    return RushV1(text=request.text)


def take_v1(request: OrderV1) -> OrderV1:
    return request


def take_optional(request: OrderV1 | None) -> OrderV1:
    return request or OrderV1(text="")


def make_parcel(request: ParcelModel) -> ParcelModel:
    return request


def take_record(request: ParcelRecord) -> ParcelRecord:
    return request


# Backends other than protobuf: a union of a message and a failure is not a protobuf type.
SERIALIZER_OF = {
    make_v1: "pydantic",
    take_v2: "pydantic",
    take_v3: "pydantic",
    take_v4: "pydantic",
    make_rush: "pydantic",
    take_v1: "pydantic",
    take_optional: "pickle",
    make_parcel: "pydantic",
    take_record: "dataclass",
    anything: "pickle",
    outcomes: "pickle",
}


def collecting_map(bound):
    return MapStep(bound.definition, SerializerRegistry.with_defaults(), "collect_all")


def declare_with_serializer(function):
    serializers = SerializerRegistry.with_defaults()
    serializers.register(Pickle())
    return Task(
        function,
        registry=TaskRegistry(),
        serializers=serializers,
        name=f"tests.{function.__name__}",
        serializer=SERIALIZER_OF.get(function, "protobuf"),
    )


FIT_CASES = [
    # (stages, a name the TypeError mentions, or None when the stages fit)
    pytest.param(lambda d: (d(to_receipt), d(shout)), "to_receipt", id="a message of another family"),
    pytest.param(lambda d: (d(shout), d(wrap)), None, id="the same type"),
    pytest.param(lambda d: (d(to_receipt), group(d(shout))), "to_receipt", id="every group member is checked"),
    pytest.param(lambda d: (group(d(shout), d(wrap)), d(count)), None, id="a group gives a list"),
    pytest.param(
        lambda d: (group(d(shout), d(to_receipt)), d(count)),
        "to_receipt",
        id="one member of another type",
    ),
    pytest.param(
        lambda d: (group(d(shout), on_error="collect_all"), d(count)),
        "TaskFailure",
        id="collect_all may hold a failure",
    ),
    pytest.param(
        lambda d: (group(d(shout), on_error="collect_all"), d(outcomes)), None, id="a consumer that takes failures"
    ),
    pytest.param(
        lambda d: (group(d(shout), on_error="collect_all"), d(anything)), None, id="a bare list takes anything"
    ),
    pytest.param(lambda d: (d(greetings_of), d(shout).map), None, id="a map after a list of what it takes"),
    pytest.param(
        lambda d: (d(numbers_of), d(shout).map), "numbers_of", id="a map after a list of another type"
    ),
    pytest.param(lambda d: (d(greetings_of), d(shout).map, d(count)), None, id="a map gives a list"),
    pytest.param(
        lambda d: (d(greetings_of), d(shout).map, d(tally)), "shout", id="a map's items of another type"
    ),
    pytest.param(
        lambda d: (flow(d(shout), d(to_receipt)), d(tally)), "shout", id="a nested flow's list of mixed types"
    ),
    pytest.param(lambda d: (d(make_v1), d(take_v3)), None, id="later versions of a family"),
    pytest.param(lambda d: (d(take_v3), d(take_v2)), "take_v3", id="a downgrade"),
    pytest.param(lambda d: (d(make_v1), d(take_v4)), "make_v1", id="across an incompatible bump"),
    pytest.param(lambda d: (d(make_v1), d(shout)), "make_v1", id="another family and serializer"),
    pytest.param(
        lambda d: (d(make_parcel), d(take_record)), None, id="one family across two JSON serializers"
    ),
    pytest.param(lambda d: (d(make_v1), d(take_optional)), None, id="a consumer that takes the type or None"),
    pytest.param(lambda d: (d(make_rush), d(take_v1)), None, id="a subclass of what the consumer takes"),
    pytest.param(
        lambda d: (d(greetings_of), collecting_map(d(shout)), d(count)),
        "TaskFailure",
        id="a map under collect_all may hold a failure",
    ),
    pytest.param(
        lambda d: (d(greetings_of), collecting_map(d(shout)), d(outcomes)),
        None,
        id="a map under collect_all into a consumer that takes failures",
    ),
    pytest.param(
        lambda d: (d(shout), group(group(d(wrap)), d(wrap))), None, id="a group nested in a group"
    ),
    pytest.param(
        lambda d: (group(group(d(shout), d(wrap)), d(wrap)), d(count)),
        "count",
        id="a nested group gives a list inside the list",
    ),
]


@pytest.mark.parametrize(("stages", "naming"), FIT_CASES)
def test_a_composition_refuses_an_output_the_next_step_cannot_take(stages, naming, monkeypatch):
    monkeypatch.setattr(schemas, "_process_schemas", order_schemas())
    # The check finds backends in the process's registry, so the pickle backend must be there.
    process_backends = SerializerRegistry.with_defaults()
    process_backends.register(Pickle())
    monkeypatch.setattr(serializer_registry, "_process_serializers", process_backends)

    if naming is None:
        flow(*stages(declare_with_serializer))
    else:
        with pytest.raises(TypeError, match=naming):
            flow(*stages(declare_with_serializer))


@pytest.mark.parametrize(
    ("build", "needs_argument"),
    [
        pytest.param(
            lambda: flow(declare(shout).bind(Greeting(text="x")), declare(wrap)),
            False,
            id="flow with a fully bound first stage",
        ),
        pytest.param(lambda: flow(declare(shout), declare(wrap)), True, id="flow whose first stage takes input"),
        pytest.param(
            lambda: group(declare(shout).bind(Greeting()), declare(wrap).bind(Greeting())),
            False,
            id="group of fully bound members",
        ),
        pytest.param(
            lambda: group(declare(shout).bind(Greeting()), declare(wrap)),
            True,
            id="group with a member that takes input",
        ),
    ],
)
def test_a_flow_or_group_takes_an_argument_only_when_its_first_steps_need_one(build, needs_argument):
    target = build()

    with pytest.raises(TypeError):
        target(Greeting()) if not needs_argument else target()
    with pytest.raises(RuntimeNotStartedError):
        target(Greeting()) if needs_argument else target()


# --- running flows ------------------------------------------------------


def stages(world, *names):
    return [world.tasks[name] for name in names]


async def until_claimed(world, count):
    while len(world.runtime.claimed) < count:
        await asyncio.sleep(0.005)


def test_a_fully_bound_stage_ignores_the_prior_output_whatever_its_type():
    world = World(to_receipt, wrap)
    pipeline = flow(world.tasks["to_receipt"], world.tasks["wrap"].bind(Greeting(text="fixed")))

    async def body():
        return await pipeline.start(world.session, Greeting(text="hi"))

    assert run(with_events(world, body))[-1].text == "[fixed]"


def test_a_flow_started_from_a_synchronous_task_bodys_thread_runs_on_the_run_loop():
    world = World(shout)
    pipeline = flow(*stages(world, "shout"))
    started = []

    def starter(request: Greeting) -> Greeting:
        started.append(pipeline(request))  # on the task's worker thread
        return request

    starter_task = Task(
        starter, registry=world.registry, serializers=world.serializers, name="tests.starter"
    )

    async def body():
        await world.session.submit(starter_task.definition, Greeting(text="a"))
        return await asyncio.wait_for(started[0], WAIT)

    with activated(world):
        assert run(with_events(world, body)) == [Greeting(text="A")]


def test_a_failed_stage_fails_the_flow_and_later_stages_never_start():
    ran = []

    def explode(request: Greeting) -> Greeting:
        raise ValueError("stage failed")

    def after(request: Greeting) -> Greeting:
        ran.append(True)
        return request

    world = World(shout, explode, after)
    pipeline = flow(*stages(world, "shout", "explode", "after"))

    async def body():
        with pytest.raises(ValueError, match="stage failed"):
            await pipeline.start(world.session, Greeting())

    run(with_events(world, body))

    assert ran == []


def test_cancelling_a_flow_cancels_the_current_stage_and_starts_no_later_one():
    ran = []

    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(30)
        return request

    def after(request: Greeting) -> Greeting:
        ran.append(True)
        return request

    world = World(slow, after)
    pipeline = flow(*stages(world, "slow", "after"))

    async def body():
        handle = pipeline.start(world.session, Greeting())
        await until_claimed(world, 1)
        assert handle.cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert ran == []


def test_a_stage_whose_result_cannot_be_stored_is_reported_lost_and_run_again_without_failing_the_flow():
    ran = []

    def counted(request: Greeting) -> Greeting:
        ran.append(True)
        return shout(request)

    world = World(counted)
    world.runtime.store_gives_up = 1
    pipeline = flow(*stages(world, "counted"))

    async def body():
        return await pipeline.start(world.session, Greeting(text="a"))

    assert run(with_events(world, body)) == [Greeting(text="A")]

    assert len(ran) == 2
    assert [event[0] for event in world.runtime.events].count("lost") == 1


def test_a_task_that_waits_for_a_flow_gives_up_its_place_meanwhile():
    async def inner_stage(request: Greeting) -> Greeting:
        return request

    world = World(inner_stage, concurrency=1)

    async def outer(request: Greeting) -> Greeting:
        await flow(world.tasks["inner_stage"]).start(world.session, request)
        return request

    outer_task = Task(
        outer, registry=world.registry, serializers=world.serializers, name="tests.outer"
    )

    async def body():
        return await world.session.submit(outer_task.definition, Greeting(text="ok"))

    assert run(with_events(world, body)).text == "ok"


def test_a_flow_callback_gets_the_array_of_results():
    world = World(shout)
    pipeline = flow(*stages(world, "shout"))
    seen = []

    async def body():
        handle = pipeline.start(world.session, Greeting(text="a"))
        handle.callback(seen.append)
        await handle
        await world.session.wait_until_idle()

    run(with_events(world, body))

    assert seen == [[Greeting(text="A")]]


def test_a_flow_submitted_outside_a_running_loop_is_refused():
    world = World(shout)
    pipeline = flow(*stages(world, "shout"))

    with pytest.raises(RuntimeNotStartedError):
        pipeline.start(world.session, Greeting())


# --- group --------------------------------------------------------------


def test_a_group_needs_members_a_known_policy_and_only_takes_steps():
    with pytest.raises(ValueError):
        group()
    with pytest.raises(ValueError, match="on_error"):
        group(declare(shout), on_error="ignore")
    with pytest.raises(TypeError):
        group(shout)


def test_fail_fast_leaves_the_other_members_running_like_asyncio_gather():
    finished = []

    def explode(request: Greeting) -> Greeting:
        raise ValueError("member failed")

    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(0.02)
        finished.append(True)
        return request

    world = World(explode, slow)
    members = group(world.tasks["explode"], world.tasks["slow"])

    async def body():
        with pytest.raises(ValueError):
            await members.start(world.session, Greeting())
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert finished == [True]


def test_cancelling_a_group_cancels_every_member_and_the_stage_after_never_starts():
    started = []
    ran_after = []

    async def slow(request: Greeting) -> Greeting:
        started.append(True)
        await asyncio.sleep(30)
        return request

    def after(request: list[Greeting]) -> Greeting:
        ran_after.append(True)
        return Greeting()

    world = World(slow, after, concurrency=4)
    pipeline = flow(
        group(world.tasks["slow"], world.tasks["slow"], world.tasks["slow"]), world.tasks["after"]
    )

    async def body():
        handle = pipeline.start(world.session, Greeting())
        while len(started) < 3:
            await asyncio.sleep(0.005)
        assert handle.cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert ran_after == []


def test_flows_and_groups_nest_and_the_results_nest_the_same_way():
    world = World(shout, wrap, to_receipt)
    inner = flow(world.tasks["shout"], world.tasks["wrap"])
    pipeline = flow(
        world.tasks["shout"],
        group(inner, world.tasks["to_receipt"]),
    )

    async def body():
        return await pipeline.start(world.session, Greeting(text="a", times=1))

    assert run(with_events(world, body)) == [
        Greeting(text="A", times=1),
        [
            [Greeting(text="A", times=1), Greeting(text="[A]", times=1)],
            Receipt(ok=True),
        ],
    ]


# --- flow and group, the same way ----------------------------------------


def two_stage_flow(world):
    return flow(*stages(world, "shout", "wrap"))


def two_member_group(world):
    return group(world.tasks["shout"], world.tasks["wrap"])


FLOW_AND_GROUP = pytest.mark.parametrize(
    ("build", "results"),
    [
        pytest.param(two_stage_flow, [Greeting(text="A"), Greeting(text="[A]")], id="flow"),
        pytest.param(two_member_group, [Greeting(text="A"), Greeting(text="[a]")], id="group"),
    ],
)


@FLOW_AND_GROUP
def test_a_flow_or_group_that_was_never_started_because_the_run_stopped_fails(build, results):
    world = World(shout, wrap)
    target = build(world)
    world.runtime.hold_claims = True

    async def body():
        await world.call("shout", Greeting())  # takes the only claim
        handle = target.start(world.session, Greeting())  # everything of it waits for a claim
        await world.session.stop_claiming()
        with pytest.raises(RunStoppedError):
            await asyncio.wait_for(handle, WAIT)

    run(with_events(world, body))


@FLOW_AND_GROUP
def test_cancelling_a_finished_flow_or_group_changes_nothing(build, results):
    world = World(shout, wrap)
    target = build(world)

    async def body():
        handle = target.start(world.session, Greeting(text="a"))
        await handle
        return handle.cancel()

    assert run(with_events(world, body)) is False


@pytest.mark.parametrize("shape", [flow, group])
def test_cancelling_a_failed_flow_or_group_changes_nothing(shape):
    def explode(request: Greeting) -> Greeting:
        raise ValueError("stage failed")

    world = World(shout, explode)
    target = shape(*stages(world, "shout", "explode"))

    async def body():
        handle = target.start(world.session, Greeting(text="a"))
        with pytest.raises(ValueError, match="stage failed"):
            await handle
        assert handle.cancel() is False
        with pytest.raises(ValueError, match="stage failed"):
            await handle

    run(with_events(world, body))


@FLOW_AND_GROUP
def test_finishing_waits_for_a_flow_or_group_that_is_still_running(build, results):
    world = World(shout, wrap)
    target = build(world)

    async def body():
        handle = target.start(world.session, Greeting(text="a"))
        # Nothing has run yet, so only the flow or group itself is pending.
        await world.session.wait_until_idle()
        assert handle.done()
        return await handle

    assert run(with_events(world, body)) == results


@FLOW_AND_GROUP
def test_a_flow_or_group_cancelled_before_it_starts_runs_nothing(build, results):
    world = World(shout, wrap)
    target = build(world)

    async def body():
        handle = target.start(world.session, Greeting())
        assert handle.cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)

    run(with_events(world, body))

    assert world.runtime.claimed == []


# --- map ----------------------------------------------------------------


def greetings(*texts):
    return [Greeting(text=text) for text in texts]


def test_map_needs_a_list_of_inputs_and_checks_each_one_before_submitting_any():
    world = World(shout)

    async def body():
        with activated(world):
            with pytest.raises(TypeError):
                world.tasks["shout"].map(Greeting())
            with pytest.raises(SerializationError):
                world.tasks["shout"].map([Greeting(text="ok"), "not a greeting"])

    run(with_events(world, body))

    assert world.runtime.submitted == []


def test_map_runs_the_task_on_every_input_and_keeps_the_order_of_the_inputs():
    async def slow_first(request: Greeting) -> Greeting:
        await asyncio.sleep(0.05 if request.text == "a" else 0)
        return Greeting(text=request.text.upper())

    world = World(slow_first, concurrency=8)

    async def body():
        with activated(world):
            return await world.tasks["slow_first"].map(greetings("a", "b", "c"))

    assert [g.text for g in run(with_events(world, body))] == ["A", "B", "C"]


def test_mapping_an_empty_list_gives_an_empty_list_and_submits_nothing():
    world = World(shout)

    async def body():
        with activated(world):
            return await world.tasks["shout"].map([])

    assert run(with_events(world, body)) == []
    assert world.runtime.submitted == []


def test_map_follows_the_group_policy():
    def sometimes(request: Greeting) -> Greeting:
        if request.text == "bad":
            raise ValueError("bad input")
        return request

    world = World(sometimes)

    async def body():
        with activated(world):
            with pytest.raises(ValueError, match="bad input"):
                await world.tasks["sometimes"].map(greetings("ok", "bad"))
            return await world.tasks["sometimes"].map(
                greetings("ok", "bad"), on_error="collect_all"
            )

    outcome = run(with_events(world, body))

    assert outcome[0] == Greeting(text="ok") and isinstance(outcome[1], ValueError)


def test_map_is_a_flow_stage_that_takes_the_prior_stages_list():
    def load(request: Greeting) -> list[Greeting]:
        return [Greeting(text=word) for word in request.text.split()]

    world = World(load, shout)
    pipeline = flow(world.tasks["load"], world.tasks["shout"].map)

    async def body():
        return await pipeline.start(world.session, Greeting(text="a b c"))

    loaded, mapped = run(with_events(world, body))

    assert [g.text for g in loaded] == ["a", "b", "c"]
    assert [g.text for g in mapped] == ["A", "B", "C"]


def test_a_map_after_a_stage_that_returns_a_list_is_expanded_by_a_worker_and_keeps_the_items_order():
    def load(request: Greeting) -> list[Greeting]:
        return [Greeting(text=text) for text in ("a", "b", "c")]

    world = World(load, shout)
    pipeline = flow(world.tasks["load"], world.tasks["shout"].map)

    async def body():
        return await pipeline.start(world.session, Greeting())

    _, mapped = run(with_events(world, body))

    assert [greeting.text for greeting in mapped] == ["A", "B", "C"]
    assert any(claim.expansion for claim in world.runtime.claimed), "an expansion run split the list"


def test_a_map_that_fails_while_its_items_are_still_being_sent_stops_the_expansion_without_reporting_it():
    def load(request: Greeting) -> list[Greeting]:
        return [Greeting(text=text) for text in ("bad", "a", "b")]

    def picky(request: Greeting) -> Greeting:
        if request.text == "bad":
            raise ValueError("bad input")
        return request

    world = World(load, picky, member_batch_items=1)
    pipeline = flow(world.tasks["load"], world.tasks["picky"].map)
    map_failed = False
    # The first batch goes at once and the next waits, so the expansion is
    # still sending when the map ends.
    world.session.has_room = lambda: map_failed or members_sent(world) == 0

    async def body():
        nonlocal map_failed
        with pytest.raises(ValueError, match="bad input"):
            await pipeline.start(world.session, Greeting())
        map_failed = True
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert members_sent(world) == 1, "only the batch sent before the map ended was taken"
    reports = [event[0] for event in world.runtime.events]
    assert reports.count("complete") == 1, "only the list's own task completed; the cancelled expansion did not"
    assert reports.count("fail") == 1, "only the bad member failed; the cancelled expansion reported nothing"


def test_a_collect_all_failure_reaches_awaiting_code_and_the_next_stage_as_the_member_failed():
    def explode(request: Greeting) -> Greeting:
        raise ValueError("member failed")

    seen = []

    def after(request: list) -> str:
        seen.append(request)
        return "after"

    world = World(shout, explode)
    world.serializers.register(Pickle())
    after_task = Task(
        after,
        registry=world.registry,
        serializers=world.serializers,
        name="tests.after",
        serializer="pickle",
    )
    members = group(world.tasks["shout"], world.tasks["explode"], on_error="collect_all")

    async def body():
        direct = await members.start(world.session, Greeting(text="x"))
        staged = await flow(members, after_task).start(world.session, Greeting(text="x"))
        return direct, staged

    direct, staged = run(with_events(world, body))

    assert direct[0] == Greeting(text="X")
    assert isinstance(direct[1], ValueError), "code awaiting the group is given the error its own member raised"
    assert seen[0][0] == Greeting(text="X")
    assert isinstance(seen[0][1], TaskFailure), "the next stage is given the failure, whatever its serializer"
    assert (seen[0][1].kind, seen[0][1].message) == ("ValueError", "member failed")
    assert str(direct[1]) == "member failed"
    assert staged[-1] == "after"


def test_a_member_that_failed_alike_but_ended_nothing_is_not_given_to_a_later_group():
    def explode(request: Greeting) -> Greeting:
        error = ValueError("alike")
        error.who = request.text
        raise error

    world = World(explode)
    both_fail = group(world.tasks["explode"], world.tasks["explode"])
    one_fails = group(world.tasks["explode"])

    async def failure_of(handle):
        with pytest.raises(ValueError) as raised:
            await handle
        return raised.value

    async def body():
        # Both members raise, and the group fails with one of the errors: the
        # other is left over, with the same type and message.
        await failure_of(both_fail.start(world.session, Greeting(text="first")))
        return await failure_of(one_fails.start(world.session, Greeting(text="second")))

    error = run(with_events(world, body))

    assert error.who == "second"


def test_groups_whose_members_fail_alike_are_each_given_their_own_member_error():
    released = asyncio.Event()

    async def explode(request: Greeting) -> Greeting:
        error = ValueError("alike")
        error.who = request.text
        raise error

    async def linger(request: Greeting) -> Greeting:
        await released.wait()
        return request

    world = World(explode, linger)
    tasks = world.tasks
    # The first group's member fails first, and its group ends last.
    ends_last = group(tasks["explode"], tasks["linger"], on_error="collect_all")
    ends_first = group(tasks["explode"], on_error="collect_all")

    async def body():
        late = ends_last.start(world.session, Greeting(text="late"))
        early = ends_first.start(world.session, Greeting(text="early"))
        (error_early,) = await early
        released.set()
        error_late, _ = await late
        return error_early, error_late

    error_early, error_late = run(with_events(world, body))

    assert (error_early.who, error_late.who) == ("early", "late")


# --- backpressure -------------------------------------------------------

# Bytes of serialized input. Past the soft limit the runtime raises SlowDown;
# a submission that would go past the hard one is refused.
SOFT_LIMIT = 100
HARD_LIMIT = 1_000
PAST_SOFT_LIMIT = Greeting(text="x" * 150)


def held_world(*functions, hard_limit=HARD_LIMIT, **options):
    """A world with real memory limits, and `hold`, a task that keeps its input
    unfinished (so SlowDown stays raised) until the returned event is set."""
    released = asyncio.Event()

    async def hold(request: Greeting) -> Greeting:
        await released.wait()
        return request

    world = World(
        hold,
        *functions,
        memory_soft_limit=SOFT_LIMIT,
        memory_hard_limit=hard_limit,
        **options,
    )
    return world, released


def run_states(world):
    """The state of each submitted task's latest run, in submission order."""
    native = world.native
    return [
        native.task_run_state(native.task_run_ids(task_id)[-1])
        for task_id, *_ in world.runtime.submitted
    ]


def members_sent(world):
    """How many members the leader took in batches so far."""
    return sum(count for _, _, count in world.runtime.members_sent)


async def until_slow_down_seen(world):
    """Waits until the runtime says memory use is past its soft limit."""
    while world.session.has_room():
        await asyncio.sleep(0.005)


async def with_watcher_only(world, body):
    """Like `with_events`, but no worker claims anything, so every task the
    body submits stays pending and its input stays counted."""
    await asyncio.wait_for(world.native.wait_until_leader(), WAIT)
    events = asyncio.ensure_future(world.session.watch_events())
    try:
        return await asyncio.wait_for(body(), WAIT)
    finally:
        events.cancel()
        await asyncio.gather(events, return_exceptions=True)


@pytest.mark.parametrize(
    ("end", "error"),
    [
        pytest.param(
            lambda world, handle: asyncio.ensure_future(world.session.stop_claiming()), RunStoppedError, id="stop"
        ),
        pytest.param(lambda world, handle: handle.cancel(), TaskCancelledError, id="cancel"),
    ],
)
def test_stopping_or_cancelling_a_streamed_group_that_is_waiting_out_slow_down_sends_nothing_more(end, error):
    world = World(shout, memory_soft_limit=SOFT_LIMIT, memory_hard_limit=HARD_LIMIT, member_batch_items=1)
    members = group(world.tasks["shout"], world.tasks["shout"])

    async def body():
        world.call("shout", PAST_SOFT_LIMIT)  # never claimed: its input stays counted
        await until_slow_down_seen(world)
        handle = members.start(world.session, Greeting())
        await asyncio.sleep(0.05)
        end(world, handle)
        with pytest.raises(error):
            await asyncio.wait_for(handle, WAIT)

    run(with_watcher_only(world, body))

    assert members_sent(world) == 0


def test_a_streamed_group_that_hits_the_hard_limit_midway_fails_and_cancels_the_members_it_sent():
    # Two members fit and the third does not. With equal limits SlowDown is
    # never raised first, so the stream reaches the hard limit rather than
    # pausing. Members hold, so the cancel reaches them still running.
    world, released = held_world(hard_limit=100, member_batch_items=1)
    members = group(*([world.tasks["hold"]] * 3))

    async def body():
        handle = members.start(world.session, Greeting(text="x" * 40))
        with pytest.raises(BackpressureError):
            await handle
        released.set()
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)
        return await world.native.composite_status(handle.task_id)

    state, _, pending, running, succeeded, failed, cancelled = run(with_events(world, body))

    assert members_sent(world) == 2
    assert (state, cancelled, pending + running + succeeded + failed) == ("cancelled", 2, 0)


def test_a_streamed_group_given_an_input_its_members_cannot_take_is_refused_at_start_with_nothing_submitted():
    world = World(shout, member_batch_items=1)
    members = group(world.tasks["shout"], world.tasks["shout"])

    async def body():
        with pytest.raises(SerializationError):
            members.start(world.session, "not a greeting")

    run(with_events(world, body))

    assert world.runtime.members_sent == []


def test_a_stream_that_cannot_cancel_what_it_sent_still_fails_its_handle_and_says_so(caplog):
    world, released = held_world(hard_limit=100, member_batch_items=1)
    members = group(*([world.tasks["hold"]] * 3))

    def not_the_leader(composite_id):
        raise RuntimeError("this worker is not the leader")

    world.runtime.cancel_composite = not_the_leader

    async def body():
        handle = members.start(world.session, Greeting(text="x" * 40))
        with pytest.raises(BackpressureError):
            await handle
        released.set()  # the members it could not cancel finish
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)
        return handle.task_id

    with caplog.at_level("WARNING", logger="kabudachi"):
        composite_id = run(with_events(world, body))

    assert [record.getMessage() for record in caplog.records if record.name == "kabudachi"] == [
        f"the members of {composite_id} that were sent could not be cancelled: this worker is not the leader"
    ]


# A pure-logic edge-case exception to testing through the public seam: the
# boundaries of a batch are many and cheap to reach here, and slow to reach by
# running a map for each.
@pytest.mark.parametrize(
    ("lengths", "max_items", "max_bytes", "expected"),
    [
        pytest.param([0] * 5, 2, 10**6, [[0, 0], [0, 0], [0]], id="at most max_items members"),
        pytest.param([36] * 5, 100, 250, [[36, 36], [36, 36], [36]], id="at most max_bytes of input"),
        pytest.param([86, 86], 100, 300, [[86, 86]], id="a batch may fill max_bytes exactly"),
        pytest.param([10, 1000, 10], 100, 250, [[10], [1000], [10]], id="a member past max_bytes has its own batch"),
        pytest.param([1000, 10], 100, 250, [[1000], [10]], id="an oversized first member is not held back"),
        pytest.param([], 100, 250, [], id="no members make no batch"),
    ],
)
def test_members_are_batched_by_count_and_by_bytes_with_an_oversized_member_alone(
    lengths, max_items, max_bytes, expected
):
    members = [b"x" * length for length in lengths]

    batches = [[len(member) for member in batch] for batch in _batches(members, max_items, max_bytes)]

    assert batches == expected


def test_a_reduce_whose_items_pass_the_hard_limit_fails_with_backpressure_error_and_runs_no_step():
    def fold(item: Greeting, previous: Greeting) -> Greeting:
        return Greeting(text=previous.text + item.text)

    world = World(fold, memory_soft_limit=100, memory_hard_limit=100, member_batch_items=1)

    async def body():
        handle = world.tasks["fold"].reduce([Greeting(text="x" * 40)] * 4, initial=Greeting())
        with pytest.raises(BackpressureError):
            await handle

    with activated(world):
        run(with_events(world, body))

    assert world.runtime.claimed == [], "the reduce was never sealed, so no step ran"


def test_a_streamed_group_pauses_when_slow_down_is_raised_while_it_is_still_sending_but_a_plain_call_does_not():
    world, released = held_world(shout, concurrency=8, member_batch_items=1)
    members = group(*([world.tasks["hold"]] * 40))

    async def body():
        handle = members.start(world.session, Greeting(text="x" * 60))
        await until_slow_down_seen(world)
        paused_at = members_sent(world)
        await asyncio.sleep(0.05)
        while_slow = members_sent(world)
        plain = await world.call("shout", Greeting(text="plain"))
        released.set()  # the held members finish, which clears SlowDown
        await asyncio.wait_for(handle, WAIT)
        return paused_at, while_slow, plain, members_sent(world)

    paused_at, while_slow, plain, after = run(with_events(world, body))

    assert while_slow == paused_at, "the group kept sending past the soft limit"
    assert paused_at < 40
    assert plain == Greeting(text="PLAIN"), "a plain call does not wait"
    assert after == 40


def test_cancelling_a_streamed_group_part_way_through_starts_no_further_member():
    world, released = held_world(concurrency=8, member_batch_items=1)
    members = group(*([world.tasks["hold"]] * 6))

    async def body():
        handle = members.start(world.session, Greeting())
        while members_sent(world) < 2:
            await asyncio.sleep(0)
        assert handle.cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)
        released.set()
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)
        return await world.native.composite_status(handle.task_id)

    state, _, pending, running, succeeded, _, cancelled = run(with_events(world, body))

    assert members_sent(world) < 6
    assert (state, pending, running, succeeded, cancelled) == ("cancelled", 0, 0, 0, members_sent(world))


def test_a_stage_reading_a_large_result_holds_it_against_the_memory_limit_until_its_run_ends():
    reading = asyncio.Event()
    released = asyncio.Event()

    async def big(request: Greeting) -> Greeting:
        return Greeting(text="x" * 600)

    async def read(request: Greeting) -> Greeting:
        reading.set()
        await released.wait()
        return Greeting()

    def small(request: Greeting) -> Greeting:
        return request

    world = World(big, read, small, memory_soft_limit=SOFT_LIMIT, memory_hard_limit=HARD_LIMIT)
    pipeline = flow(world.tasks["big"], world.tasks["read"])
    late = Greeting(text="y" * 500)

    async def body():
        done = pipeline.start(world.session, Greeting(text="in"))
        await asyncio.wait_for(reading.wait(), WAIT)
        # The 600 bytes the stage was given and the 500 would pass the limit.
        with pytest.raises(BackpressureError):
            world.session.submit(world.tasks["small"].definition, late)
        released.set()
        await done
        return await world.session.submit(world.tasks["small"].definition, late)

    assert run(with_events(world, body)) == late


def test_a_step_bound_to_a_value_past_the_hard_limit_alone_is_refused_before_anything_is_stored():
    world, released = held_world(hard_limit=100)
    pipeline = flow(world.tasks["hold"].bind(Greeting(text="x" * 200)))

    async def body():
        with pytest.raises(PayloadTooLargeError):
            pipeline.start(world.session, Greeting())

    run(with_events(world, body))

    assert world.runtime.claimed == []


# --- a task that returns a step --------------------------------------------

def acknowledge(request: Greeting) -> Receipt:
    return Receipt(ok=True)


def continuing_world(*functions, **options):
    return World(shout, wrap, acknowledge, *functions, **options)


def declare_planner(world, body, **options):
    task = Task(
        body,
        registry=world.registry,
        serializers=world.serializers,
        name=f"tests.{body.__name__}",
        **options,
    )
    world.tasks[body.__name__] = task
    return task


def started_runs(world):
    return [event[0] for event in world.runtime.events].count("started")


def run_planner(world, planner, argument=None):
    async def body():
        return await world.session.submit(planner.definition, argument or Greeting(text="in"))

    return run(with_events(world, body))


# --- declaration -----------------------------------------------------------


def test_a_task_that_returns_a_step_needs_no_serializer_support_for_it():
    from kabudachi.session import validate_definitions

    world = continuing_world()

    def plans_flow(request: Greeting) -> Flow: ...

    declare_planner(world, plans_flow)
    validate_definitions(world.registry, world.serializers)


# --- running ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("annotation", "step", "text", "expected"),
    [
        pytest.param(
            Flow,
            lambda world, request: flow(world.tasks["shout"].bind(request), world.tasks["wrap"]),
            "hi",
            [Greeting(text="HI"), Greeting(text="[HI]")],
            id="flow",
        ),
        pytest.param(
            Group,
            lambda world, request: group(
                world.tasks["shout"].bind(request), world.tasks["acknowledge"].bind(request)
            ),
            "a",
            [Greeting(text="A"), Receipt(ok=True)],
            id="group",
        ),
        pytest.param(
            BoundTask,
            lambda world, request: world.tasks["shout"].bind(request),
            "a",
            [Greeting(text="A")],
            id="bound task",
        ),
    ],
)
def test_a_returned_step_runs_after_the_certification_and_the_handle_is_its_results(
    annotation, step, text, expected
):
    world = continuing_world()

    def plan(request: Greeting) -> annotation:
        return step(world, request)

    planner = declare_planner(world, plan)

    results = run_planner(world, planner, Greeting(text=text))

    assert results == expected
    order = [event[0] for event in world.runtime.events]
    first_certification = order.index("complete")
    # The planning run is certified before any stage of the continuation starts.
    assert order[: first_certification + 1].count("started") == 1


def test_a_returned_step_that_needs_an_input_is_refused_and_fails_the_run():
    world = continuing_world()

    def plan(request: Greeting) -> Flow:
        return flow(world.tasks["shout"])  # nothing to give its first stage

    planner = declare_planner(world, plan)

    async def body():
        with pytest.raises(TaskDefinitionError, match="input"):
            await world.session.submit(planner.definition, Greeting())

    run(with_events(world, body))

    assert [e[0] for e in world.runtime.events][-1] == "fail"


def test_a_returned_step_whose_stages_do_not_fit_fails_its_run_and_starts_nothing():
    world = continuing_world(to_receipt)

    def plan(request: Greeting) -> Flow:
        # Built without flow(), so only the check at the continuation's commit can catch it.
        return Flow([world.tasks["to_receipt"].bind(Greeting()), BoundTask(world.tasks["shout"].definition)])

    planner = declare_planner(world, plan)

    async def body():
        with pytest.raises(TypeError, match="to_receipt"):
            await world.session.submit(planner.definition, Greeting())

    run(with_events(world, body))

    assert started_runs(world) == 1, "no stage of the step started"
    assert [e[0] for e in world.runtime.events][-1] == "fail"


def test_a_returned_step_bound_to_a_value_past_the_hard_limit_alone_fails_the_run():
    world = continuing_world(memory_soft_limit=SOFT_LIMIT, memory_hard_limit=200)

    def plan(request: Greeting) -> Flow:
        return flow(world.tasks["shout"].bind(Greeting(text="x" * 400)))

    planner = declare_planner(world, plan)

    async def body():
        with pytest.raises(PayloadTooLargeError):
            await world.session.submit(planner.definition, Greeting())

    run(with_events(world, body))

    assert [e[0] for e in world.runtime.events][-1] == "fail"


def test_a_returned_step_whose_bound_values_pass_a_record_runs_with_them_stored_as_blobs():
    world = continuing_world()
    # One value alone is past a record; the other two are past it together.
    large = [Greeting(text=letter * size) for letter, size in zip("abc", (1_100_000, 600_000, 600_000))]

    def plan(request: Greeting) -> Group:
        return group(
            *(world.tasks["shout"].bind(value) for value in large), world.tasks["acknowledge"].bind(request)
        )

    planner = declare_planner(world, plan)
    results = run_planner(world, planner, Greeting())

    assert results == [Greeting(text=value.text.upper()) for value in large] + [Receipt(ok=True)]


def test_a_returned_step_too_large_by_its_structure_alone_fails_its_run_for_good_with_continuation_too_large():
    world = continuing_world()
    members = 32_000  # 35 bytes each: just past the 1,114,112 a record may hold

    step = group(*([world.tasks["shout"].bind(Greeting(text="x"))] * members))

    def plan(request: Greeting) -> Group:
        return step

    tree = step.describe(world.session)
    assert len(_native.describe_step(tree)) > _native.MAX_RECORD_BYTES, "the structure alone passes a record"
    planner = declare_planner(world, plan)

    async def body():
        handle = world.session.submit(planner.definition, Greeting())
        with pytest.raises(TaskBodyError) as failure:
            await handle
        assert failure.value.kind == "ContinuationTooLarge"
        return await world.native.task_record(handle.task_id)

    record = run(with_events(world, body))

    assert [(attempt.failure_kind, attempt.state) for attempt in record.runs] == [
        ("ContinuationTooLarge", _native.RunState.FAILED)
    ], "failed once, with no retry"


def test_a_two_thousand_item_map_toggles_slow_down_and_keeps_its_order():
    # Every batch of 100 items (about 900 bytes) passes the soft limit on
    # its own, so each one raises SlowDown and the next waits for it to clear.
    world = World(shout, memory_soft_limit=500, memory_hard_limit=1_000_000, concurrency=8, member_batch_items=100)
    items = [Greeting(text=f"item {index}") for index in range(2_000)]

    async def body():
        with activated(world):
            return await world.tasks["shout"].map(items)

    results = run(with_events(world, body))

    assert [result.text for result in results] == [item.text.upper() for item in items]
    starts = [start for _, start, _ in world.runtime.members_sent]
    assert starts == list(range(0, 2_000, 100)), "every batch once, in order"
    assert world.runtime.slow_downs.count(True) >= 10 and world.runtime.slow_downs.count(False) >= 10


def test_a_task_annotated_to_return_a_step_but_returning_something_else_fails():
    world = continuing_world()

    def plan(request: Greeting) -> Flow:
        return "not a step"

    planner = declare_planner(world, plan)

    async def body():
        with pytest.raises(TaskDefinitionError):
            await world.session.submit(planner.definition, Greeting())

    run(with_events(world, body))


def test_a_returned_step_may_only_use_tasks_this_process_knows():
    world = continuing_world()
    stranger = Task(
        shout,
        registry=TaskRegistry(),
        serializers=SerializerRegistry.with_defaults(),
        name="tests.stranger",
    )

    def plan(request: Greeting) -> BoundTask:
        return stranger.bind(request)

    planner = declare_planner(world, plan)

    async def body():
        with pytest.raises(UnknownTaskError, match="stranger"):
            await world.session.submit(planner.definition, Greeting())

    run(with_events(world, body))


def test_a_run_the_leader_will_not_certify_leaves_no_continuation_behind():
    world = continuing_world()
    world.runtime.refuse_completion = True

    def plan(request: Greeting) -> Flow:
        return flow(world.tasks["shout"].bind(request))

    planner = declare_planner(world, plan)

    async def body():
        with pytest.raises(RuntimeError):
            await world.session.submit(planner.definition, Greeting())
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)
        [refused] = [event[1] for event in world.runtime.events if event[0] == "complete"]
        assert await world.native.composite_status(_native.continuation_of(refused)) is None

    run(with_events(world, body))

    assert world.runtime.claimed[0].definition_id == "tests.plan"
    assert len(world.runtime.claimed) == 1, "no stage of a continuation was ever made"


def test_a_failing_continuation_fails_the_handle_and_never_reruns_the_planning_task():
    plans = []

    def explode(request: Greeting) -> Greeting:
        raise ValueError("stage failed")

    world = continuing_world(explode)

    def plan(request: Greeting) -> Flow:
        plans.append(True)
        return flow(world.tasks["shout"].bind(request), world.tasks["explode"])

    planner = declare_planner(world, plan, retries=3)

    async def body():
        with pytest.raises(ValueError, match="stage failed"):
            await world.session.submit(planner.definition, Greeting())

    run(with_events(world, body))

    assert plans == [True]


def test_cancelling_the_handle_during_the_continuation_cancels_its_current_stage():
    ran_after = []

    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(30)
        return request

    def after(request: Greeting) -> Greeting:
        ran_after.append(True)
        return request

    world = continuing_world(slow, after)

    def plan(request: Greeting) -> Flow:
        return flow(world.tasks["slow"].bind(request), world.tasks["after"])

    planner = declare_planner(world, plan)

    async def body():
        handle = world.session.submit(planner.definition, Greeting())
        while started_runs(world) < 2:  # the planning run, then the slow stage
            await asyncio.sleep(0.005)
        assert handle.cancel()
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert ran_after == []


def test_finishing_waits_for_a_continuation_that_is_still_running():
    world = continuing_world()

    def plan(request: Greeting) -> Flow:
        return flow(world.tasks["shout"].bind(request), world.tasks["wrap"])

    planner = declare_planner(world, plan)
    handles = []

    async def body():
        handles.append(world.session.submit(planner.definition, Greeting(text="a")))
        await world.session.wait_until_idle()
        assert handles[0].done()

    run(with_events(world, body))


def test_stopping_fails_a_continuation_that_has_not_started_its_stage():
    world = continuing_world()
    world.runtime.hold_claims = True

    def plan(request: Greeting) -> Flow:
        return flow(world.tasks["shout"].bind(request))

    planner = declare_planner(world, plan)

    async def body():
        handle = world.session.submit(planner.definition, Greeting())
        while ("complete" not in [event[0] for event in world.runtime.events]):
            await asyncio.sleep(0.005)  # the planning run is certified; its stage waits for a claim
        await world.session.stop_claiming()
        with pytest.raises(RunStoppedError):
            await asyncio.wait_for(handle, WAIT)

    run(with_events(world, body))


def test_a_cancel_that_arrives_while_the_run_is_being_certified_reaches_the_continuation():
    ran_after = []

    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(30)
        return request

    def after(request: Greeting) -> Greeting:
        ran_after.append(True)
        return request

    world = continuing_world(slow, after)

    def plan(request: Greeting) -> Flow:
        return flow(world.tasks["slow"].bind(request), world.tasks["after"])

    planner = declare_planner(world, plan)
    handles = []
    cancelled = []

    def cancel_from_another_thread(run_id):
        # Arrives while `complete` is running, after the leader has accepted it.
        thread = threading.Thread(target=lambda: cancelled.append(handles[0].cancel()))
        thread.start()
        cancelling.append(thread)

    cancelling = []
    world.runtime.after_certification = cancel_from_another_thread

    async def body():
        handles.append(world.session.submit(planner.definition, Greeting()))
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handles[0], WAIT)
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))
    for thread in cancelling:
        thread.join()

    assert cancelled == [True], "the cancel was lost in the window before the continuation existed"
    assert ran_after == []


# --- properties ------------------------------------------------------------

SERIALIZERS = SerializerRegistry.with_defaults()
PROTOBUF = SERIALIZERS.get("protobuf")


def fold_left(older: Greeting, newer: Greeting) -> Greeting:
    """Not associative, so an order or grouping mistake changes the result."""
    return Greeting(text=f"({older.text}>{newer.text})")


def coalescing_definition():
    def refresh(request: Greeting) -> Greeting:
        return request

    coalescing_task = Task(
        refresh,
        registry=TaskRegistry(),
        serializers=SERIALIZERS,
        name="tests.refresh",
        kind=TaskKind.COALESCING,
        merge=fold_left,
    )
    return coalescing_task.definition


def claim_of(texts):
    encode = lambda text: PROTOBUF.encode(Greeting(text=text), Greeting)  # noqa: E731
    return SimpleNamespace(chain=[encode(t) for t in texts[:-1]], serialized_input=encode(texts[-1]))


texts = st.lists(st.text(alphabet="abcdef", min_size=1, max_size=3), min_size=1, max_size=12)


@given(texts)
def test_the_claiming_worker_folds_every_absorbed_payload_in_order(payloads):
    """A superseded payload is folded, never dropped, oldest first.

    The fold does not depend on where a prefix was compacted: a left fold
    gives the same result however its prefix was grouped.
    """
    definition = coalescing_definition()

    claim = claim_of(payloads)
    folded = fold_payloads(definition, PROTOBUF, [*claim.chain, claim.serialized_input])

    expected = functools.reduce(fold_left, [Greeting(text=t) for t in payloads])
    assert folded == expected


# --- the stage after a group runs exactly once --------------------------

RETRIES = 2


def run_group_then_stage(failures, on_error):
    """A group whose member `i` fails its first `failures[i]` attempts (a
    member that is retried `RETRIES` times still fails if that is not enough),
    then a stage that records how it was called. Returns what the stage saw
    and how the flow ended."""
    attempts = [0] * len(failures)
    seen = []

    def member(index):
        def body(request: Greeting) -> Greeting:
            attempts[index] += 1
            if attempts[index] <= failures[index]:
                raise ValueError(f"member {index} failed")
            return Greeting(text=f"m{index}")

        body.__name__ = f"member_{index}"
        return body

    def stage(request: list) -> str:
        seen.append(request)
        return "stage"

    world = World(*[member(i) for i in range(len(failures))], retries=RETRIES)
    world.serializers.register(Pickle())
    stage_task = Task(
        stage,
        registry=world.registry,
        serializers=world.serializers,
        name="tests.stage",
        serializer="pickle",
    )
    members = group(
        *[world.tasks[f"member_{i}"] for i in range(len(failures))], on_error=on_error
    )
    pipeline = flow(members, stage_task)

    async def body():
        try:
            return await pipeline.start(world.session, Greeting())
        except ValueError as error:
            return error

    outcome = run(with_events(world, body))
    return seen, outcome


failure_counts = st.lists(st.integers(min_value=0, max_value=RETRIES + 1), min_size=1, max_size=5)


@settings(max_examples=8, deadline=None)
@given(failure_counts)
def test_under_collect_all_the_stage_after_a_group_runs_once_with_every_outcome(failures):
    seen, outcome = run_group_then_stage(failures, "collect_all")

    assert len(seen) == 1, "the stage after the group must run exactly once"
    [array] = seen
    assert len(array) == len(failures)
    for index, entry in enumerate(array):
        if failures[index] <= RETRIES:
            assert entry == Greeting(text=f"m{index}")
        else:
            assert isinstance(entry, TaskFailure)
            assert (entry.kind, entry.message) == ("ValueError", f"member {index} failed")
    assert outcome[-1] == "stage"


@settings(max_examples=8, deadline=None)
@given(failure_counts)
def test_under_fail_fast_the_stage_runs_once_only_if_every_member_succeeds(failures):
    seen, outcome = run_group_then_stage(failures, "fail_fast")

    if all(count <= RETRIES for count in failures):
        assert len(seen) == 1
        assert outcome[-1] == "stage"
    else:
        assert seen == [], "a failed group must not start the stage after it"
        assert isinstance(outcome, ValueError)
        failing = {f"member {i} failed" for i, count in enumerate(failures) if count > RETRIES}
        assert str(outcome) in failing


# --- reduce ---------------------------------------------------------------


def chain_concat(item: str, previous: str) -> str:
    return previous + item


def tree_concat(item: str, previous: str) -> str:
    return previous + item


def count_letters(item: str, previous: dict) -> dict:
    return {**previous, item: previous.get(item, 0) + 1}


def add_counts(left: dict, right: dict) -> dict:
    return {key: left.get(key, 0) + right.get(key, 0) for key in left | right}


def explode_on_bad(item: str, previous: str) -> str:
    if item == "bad":
        raise ValueError("bad item")
    return previous + item


async def async_chain_concat(item: str, previous: str) -> str:
    await asyncio.sleep(0)
    return previous + item


async def async_tree_concat(item: str, previous: str) -> str:
    await asyncio.sleep(0)
    return previous + item


def concatenated(left: str, right: str) -> str:
    return left + right


def reducing_world(*functions, **options):
    """A world whose tasks use pickle, with `functions` declared as tasks
    and `options` (name to Task options) declared alongside."""
    world = World(serializer="pickle")
    world.serializers.register(Pickle())
    for function in functions:
        world.tasks[function.__name__] = Task(
            function,
            registry=world.registry,
            serializers=world.serializers,
            name=f"tests.{function.__name__}",
            serializer="pickle",
            **options.get(function.__name__, {}),
        )
    return world


def reduce_claims(world, name):
    return [claim for claim in world.runtime.claimed if claim.definition_id == f"tests.{name}"]


def test_an_associative_reduce_gives_the_chains_result_whether_it_runs_as_a_chain_or_a_tree():
    world = reducing_world(chain_concat, tree_concat, tree_concat={"associative": True})
    letters = list("abcdefg")

    async def body():
        chained = await world.tasks["chain_concat"].reduce(letters)
        treed = await world.tasks["tree_concat"].reduce(letters, chunk_size=2)
        return chained, treed

    with activated(world):
        assert run(with_events(world, body)) == ("abcdefg", "abcdefg")
    assert len(reduce_claims(world, "chain_concat")) == 6, "a seedless chain runs one step per item after the first"
    levels = sorted(claim.reduce_node[0] for claim in reduce_claims(world, "tree_concat"))
    assert levels == [0, 0, 0, 0, 1, 1, 2], "four chunks of two, then three combines"


def test_an_empty_or_one_item_reduce_runs_no_step_and_a_seedless_empty_one_has_no_result():
    world = reducing_world(chain_concat)
    concat = world.tasks["chain_concat"]

    async def body():
        return await concat.reduce([], initial="seed"), await concat.reduce(["only"])

    with activated(world):
        assert run(with_events(world, body)) == ("seed", "only")
        with pytest.raises(ReduceEmptyError):
            concat.reduce([])
    assert reduce_claims(world, "chain_concat") == []


def test_a_reduce_the_leader_finds_empty_fails_with_reduce_empty_error():
    world = reducing_world(split_words, chain_concat)

    async def body():
        # The empty list is refused before it is sent when given; a stage's
        # return is found empty only once it is streamed.
        with pytest.raises(ReduceEmptyError):
            await flow(world.tasks["split_words"], world.tasks["chain_concat"].reduce).start(world.session, "")

    with activated(world):
        run(with_events(world, body))


def test_a_step_that_fails_after_its_retries_fails_the_reduce_and_no_later_step_runs():
    world = reducing_world(explode_on_bad, explode_on_bad={"retries": 1})

    async def body():
        return await world.tasks["explode_on_bad"].reduce(["a", "bad", "c"], initial="")

    with activated(world):
        with pytest.raises(ValueError, match="bad item"):
            run(with_events(world, body))
    claims = [claim.reduce_node[1] for claim in reduce_claims(world, "explode_on_bad")]
    assert claims == [0, 1, 1], "the failing step was tried twice and the step after it never ran"


def test_a_step_lost_after_storing_its_result_and_before_complete_is_replayed_and_the_reduce_ends_as_the_chain_does():
    calls = []

    def numbered_concat(item: str, previous: str) -> str:
        # Not idempotent: each call leaves its own number in the result, so the
        # replay's result differs from the lost first run's.
        calls.append(item)
        return f"{previous}{item}{len(calls)}"

    world = reducing_world(numbered_concat)
    lost = []

    def lose_the_first_completion_of_step_one(task_run_id):
        # The session has stored the step's result blob (`store_result`) and is
        # about to report COMPLETE: the run is lost instead, as when its worker dies.
        claim = next(claim for claim in world.runtime.claimed if claim.task_run_id == task_run_id)
        losing = claim.reduce_node == (0, 1) and not lost
        if losing:
            lost.append(task_run_id)
            world.runtime.native.report_lost(task_run_id)
        world.runtime.refuse_completion = losing

    world.runtime.on_complete = lose_the_first_completion_of_step_one

    async def body():
        return await world.tasks["numbered_concat"].reduce(list("abc"), initial="")

    with activated(world):
        assert run(with_events(world, body)) == "a1b3c4", "step 2 folded the replay's result (b3), not the lost run's (b2)"
    steps = [claim.reduce_node[1] for claim in reduce_claims(world, "numbered_concat")]
    assert lost and steps == [0, 1, 1, 2], "step 1 ran again on the same operands"


def test_async_reducers_fold_a_chain_and_a_tree_whose_merge_is_synchronous():
    world = reducing_world(
        async_chain_concat,
        async_tree_concat,
        async_tree_concat={"tree": True, "merge": concatenated},
    )

    async def body():
        chained = await world.tasks["async_chain_concat"].reduce(list("abcde"), initial="")
        treed = await world.tasks["async_tree_concat"].reduce(list("abcde"), initial="", chunk_size=2)
        return chained, treed

    with activated(world):
        assert run(with_events(world, body)) == ("abcde", "abcde")
    levels = sorted(claim.reduce_node[0] for claim in reduce_claims(world, "async_tree_concat"))
    assert levels == [0, 0, 0, 1, 2], "three chunks of two, two and one, then two combines"


def test_a_reduce_seeded_with_an_initial_value_longer_than_an_inline_value_reads_it_back_in_a_chain_and_in_every_chunk_of_a_tree():
    world = reducing_world(
        chain_concat, count_letters, count_letters={"tree": True, "merge": add_counts}
    )
    long_text = "s" * (INLINE_VALUE_BYTES * 3)
    long_key = "k" * (INLINE_VALUE_BYTES * 3)

    async def body():
        chained = await world.tasks["chain_concat"].reduce(list("abc"), initial=long_text)
        treed = await world.tasks["count_letters"].reduce(
            list("abab"), initial={long_key: 0}, chunk_size=2
        )
        return chained, treed

    with activated(world):
        assert run(with_events(world, body)) == (long_text + "abc", {long_key: 0, "a": 2, "b": 2})
    levels = sorted(claim.reduce_node[0] for claim in reduce_claims(world, "count_letters"))
    assert levels == [0, 0, 1], "two chunks each from the initial value, then merged"


def split_words(text: str) -> list[str]:
    return text.split()


def upper(word: str) -> str:
    return word.upper()


def join_words(item: str, previous: str) -> str:
    return f"{previous} {item}".strip()


def numbered(count: int) -> list[str]:
    """`count` items starting with their own index: every seventh is just that
    index, short enough to sit inline on an item page, and the rest about 1 KB,
    stored as blobs."""
    return [f"{index:06d}" + ("" if index % 7 == 0 else "x" * 994) for index in range(count)]


def take_tag(item: str, previous: str) -> str:
    return previous + item[:6]


def test_a_reduce_stage_folds_a_maps_results_from_its_pages_and_a_returned_list_streamed_by_its_expansion():
    world = reducing_world(split_words, upper, join_words, join_words={"associative": True})
    join = world.tasks["join_words"]

    async def body():
        mapped = await flow(world.tasks["split_words"], world.tasks["upper"].map, join.reduce.options(chunk_size=2)).start(
            world.session, "a b c d e"
        )
        expanded = await flow(world.tasks["split_words"], join.reduce.options(initial="")).start(world.session, "x y")
        return mapped[-1], expanded[-1]

    with activated(world):
        assert run(with_events(world, body)) == ("A B C D E", "x y")
    expansions = [claim for claim in world.runtime.claimed if claim.expansion]
    assert len(expansions) == 2, "the map's list and the second flow's list were streamed; the map's results were not"


def test_a_list_larger_than_one_record_reduces_in_order_across_item_pages_whether_it_is_given_or_returned():
    count = _native.MAX_RECORD_BYTES // 800 + 64  # more item bytes than one record holds
    world = reducing_world(numbered, take_tag, take_tag={"tree": True, "merge": concatenated})
    tag = world.tasks["take_tag"].reduce.options(initial="", chunk_size=100)

    async def body():
        given = await world.tasks["take_tag"].reduce(numbered(count), initial="", chunk_size=100)
        returned = await flow(world.tasks["numbered"], tag).start(world.session, count)
        return given, returned[-1]

    expected = "".join(f"{index:06d}" for index in range(count))
    with activated(world):
        assert run(with_events(world, body)) == (expected, expected)
    reduces = {composite for composite, _, _ in world.runtime.members_sent}
    assert len(reduces) == 2 and len(world.runtime.members_sent) >= 4, "each list went in more than one batch"


def test_a_reduce_cannot_be_the_first_stage_of_a_flow_and_says_how_to_give_it_items():
    world = reducing_world(chain_concat)

    with pytest.raises(TypeError, match=r"chain_concat.*first stage.*reduce\(items\)"):
        flow(world.tasks["chain_concat"].reduce)
