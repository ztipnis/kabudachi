"""Steps that run one after another or side by side: `.bind()` fixes inputs,
`flow()` runs stages in sequence, `group()` runs members together, and a task
that returns a step runs it as its continuation after its own run is certified,
the original handle resolving to the continuation's results. Python-side
properties are checked too: payloads are folded in order whatever the
compaction points, and the stage after a group runs exactly once."""

import asyncio
import functools
import pickle
import threading
from types import SimpleNamespace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from kabudachi import _native
from kabudachi._native import KabudachiError
from kabudachi.body import fold_payloads
from kabudachi.errors import (
    BackpressureError,
    PayloadTooLargeError,
    RunStoppedError,
    RuntimeNotStartedError,
    SerializationError,
    TaskCancelledError,
    TaskDefinitionError,
    TaskFailure,
    UnknownTaskError,
)
from kabudachi.flow import BoundTask, Flow, Group, flow, group
from kabudachi.registry import TaskKind, TaskRegistry
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


def test_stages_must_fit_together_by_type():
    with pytest.raises(TaskDefinitionError, match="to_receipt"):
        flow(declare(to_receipt), declare(shout))


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


def test_every_member_that_takes_the_prior_output_is_checked_against_it():
    with pytest.raises(TaskDefinitionError, match="shout"):
        flow(declare(to_receipt), group(declare(shout)))


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


def test_a_map_stage_must_follow_a_stage_that_returns_a_list_of_what_the_task_takes():
    def numbers_of(request: Greeting) -> list[Receipt]:
        return [Receipt(ok=True)]

    with pytest.raises(TaskDefinitionError, match="shout"):
        flow(declare(numbers_of), declare(shout).map)


# --- backpressure -------------------------------------------------------

# Bytes of serialized input. Past the soft limit the runtime raises SlowDown;
# a submission that would go past the hard one is refused.
SOFT_LIMIT = 100
HARD_LIMIT = 1_000


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


def test_a_group_past_the_hard_limit_is_refused_whole_and_starts_nothing():
    # Two members would fit and the third would not: the group is refused as a
    # whole, so none of them is started.
    world, released = held_world(hard_limit=100)
    members = group(*([world.tasks["hold"]] * 3))

    async def body():
        with pytest.raises(BackpressureError):
            members.start(world.session, Greeting(text="x" * 40))
        released.set()
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert world.runtime.claimed == []
    assert run_states(world) == []


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


def test_a_returned_step_bound_to_a_value_too_large_to_inline_runs_with_that_value():
    big = "z" * 1_100_000
    world = continuing_world()

    def plan(request: Greeting) -> Group:
        return group(world.tasks["shout"].bind(Greeting(text=big)), world.tasks["acknowledge"].bind(request))

    planner = declare_planner(world, plan)

    results = run_planner(world, planner, Greeting(text="in"))

    assert results == [Greeting(text=big.upper()), Receipt(ok=True)]


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
