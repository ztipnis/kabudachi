""".bind() and flow(): fixed inputs, and stages that run one after another."""

import asyncio

import pytest

from kabudachi.errors import (
    RunStoppedError,
    RuntimeNotStartedError,
    SerializationError,
    TaskCancelledError,
    TaskDefinitionError,
    TaskInterruptedError,
)
from kabudachi.flow import flow, group
from kabudachi.registry import TaskRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.tasks import Task
from proto_messages import Greeting, Receipt
from session_world import WAIT, Pickle, World, activated, run, with_events


def declare(function):
    return Task(
        function,
        registry=TaskRegistry(),
        serializers=SerializerRegistry.with_defaults(),
        name=f"tests.{function.__name__}",
    )


def shout(request: Greeting) -> Greeting:
    return Greeting(text=request.text.upper(), times=request.times)


def to_receipt(request: Greeting) -> Receipt:
    return Receipt(ok=request.times > 0)


def wrap(request: Greeting) -> Greeting:
    return Greeting(text=f"[{request.text}]", times=request.times)


def numbers(request: list[Greeting]) -> list[Greeting]:
    return request


# --- bind ---------------------------------------------------------------


def test_binding_a_value_fully_binds_the_input_so_the_bound_task_takes_none():
    bound = declare(shout).bind(Greeting(text="x"))

    assert bound.needs_input is False
    with pytest.raises(TypeError):
        bound(Greeting())
    with pytest.raises(RuntimeNotStartedError):
        bound()


def test_binding_fields_overlays_them_on_the_input_the_bound_task_is_called_with():
    bound = declare(shout).bind(text="fixed")

    assert bound.needs_input is True
    with pytest.raises(TypeError):
        bound()
    with pytest.raises(RuntimeNotStartedError):
        bound(Greeting(times=3))


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


def test_a_bound_field_of_the_wrong_type_is_refused_when_it_is_bound():
    with pytest.raises((TypeError, ValueError)):
        declare(shout).bind(times="many")


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


def test_a_stage_that_ignores_the_prior_output_may_follow_any_stage():
    assert flow(declare(to_receipt), declare(shout).bind(Greeting(text="x"))) is not None


def test_a_flow_of_bound_and_plain_stages_is_accepted():
    assert flow(declare(shout), declare(wrap).bind(times=2), declare(to_receipt)) is not None


def test_a_flow_whose_first_stage_is_fully_bound_takes_no_argument():
    pipeline = flow(declare(shout).bind(Greeting(text="x")), declare(wrap))

    with pytest.raises(RuntimeNotStartedError):
        pipeline()
    with pytest.raises(TypeError):
        pipeline(Greeting())


def test_a_flow_whose_first_stage_takes_input_needs_one_argument():
    pipeline = flow(declare(shout), declare(wrap))

    with pytest.raises(TypeError):
        pipeline()
    with pytest.raises(RuntimeNotStartedError):
        pipeline(Greeting())


# --- running flows ------------------------------------------------------


def flow_world(*functions, **options):
    world = World(*functions, **options)
    return world


def stages(world, *names):
    return [world.tasks[name] for name in names]


def test_stages_run_in_order_each_receiving_the_prior_output_and_the_handle_is_the_array():
    world = flow_world(shout, wrap)
    pipeline = flow(*stages(world, "shout", "wrap"))

    async def body():
        return await world.session.submit_flow(pipeline, Greeting(text="hi", times=1))

    results = run(with_events(world, body))

    assert results == [Greeting(text="HI", times=1), Greeting(text="[HI]", times=1)]


def test_a_stage_may_change_the_type_the_next_stage_receives():
    world = flow_world(shout, to_receipt)
    pipeline = flow(*stages(world, "shout", "to_receipt"))

    async def body():
        return await world.session.submit_flow(pipeline, Greeting(text="x", times=2))

    assert run(with_events(world, body))[-1] == Receipt(ok=True)


def test_an_overlay_stage_gets_its_fixed_fields_on_top_of_the_prior_output():
    world = flow_world(shout, wrap)
    pipeline = flow(world.tasks["shout"], world.tasks["wrap"].bind(times=9))

    async def body():
        return await world.session.submit_flow(pipeline, Greeting(text="hi", times=1))

    results = run(with_events(world, body))

    assert results[-1] == Greeting(text="[HI]", times=9)


def test_a_fully_bound_stage_ignores_the_prior_output():
    world = flow_world(shout, wrap)
    pipeline = flow(world.tasks["shout"], world.tasks["wrap"].bind(Greeting(text="fixed")))

    async def body():
        return await world.session.submit_flow(pipeline, Greeting(text="hi"))

    assert run(with_events(world, body))[-1].text == "[fixed]"


def test_a_failed_stage_fails_the_flow_and_later_stages_never_start():
    ran = []

    def explode(request: Greeting) -> Greeting:
        raise ValueError("stage failed")

    def after(request: Greeting) -> Greeting:
        ran.append(True)
        return request

    world = flow_world(shout, explode, after)
    pipeline = flow(*stages(world, "shout", "explode", "after"))

    async def body():
        with pytest.raises(ValueError, match="stage failed"):
            await world.session.submit_flow(pipeline, Greeting())

    run(with_events(world, body))

    assert ran == []


def test_a_stage_that_was_never_started_because_the_run_stopped_fails_the_flow():
    world = flow_world(shout, wrap)
    pipeline = flow(*stages(world, "shout", "wrap"))
    world.runtime.hold_claims = True

    async def body():
        await world.call("shout", Greeting())  # takes the only claim
        handle = world.session.submit_flow(pipeline, Greeting())
        await asyncio.sleep(0.05)
        world.session.stop_claiming()
        with pytest.raises(RunStoppedError):
            await asyncio.wait_for(handle, WAIT)

    run(with_events(world, body))


def test_cancelling_a_flow_cancels_the_current_stage_and_starts_no_later_one():
    ran = []

    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(30)
        return request

    def after(request: Greeting) -> Greeting:
        ran.append(True)
        return request

    world = flow_world(slow, after)
    pipeline = flow(*stages(world, "slow", "after"))

    async def body():
        handle = world.session.submit_flow(pipeline, Greeting())
        await asyncio.sleep(0.05)
        assert handle.cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert ran == []


def test_cancelling_a_finished_flow_changes_nothing():
    world = flow_world(shout)
    pipeline = flow(*stages(world, "shout"))

    async def body():
        handle = world.session.submit_flow(pipeline, Greeting(text="a"))
        await handle
        return handle.cancel()

    assert run(with_events(world, body)) is False


def test_finishing_waits_for_a_flow_that_is_still_running():
    world = flow_world(shout, wrap)
    pipeline = flow(*stages(world, "shout", "wrap"))
    handles = []

    async def body():
        handles.append(world.session.submit_flow(pipeline, Greeting(text="a")))
        # Nothing has been submitted yet, so only the flow itself is pending.
        await world.session.wait_until_idle()
        assert handles[0].done()
        return await handles[0]

    assert run(with_events(world, body)) == [Greeting(text="A"), Greeting(text="[A]")]


def test_a_task_that_waits_for_a_flow_gives_up_its_place_meanwhile():
    async def inner_stage(request: Greeting) -> Greeting:
        return request

    world = flow_world(inner_stage, concurrency=1)

    async def outer(request: Greeting) -> Greeting:
        await world.session.submit_flow(flow(world.tasks["inner_stage"]), request)
        return request

    outer_task = Task(
        outer, registry=world.registry, serializers=world.serializers, name="tests.outer"
    )

    async def body():
        return await world.session.submit(outer_task.definition, Greeting(text="ok"))

    assert run(with_events(world, body)).text == "ok"


def test_a_flow_callback_gets_the_array_of_results():
    world = flow_world(shout)
    pipeline = flow(*stages(world, "shout"))
    seen = []

    async def body():
        handle = world.session.submit_flow(pipeline, Greeting(text="a"))
        handle.callback(seen.append)
        await handle
        await world.session.wait_until_idle()

    run(with_events(world, body))

    assert seen == [[Greeting(text="A")]]


def test_a_flow_submitted_outside_a_running_loop_is_refused():
    world = flow_world(shout)
    pipeline = flow(*stages(world, "shout"))

    with pytest.raises(RuntimeNotStartedError):
        world.session.submit_flow(pipeline, Greeting())


def test_a_flow_cancelled_before_its_first_stage_starts_submits_nothing():
    world = flow_world(shout, wrap)
    pipeline = flow(*stages(world, "shout", "wrap"))

    async def body():
        handle = world.session.submit_flow(pipeline, Greeting())
        assert handle.cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)

    run(with_events(world, body))

    assert world.runtime.submitted == []


# --- group --------------------------------------------------------------


def test_a_group_needs_members_a_known_policy_and_only_takes_steps():
    with pytest.raises(ValueError):
        group()
    with pytest.raises(ValueError, match="on_error"):
        group(declare(shout), on_error="ignore")
    with pytest.raises(TypeError):
        group(shout)


def test_a_group_takes_an_argument_when_any_member_needs_one():
    fixed = group(declare(shout).bind(Greeting()), declare(wrap).bind(Greeting()))
    open_ = group(declare(shout).bind(Greeting()), declare(wrap))

    with pytest.raises(TypeError):
        fixed(Greeting())
    with pytest.raises(RuntimeNotStartedError):
        fixed()
    with pytest.raises(TypeError):
        open_()
    with pytest.raises(RuntimeNotStartedError):
        open_(Greeting())


def test_the_stage_after_a_group_takes_the_array_so_its_type_is_not_checked_against_a_member():
    assert flow(group(declare(shout), declare(wrap)), declare(to_receipt).bind(Greeting())) is not None


def test_every_member_that_takes_the_prior_output_is_checked_against_it():
    with pytest.raises(TaskDefinitionError, match="shout"):
        flow(declare(to_receipt), group(declare(shout)))


def test_groups_and_flows_nest():
    inner = flow(declare(shout), declare(wrap))
    assert flow(declare(shout), group(inner, declare(wrap))) is not None


def group_world(*functions, **options):
    return World(*functions, **options)


def test_every_member_gets_the_same_input_and_the_result_is_in_member_order():
    world = group_world(shout, wrap)
    members = group(world.tasks["shout"], world.tasks["wrap"].bind(times=4))

    async def body():
        return await world.session.submit_group(members, Greeting(text="hi", times=1))

    assert run(with_events(world, body)) == [
        Greeting(text="HI", times=1),
        Greeting(text="[hi]", times=4),
    ]


def test_the_order_of_results_does_not_depend_on_which_member_finishes_first():
    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(0.15)
        return Greeting(text="slow")

    async def quick(request: Greeting) -> Greeting:
        return Greeting(text="quick")

    world = group_world(slow, quick)
    members = group(world.tasks["slow"], world.tasks["quick"])

    async def body():
        return await world.session.submit_group(members, Greeting())

    assert [g.text for g in run(with_events(world, body))] == ["slow", "quick"]


def failing_and_ok():
    ran_after = []

    def explode(request: Greeting) -> Greeting:
        raise ValueError("member failed")

    def fine(request: Greeting) -> Greeting:
        return Greeting(text="fine")

    def after(request: list[Greeting]) -> Greeting:
        ran_after.append(request)
        return Greeting(text="after")

    return explode, fine, after, ran_after


def test_fail_fast_raises_the_first_failure_and_the_stage_after_never_starts():
    explode, fine, after, ran_after = failing_and_ok()
    world = group_world(explode, fine, after)
    pipeline = flow(group(world.tasks["explode"], world.tasks["fine"]), world.tasks["after"])

    async def body():
        with pytest.raises(ValueError, match="member failed"):
            await world.session.submit_flow(pipeline, Greeting())
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert ran_after == []


def test_fail_fast_leaves_the_other_members_running_like_asyncio_gather():
    finished = []

    def explode(request: Greeting) -> Greeting:
        raise ValueError("member failed")

    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(0.1)
        finished.append(True)
        return request

    world = group_world(explode, slow)
    members = group(world.tasks["explode"], world.tasks["slow"])

    async def body():
        with pytest.raises(ValueError):
            await world.session.submit_group(members, Greeting())
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert finished == [True]


def test_collect_all_returns_every_value_and_failure_and_the_next_stage_runs_once():
    explode, fine, _, _ = failing_and_ok()
    received = []

    def after(request: list) -> str:
        received.append(request)
        return "after"

    world = group_world(explode, fine)
    world.serializers.register(Pickle())
    after_task = Task(
        after,
        registry=world.registry,
        serializers=world.serializers,
        name="tests.after",
        serializer="pickle",
    )
    pipeline = flow(
        group(world.tasks["explode"], world.tasks["fine"], on_error="collect_all"),
        after_task,
    )

    async def body():
        return await world.session.submit_flow(pipeline, Greeting())

    results = run(with_events(world, body))

    [(failure, value)] = received
    assert isinstance(failure, ValueError) and value == Greeting(text="fine")
    assert isinstance(results[0][0], ValueError) and results[0][1] == Greeting(text="fine")
    assert results[1] == "after"


def test_a_stage_that_cannot_encode_a_failure_fails_the_flow_with_a_serialization_error():
    explode, fine, after, _ = failing_and_ok()
    world = group_world(explode, fine, after)
    pipeline = flow(
        group(world.tasks["explode"], world.tasks["fine"], on_error="collect_all"),
        world.tasks["after"],
    )

    async def body():
        with pytest.raises(SerializationError):
            await world.session.submit_flow(pipeline, Greeting())

    run(with_events(world, body))


def test_the_stage_after_a_group_runs_once_even_when_members_are_retried():
    attempts = {"a": 0, "b": 0}
    after_calls = []

    def make(name):
        def member(request: Greeting) -> Greeting:
            attempts[name] += 1
            if attempts[name] < 3:
                raise ValueError("retry me")
            return Greeting(text=name)

        member.__name__ = f"member_{name}"
        return member

    def after(request: list[Greeting]) -> Greeting:
        after_calls.append(len(request))
        return Greeting(text="after")

    a, b = make("a"), make("b")
    world = group_world(a, b, after, retries=2)
    pipeline = flow(group(world.tasks["member_a"], world.tasks["member_b"]), world.tasks["after"])

    async def body():
        return await world.session.submit_flow(pipeline, Greeting())

    run(with_events(world, body))

    assert attempts == {"a": 3, "b": 3}
    assert after_calls == [2]


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

    world = group_world(slow, after, concurrency=4)
    pipeline = flow(
        group(world.tasks["slow"], world.tasks["slow"], world.tasks["slow"]), world.tasks["after"]
    )

    async def body():
        handle = world.session.submit_flow(pipeline, Greeting())
        while len(started) < 3:
            await asyncio.sleep(0.005)
        assert handle.cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert ran_after == []


def test_a_group_cancelled_before_its_members_start_submits_nothing():
    world = group_world(shout, wrap)
    members = group(world.tasks["shout"], world.tasks["wrap"])

    async def body():
        handle = world.session.submit_group(members, Greeting())
        assert handle.cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)

    run(with_events(world, body))

    assert world.runtime.submitted == []


def test_cancelling_a_finished_group_changes_nothing():
    world = group_world(shout, wrap)
    members = group(world.tasks["shout"], world.tasks["wrap"])

    async def body():
        handle = world.session.submit_group(members, Greeting(text="a"))
        await handle
        return handle.cancel()

    assert run(with_events(world, body)) is False


def test_a_group_with_a_member_the_run_stopped_before_starting_fails_the_group():
    world = group_world(shout, wrap)
    members = group(world.tasks["shout"], world.tasks["wrap"])
    world.runtime.hold_claims = True

    async def body():
        await world.call("shout", Greeting())  # takes the only claim
        handle = world.session.submit_group(members, Greeting())
        await asyncio.sleep(0.05)
        world.session.stop_claiming()
        with pytest.raises(RunStoppedError):
            await asyncio.wait_for(handle, WAIT)

    run(with_events(world, body))


def test_flows_and_groups_nest_and_the_results_nest_the_same_way():
    world = group_world(shout, wrap, to_receipt)
    inner = flow(world.tasks["shout"], world.tasks["wrap"])
    pipeline = flow(
        world.tasks["shout"],
        group(inner, world.tasks["to_receipt"]),
    )

    async def body():
        return await world.session.submit_flow(pipeline, Greeting(text="a", times=1))

    assert run(with_events(world, body)) == [
        Greeting(text="A", times=1),
        [
            [Greeting(text="A", times=1), Greeting(text="[A]", times=1)],
            Receipt(ok=True),
        ],
    ]


def test_finishing_waits_for_a_group_that_is_still_running():
    world = group_world(shout, wrap)
    members = group(world.tasks["shout"], world.tasks["wrap"])

    async def body():
        handle = world.session.submit_group(members, Greeting(text="a"))
        await world.session.wait_until_idle()
        assert handle.done()

    run(with_events(world, body))


# --- map ----------------------------------------------------------------


def greetings(*texts):
    return [Greeting(text=text) for text in texts]


def test_map_needs_a_list_of_inputs_and_checks_each_one_before_submitting_any():
    world = group_world(shout)

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
        await asyncio.sleep(0.2 if request.text == "a" else 0)
        return Greeting(text=request.text.upper())

    world = group_world(slow_first, concurrency=8)

    async def body():
        with activated(world):
            return await world.tasks["slow_first"].map(greetings("a", "b", "c"))

    assert [g.text for g in run(with_events(world, body))] == ["A", "B", "C"]


def test_mapping_an_empty_list_gives_an_empty_list_and_submits_nothing():
    world = group_world(shout)

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

    world = group_world(sometimes)

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

    world = group_world(load, shout)
    pipeline = flow(world.tasks["load"], world.tasks["shout"].map)

    async def body():
        return await world.session.submit_flow(pipeline, Greeting(text="a b c"))

    loaded, mapped = run(with_events(world, body))

    assert [g.text for g in loaded] == ["a", "b", "c"]
    assert [g.text for g in mapped] == ["A", "B", "C"]


def test_a_map_stage_must_follow_a_stage_that_returns_a_list_of_what_the_task_takes():
    def numbers_of(request: Greeting) -> list[Receipt]:
        return [Receipt(ok=True)]

    with pytest.raises(TaskDefinitionError, match="shout"):
        flow(declare(numbers_of), declare(shout).map)


# --- backpressure -------------------------------------------------------


def test_a_group_pauses_submitting_while_slow_down_is_raised_but_a_plain_call_does_not():
    world = group_world(shout)
    members = group(*([world.tasks["shout"]] * 3))

    async def body():
        world.runtime.slow_down(True)
        await asyncio.sleep(0.05)
        handle = world.session.submit_group(members, Greeting(text="a"))
        await asyncio.sleep(0.15)
        during = len(world.runtime.submitted)
        plain = world.call("shout", Greeting(text="plain"))
        await plain
        while_slow = len(world.runtime.submitted)
        world.runtime.slow_down(False)
        await asyncio.wait_for(handle, WAIT)
        return during, while_slow, len(world.runtime.submitted)

    during, while_slow, after = run(with_events(world, body))

    assert (during, while_slow, after) == (0, 1, 4)


def test_map_pauses_its_bulk_submission_on_slow_down_too():
    world = group_world(shout)

    async def body():
        world.runtime.slow_down(True)
        await asyncio.sleep(0.05)
        with activated(world):
            handle = world.tasks["shout"].map(greetings("a", "b"))
            await asyncio.sleep(0.15)
            during = len(world.runtime.submitted)
            world.runtime.slow_down(False)
            await asyncio.wait_for(handle, WAIT)
        return during

    assert run(with_events(world, body)) == 0


def test_stopping_ends_a_group_that_is_waiting_out_slow_down():
    world = group_world(shout)
    members = group(world.tasks["shout"], world.tasks["shout"])

    async def body():
        world.runtime.slow_down(True)
        await asyncio.sleep(0.05)
        handle = world.session.submit_group(members, Greeting())
        await asyncio.sleep(0.1)
        world.session.stop_claiming()
        with pytest.raises(RunStoppedError):
            await asyncio.wait_for(handle, WAIT)

    run(with_events(world, body))


def test_cancelling_a_group_that_is_waiting_out_slow_down_submits_nothing():
    world = group_world(shout)
    members = group(world.tasks["shout"], world.tasks["shout"])

    async def body():
        world.runtime.slow_down(True)
        await asyncio.sleep(0.05)
        handle = world.session.submit_group(members, Greeting())
        await asyncio.sleep(0.1)
        assert handle.cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)

    run(with_events(world, body))

    assert world.runtime.submitted == []


def test_a_plain_call_past_the_hard_limit_raises_backpressure_error():
    from kabudachi.errors import BackpressureError

    world = group_world(shout)
    world.runtime.refuse_after = 1

    async def body():
        await world.call("shout", Greeting())
        with pytest.raises(BackpressureError):
            world.call("shout", Greeting())

    run(with_events(world, body))


def test_a_group_that_hits_the_hard_limit_midway_fails_and_cancels_the_members_it_started():
    from kabudachi.errors import BackpressureError

    world = group_world(shout)
    world.runtime.hold_claims = True
    world.runtime.refuse_after = 2
    members = group(*([world.tasks["shout"]] * 3))

    async def body():
        with pytest.raises(BackpressureError):
            await world.session.submit_group(members, Greeting())
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert len(world.runtime.submitted) == 2
    assert len(world.runtime.cancelled) == 2


def test_a_bulk_submission_pauses_when_slow_down_is_raised_while_it_is_still_submitting():
    world = group_world(shout, concurrency=8)
    world.runtime.slow_down_after = 2
    members = group(*([world.tasks["shout"]] * 6))

    async def body():
        handle = world.session.submit_group(members, Greeting(text="a"))
        await asyncio.sleep(0.2)
        while_slow = len(world.runtime.submitted)
        world.runtime.slow_down(False)
        await asyncio.wait_for(handle, WAIT)
        return while_slow, len(world.runtime.submitted)

    while_slow, after = run(with_events(world, body))

    assert while_slow == 2, "the group kept submitting past the soft limit"
    assert after == 6


def test_cancelling_a_group_part_way_through_its_submission_starts_no_further_member():
    world = group_world(shout, concurrency=8)
    world.runtime.hold_claims = True
    members = group(*([world.tasks["shout"]] * 6))
    handles = []

    async def body():
        handles.append(world.session.submit_group(members, Greeting()))
        while len(world.runtime.submitted) < 2:
            await asyncio.sleep(0)
        assert handles[0].cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handles[0], WAIT)
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert len(world.runtime.submitted) < 6
    assert len(world.runtime.cancelled) == len(world.runtime.submitted)


def test_a_flow_whose_orchestration_is_cancelled_before_it_starts_still_settles_and_is_not_leaked():
    world = flow_world(shout)
    pipeline = flow(*stages(world, "shout"))

    async def body():
        handle = world.session.submit_flow(pipeline, Greeting())
        await asyncio.sleep(0)  # the orchestration task exists now, and has not started
        for task in asyncio.all_tasks():
            if "_run_flow" in repr(task.get_coro()):
                task.cancel()
        with pytest.raises(TaskInterruptedError):
            await asyncio.wait_for(handle, WAIT)
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))
