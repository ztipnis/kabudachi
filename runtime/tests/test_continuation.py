"""A task that returns a step (an implicit flow): its run is certified first,
then the returned flow, group or bound task runs as its continuation, and the
original handle resolves to that continuation's results (README §3.4)."""

import asyncio
import threading

import pytest

from kabudachi.errors import (
    RunStoppedError,
    TaskCancelledError,
    TaskDefinitionError,
    UnknownTaskError,
)
from kabudachi.flow import BoundTask, Flow, Group, flow, group
from kabudachi.registry import TaskRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.tasks import Task
from proto_messages import Greeting, Receipt
from session_world import WAIT, World, run, with_events


def shout(request: Greeting) -> Greeting:
    return Greeting(text=request.text.upper(), times=request.times)


def wrap(request: Greeting) -> Greeting:
    return Greeting(text=f"[{request.text}]", times=request.times)


def to_receipt(request: Greeting) -> Receipt:
    return Receipt(ok=True)


def continuing_world(*functions, **options):
    world = World(shout, wrap, to_receipt, *functions, **options)
    return world


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


def test_a_returned_flow_runs_after_the_certification_and_the_handle_is_its_results():
    world = continuing_world()

    def plan(request: Greeting) -> Flow:
        return flow(world.tasks["shout"].bind(request), world.tasks["wrap"])

    planner = declare_planner(world, plan)

    results = run_planner(world, planner, Greeting(text="hi"))

    assert results == [Greeting(text="HI"), Greeting(text="[HI]")]
    order = [event[0] for event in world.runtime.events]
    first_certification = order.index("complete")
    # The planning run is certified before any stage of the continuation starts.
    assert order[: first_certification + 1].count("started") == 1


def test_a_returned_group_resolves_to_the_members_results():
    world = continuing_world()

    def plan(request: Greeting) -> Group:
        return group(world.tasks["shout"].bind(request), world.tasks["to_receipt"].bind(request))

    planner = declare_planner(world, plan)

    assert run_planner(world, planner, Greeting(text="a")) == [Greeting(text="A"), Receipt(ok=True)]


def test_a_returned_bound_task_resolves_to_a_list_of_one():
    world = continuing_world()

    def plan(request: Greeting) -> BoundTask:
        return world.tasks["shout"].bind(request)

    planner = declare_planner(world, plan)

    assert run_planner(world, planner, Greeting(text="a")) == [Greeting(text="A")]


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

    run(with_events(world, body))

    assert [entry[1] for entry in world.runtime.submitted] == ["tests.plan"]


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


def test_the_continuation_is_ended_once_when_it_succeeds():
    world = continuing_world()

    def plan(request: Greeting) -> BoundTask:
        return world.tasks["shout"].bind(request)

    planner = declare_planner(world, plan)
    run_planner(world, planner)

    assert [e[0] for e in world.runtime.events].count("end_continuation") == 1


def test_a_continuation_that_fails_is_still_ended():
    def explode(request: Greeting) -> Greeting:
        raise ValueError("no")

    world = continuing_world(explode)

    def plan(request: Greeting) -> BoundTask:
        return world.tasks["explode"].bind(request)

    planner = declare_planner(world, plan)

    async def body():
        with pytest.raises(ValueError):
            await world.session.submit(planner.definition, Greeting())

    run(with_events(world, body))

    assert [e[0] for e in world.runtime.events].count("end_continuation") == 1


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
    assert [e[0] for e in world.runtime.events].count("end_continuation") == 1


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
        while len(world.runtime.submitted) < 2:  # the planning task, then its first stage
            await asyncio.sleep(0.005)
        world.session.stop_claiming()
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


def test_a_cancel_that_arrives_while_the_first_stage_is_being_submitted_is_not_lost():
    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(30)
        return request

    world = continuing_world(slow)

    def plan(request: Greeting) -> BoundTask:
        return world.tasks["slow"].bind(request)

    planner = declare_planner(world, plan)
    handles = []
    cancelled = []
    threads = []

    def cancel_soon(run_id):
        # Only the continuation's own submission is slow, so the cancel lands in it.
        world.runtime.submit_lingers_for = 0.3
        thread = threading.Thread(target=lambda: cancelled.append(handles[0].cancel()))
        thread.start()
        threads.append(thread)

    world.runtime.after_certification = cancel_soon

    async def body():
        handles.append(world.session.submit(planner.definition, Greeting()))
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handles[0], WAIT)
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))
    for thread in threads:
        thread.join()

    assert cancelled == [True]
