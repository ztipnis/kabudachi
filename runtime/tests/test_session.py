"""The session: what happens between a task being called and its handle being
settled, checked against a real native runtime that a few faults can be
provoked on."""

import asyncio
import gc
import logging
import threading
import time
from datetime import timedelta

import pytest

from kabudachi._native import EventKind, RunState, result_digest
from kabudachi.concurrency_places import ConcurrencyPlaces
from kabudachi.config import Configuration
from kabudachi.errors import (
    CertificationError,
    CoalescedPayloadTooLargeError,
    RunStoppedError,
    SerializationError,
    TaskCancelledError,
    TaskDefinitionError,
    TaskExpiredError,
    TaskRecordFullError,
    TaskSupersededError,
    TaskTimeoutError,
    UnknownTaskError,
)
from kabudachi.options import SubmissionOptions
from kabudachi.registry import TaskKind, TaskRegistry
from kabudachi.serializers import SerializerRegistry
from kabudachi.session import Session, validate_definitions
from kabudachi.tasks import Task
from proto_messages import Greeting, Receipt
from session_world import WAIT, World, run, until, with_events


def echo(request: Greeting) -> Greeting:
    return request


def explode(request: Greeting) -> Greeting:
    raise ValueError(f"cannot handle {request.text}")



@pytest.mark.parametrize(
    ("task_options", "default_queue", "version", "queue"),
    [
        pytest.param({}, None, 0, "default", id="defaults"),
        pytest.param({"queue": "gpu", "version": 4}, None, 4, "gpu", id="the task's own queue and version"),
        pytest.param({}, "emails", 0, "emails", id="configured default queue"),
    ],
)
def test_submission_carries_the_task_definition_encoded_input_queue_and_version(
    task_options, default_queue, version, queue
):
    world = World(echo, **task_options)
    if default_queue is not None:
        world.configuration.configure(queue=default_queue)
    argument = Greeting(text="hi", times=2)

    world.call("echo", argument)

    [(_task_id, definition_id, submitted_version, payload, submitted_queue)] = world.runtime.submitted
    assert definition_id == "tests.echo"
    assert payload == argument.SerializeToString()
    assert (submitted_version, submitted_queue) == (version, queue)


def test_an_argument_of_the_wrong_type_is_refused_before_anything_is_submitted():
    world = World(echo)

    with pytest.raises(SerializationError):
        world.call("echo", Receipt(ok=True))

    assert world.runtime.submitted == []


def test_the_handle_is_not_settled_before_the_leader_certifies():
    world = World(echo)
    seen = {}

    async def body():
        handle = world.call("echo", Greeting(text="x"))
        world.runtime.on_complete = lambda run_id: seen.setdefault("done", handle.done())
        return await handle

    run(world.working(body))

    assert seen == {"done": False}
    assert [event[0] for event in world.runtime.events] == ["started", "complete"]


def test_a_result_the_leader_refuses_to_certify_is_never_delivered():
    world = World(echo)
    world.runtime.refuse_completion = True

    async def body():
        return await world.call("echo", Greeting(text="secret"))

    with pytest.raises(RuntimeError, match="does not belong"):
        run(world.working(body))


def test_a_certification_for_a_different_result_is_never_delivered():
    world = World(echo)
    world.runtime.altered_digest = result_digest(b"something else")

    async def body():
        return await world.call("echo", Greeting(text="x"))

    with pytest.raises(CertificationError):
        run(world.working(body))


def test_a_failing_task_is_reported_failed_by_its_error_type_before_its_handle_settles():
    world = World(explode)
    seen_by_handle = []

    async def body():
        handle = world.call("explode", Greeting(text="secret text"))
        try:
            await handle
        except ValueError:
            seen_by_handle.append(list(world.runtime.events))

    run(world.working(body))

    [events] = seen_by_handle
    assert [event[0] for event in events] == ["started", "fail"]
    assert events[1][2] == "ValueError"
    assert all("secret text" not in str(event) for event in events)


def test_a_failure_the_leader_refuses_does_not_hide_the_tasks_error():
    world = World(explode)
    world.runtime.refuse_failure = True

    async def body():
        with pytest.raises(ValueError, match="cannot handle"):
            await world.call("explode", Greeting(text="x"))

    run(world.working(body))


def test_a_claim_that_cannot_run_is_failed_by_its_error_type_and_retried_like_any_failed_run():
    world = World(echo, retries=1)
    world.session = Session(world.runtime, TaskRegistry(), world.serializers, world.configuration)

    async def body():
        handle = world.call("echo", Greeting())
        with pytest.raises(UnknownTaskError, match=r"tests\.echo"):
            await handle
        return handle.task_id

    task_id = run(world.working(body))

    assert [(event[0], event[2]) for event in world.runtime.events] == [
        ("fail", "UnknownTaskError")
    ] * 2
    run_ids = world.native.task_run_ids(task_id)
    assert len(run_ids) == 2
    assert {world.native.task_run_state(run_id) for run_id in run_ids} == {RunState.FAILED}


def test_a_run_that_cannot_be_started_fails_its_handle_and_runs_nothing():
    ran = []

    def recording(request: Greeting) -> Greeting:
        ran.append(request)
        return request

    world = World(recording)
    world.runtime.fail_start = True

    async def body():
        return await world.call("recording", Greeting())

    with pytest.raises(RuntimeError, match="not the leader"):
        run(world.working(body))
    assert ran == []


class Garbled:
    """Encodes anything as bytes that protobuf cannot decode back."""

    name = "garbled"

    def available(self):
        return True

    def supports(self, value_type):
        return True

    def encode(self, value, value_type):
        return b"\xff\xff\xff\xff\xff"

    def decode(self, payload, target_type):
        return SerializerRegistry.with_defaults().get("protobuf").decode(payload, target_type)


def test_input_bytes_that_do_not_decode_fail_the_handle():
    world = World(echo, serializer="garbled")
    world.serializers.register(Garbled())

    async def body():
        return await world.call("echo", Greeting())

    with pytest.raises(SerializationError):
        run(world.working(body))


def test_a_task_that_finishes_before_its_submit_returns_is_still_settled():
    world = World(echo)
    world.runtime.submit_lingers_for = 0.15

    async def body():
        handles = []
        thread = threading.Thread(
            target=lambda: handles.append(world.call("echo", Greeting(text="fast")))
        )
        thread.start()
        while thread.is_alive():
            await asyncio.sleep(0.01)
        return await handles[0]

    assert run(world.working(body)).text == "fast"


def test_finishing_also_waits_for_work_that_running_tasks_submit():
    finished = []

    async def child(request: Greeting) -> Greeting:
        finished.append("child")
        return request

    world = World(child)

    async def parent(request: Greeting) -> Greeting:
        world.call("child", Greeting())
        finished.append("parent")
        return request

    parent_task = Task(
        parent,
        registry=world.registry,
        serializers=world.serializers,
        name="tests.parent",
    )

    async def body():
        world.session.submit(parent_task.definition, Greeting())
        await world.session.wait_until_idle()

    run(world.working(body))

    assert finished == ["parent", "child"]


def test_validation_names_every_task_that_cannot_work():
    registry = TaskRegistry()
    serializers = SerializerRegistry.with_defaults()

    Task(echo, registry=registry, name="tests.unknown", serializer="nope")
    Task(echo, registry=registry, name="tests.fine")

    with pytest.raises(TaskDefinitionError) as failure:
        validate_definitions(registry, serializers)

    assert "tests.unknown" in str(failure.value)
    assert "nope" in str(failure.value)
    assert "tests.fine" not in str(failure.value)


def test_validation_refuses_a_serializer_that_cannot_run_here():
    class Unavailable:
        name = "offline"

        def available(self):
            return False

        def supports(self, value_type):
            return True

        def encode(self, value, value_type):
            return b""

        def decode(self, payload, target_type):
            return None

    registry = TaskRegistry()
    serializers = SerializerRegistry.with_defaults()
    serializers.register(Unavailable())
    Task(echo, registry=registry, serializers=serializers, serializer="offline")

    with pytest.raises(TaskDefinitionError, match="offline"):
        validate_definitions(registry, serializers)


def test_validation_refuses_types_the_serializer_cannot_handle():
    class TextOnly:
        name = "text"

        def available(self):
            return True

        def supports(self, value_type):
            return value_type is str

        def encode(self, value, value_type):
            return value.encode()

        def decode(self, payload, target_type):
            return payload.decode()

    registry = TaskRegistry()
    early = SerializerRegistry.with_defaults()
    Task(echo, registry=registry, serializers=early, serializer="text", name="tests.text")
    late = SerializerRegistry.with_defaults()
    late.register(TextOnly())

    with pytest.raises(TaskDefinitionError, match="input type"):
        validate_definitions(registry, late)


def test_many_tasks_that_each_wait_for_one_they_called_do_not_deadlock():
    async def inner(request: Greeting) -> Greeting:
        await asyncio.sleep(0.01)
        return request

    world = World(inner, concurrency=4)

    async def outer(request: Greeting) -> Greeting:
        return await world.call("inner", request)

    outer_task = Task(
        outer, registry=world.registry, serializers=world.serializers, name="tests.outer"
    )

    async def body():
        handles = [
            world.session.submit(outer_task.definition, Greeting(times=n)) for n in range(40)
        ]
        return await asyncio.gather(*handles)

    assert [g.times for g in run(world.working(body))] == list(range(40))


def test_a_task_waiting_on_several_tasks_gives_up_one_place_not_several():
    peak = 0
    active = 0

    async def leaf(request: Greeting) -> Greeting:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.03)
        active -= 1
        return request

    world = World(leaf, concurrency=2)

    async def fan_out(request: Greeting) -> Greeting:
        await asyncio.gather(*(world.call("leaf", Greeting()) for _ in range(6)))
        return request

    fan_out_task = Task(
        fan_out, registry=world.registry, serializers=world.serializers, name="tests.fan_out"
    )

    async def body():
        await world.session.submit(fan_out_task.definition, Greeting())

    run(world.working(body))

    # The parent waits and so is not counted, but it gave up one place, not six.
    assert peak <= 2


def test_stopping_fails_tasks_that_have_not_started_and_refuses_new_ones():
    begun = []

    async def slow(request: Greeting) -> Greeting:
        begun.append(request.text)
        await asyncio.sleep(0.1)
        return request

    world = World(slow, concurrency=1)

    async def body():
        running = world.call("slow", Greeting(text="running"))
        queued = world.call("slow", Greeting(text="queued"))
        await until(lambda: begun, "the slow task beginning")
        world.session.stop_claiming()
        with pytest.raises(RunStoppedError):
            await queued
        with pytest.raises(RunStoppedError):
            world.call("slow", Greeting())
        return await running

    assert run(world.working(body)).text == "running"


def test_a_claim_delivered_after_stopping_does_not_run_its_failed_task():
    ran = []

    async def record(request: Greeting) -> Greeting:
        ran.append(request.text)
        return request

    world = World(record)

    async def body():
        # Let the worker block in `claim_pending`, then stop in the same step
        # as the submission, so the claim arrives after the task was failed.
        await asyncio.sleep(0.02)
        handle = world.call("record", Greeting(text="late"))
        world.session.stop_claiming()
        await asyncio.sleep(0.05)
        with pytest.raises(RunStoppedError):
            await handle

    run(world.working(body))

    assert ran == []


def test_stopping_frees_a_running_task_that_was_waiting_for_one_that_never_started():
    async def inner(request: Greeting) -> Greeting:
        return request

    world = World(inner, concurrency=1)
    order = []

    async def outer(request: Greeting) -> Greeting:
        child = world.call("inner", request)
        order.append("waiting")
        try:
            await child
        except RunStoppedError:
            order.append("freed")
            raise
        return request

    outer_task = Task(
        outer, registry=world.registry, serializers=world.serializers, name="tests.outer"
    )
    world.runtime.hold_claims = True

    async def body():
        parent = world.session.submit(outer_task.definition, Greeting())
        while "waiting" not in order:
            await asyncio.sleep(0.005)
        world.session.stop_claiming()
        await world.session.wait_until_running_finish()
        with pytest.raises(RunStoppedError):
            await parent

    run(world.working(body))

    assert order == ["waiting", "freed"]


def test_a_failure_is_logged_without_the_errors_own_text(caplog):
    world = World(explode)

    async def body():
        with pytest.raises(ValueError):
            await world.call("explode", Greeting(text="a secret payload"))

    with caplog.at_level(logging.DEBUG, logger="kabudachi"):
        run(world.working(body))

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings and all("secret" not in r.getMessage() for r in warnings)
    assert any(r.exc_info for r in caplog.records if r.levelno == logging.DEBUG)


def test_synchronous_tasks_run_as_many_at_once_as_the_concurrency_allows():
    def blocking(request: Greeting) -> Greeting:
        time.sleep(0.4)
        return request

    # More than the default thread pool ever has, so a small pool would take turns.
    world = World(blocking, concurrency=48, concurrency_override=True)

    async def body():
        started = time.monotonic()
        await asyncio.gather(*(world.call("blocking", Greeting()) for _ in range(48)))
        return time.monotonic() - started

    assert run(world.working(body)) < 0.7


def flaky(failures):
    """A task body that raises `failures` times and then succeeds; `calls` counts its runs."""
    calls = []

    def body(request: Greeting) -> Greeting:
        calls.append(request.text)
        if len(calls) <= failures:
            raise ValueError(f"attempt {len(calls)} failed")
        return request

    body.__name__ = "flaky_body"
    return body, calls


def test_the_handle_is_not_settled_by_a_failure_that_will_be_retried():
    release = asyncio.Event()
    attempts = []

    async def fails_then_waits(request: Greeting) -> Greeting:
        attempts.append(True)
        if len(attempts) == 1:
            raise ValueError("first attempt")
        await release.wait()
        return request

    world = World(fails_then_waits, retries=1)
    states = []

    async def body_():
        handle = world.call("fails_then_waits", Greeting())
        while len(attempts) < 2:
            await asyncio.sleep(0.001)
        states.append(handle.done())
        release.set()
        await handle

    run(world.working(body_))

    assert states == [False]


def test_a_task_that_keeps_failing_gives_its_handle_the_last_error_after_its_retries():
    calls = []

    def always_fails(request: Greeting) -> Greeting:
        calls.append(True)
        raise ValueError(f"failure {len(calls)}")

    world = World(always_fails, retries=2)

    async def body():
        with pytest.raises(ValueError, match="failure 3"):
            await world.call("always_fails", Greeting())

    run(world.working(body))

    assert len(calls) == 3


def test_a_retry_the_leader_never_schedules_fails_the_handle_at_once():
    body, calls = flaky(failures=5)
    world = World(body, retries=3)
    world.runtime.refuse_failure = True

    async def body_():
        with pytest.raises(ValueError):
            await world.call("flaky_body", Greeting())

    run(world.working(body_))

    assert len(calls) == 1


def test_stopping_fails_a_task_that_is_waiting_for_its_retry():
    body, _ = flaky(failures=1)
    world = World(body, retries=1)
    world.runtime.hold_claims = True

    async def body_():
        handle = world.call("flaky_body", Greeting())
        while not any(e[0] == "fail" for e in world.runtime.events):
            await asyncio.sleep(0.001)
        world.session.stop_claiming()
        with pytest.raises(RunStoppedError):
            await handle

    run(world.working(body_))


def test_finishing_waits_through_the_retries_of_a_task():
    body, calls = flaky(failures=2)
    world = World(body, retries=2)

    async def body_():
        world.call("flaky_body", Greeting())
        await world.session.wait_until_idle()

    run(world.working(body_))

    assert len(calls) == 3


SHORT_LIFE = 30


def submit_expiring(world, text=""):
    """Submits an `echo` that the runtime gives only `SHORT_LIFE` to be claimed in."""
    return world.session.submit(
        world.tasks["echo"].definition,
        Greeting(text=text),
        SubmissionOptions(expires_in_ms=SHORT_LIFE),
    )


def test_a_task_the_runtime_says_expired_fails_its_handle_with_task_expired_error():
    world = World(echo)
    world.runtime.hold_claims = True
    # The first task takes the only claim; the second is never claimed, so the
    # runtime runs it out of the short life it was submitted with.

    async def body():
        await world.call("echo", Greeting(text="claimed"))
        waiting = submit_expiring(world, "waiting")
        with pytest.raises(TaskExpiredError):
            await asyncio.wait_for(waiting, WAIT)
        # Finishing does not wait for a task that expired.
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))


def test_a_task_the_runtime_says_has_a_full_record_fails_its_handle_with_task_record_full_error():
    world = World(echo)

    async def body():
        handle = world.call("echo", Greeting(text="no room"))
        world.runtime.inject_event(EventKind.RECORD_FULL, task_id=handle.task_id)
        with pytest.raises(TaskRecordFullError):
            await asyncio.wait_for(handle, WAIT)

    run(with_events(world, body))


def test_a_task_the_runtime_says_folded_too_large_fails_its_handle_with_coalesced_payload_too_large_error():
    world = World(echo)

    async def body():
        handle = world.call("echo", Greeting(text="folded"))
        world.runtime.inject_event(EventKind.COALESCED_PAYLOAD_TOO_LARGE, task_id=handle.task_id)
        with pytest.raises(CoalescedPayloadTooLargeError):
            await asyncio.wait_for(handle, WAIT)

    run(with_events(world, body))


def test_a_compaction_the_leader_refuses_to_take_is_not_reported_as_a_failed_merge():
    from types import SimpleNamespace

    world = World(echo)
    reported = []
    world.runtime.report_failure = lambda run, kind: reported.append((run, kind))

    def refuse(run, folded):
        raise RuntimeError("the leader refused the fold")

    world.runtime.complete_compaction = refuse
    serializer = world.serializers.get(world.tasks["echo"].definition.serializer)
    payload = serializer.encode(Greeting(text="a"), Greeting)
    claim = SimpleNamespace(
        compaction=True,
        definition_id=world.tasks["echo"].definition.name,
        task_run_id="run-1",
        chain=[payload, payload],
    )

    async def body():
        world.session._start(claim)
        await world.session.wait_until_running_finish()

    run(body())

    assert reported == [], "the merge was fine; only the report was refused"


def test_an_event_for_a_task_this_session_does_not_have_is_ignored():
    world = World(echo)
    handed_to_the_session = []
    next_events = world.runtime.next_events

    async def recording_next_events():
        events = await next_events()
        handed_to_the_session.extend((event.kind, event.task_id) for event in events)
        return events

    world.runtime.next_events = recording_next_events

    async def working():
        watcher = asyncio.ensure_future(world.session.watch_events())
        # No real event names a task this session never submitted, so this one
        # is put in front of the watcher by hand.
        world.runtime.inject_event(EventKind.EXPIRED, task_id="task-that-is-not-ours")

        async def body():
            return await world.call("echo", Greeting(text="fine")), watcher.done()

        try:
            return await world.working(body)
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)

    result, watcher_ended = run(working())

    assert result.text == "fine"
    assert (EventKind.EXPIRED, "task-that-is-not-ours") in handed_to_the_session, (
        "the injected event never reached the session, so nothing was ignored"
    )
    assert not watcher_ended, "the event ended the watcher instead of being ignored"


def test_an_event_of_a_kind_the_session_does_not_know_ends_serving_with_an_error():
    world = World(echo)

    async def body():
        await asyncio.wait_for(world.native.wait_until_leader(), WAIT)
        serving = asyncio.ensure_future(world.session.serve())
        world.runtime.inject_event("mystery")
        with pytest.raises(RuntimeError, match="mystery"):
            await asyncio.wait_for(serving, WAIT)

    run(body())


def test_serving_ends_with_the_error_of_whichever_loop_breaks():
    world = World(echo)
    world.runtime.event_error = RuntimeError("events broke")

    with pytest.raises(RuntimeError, match="events broke"):
        run(asyncio.wait_for(world.session.serve(), WAIT))


SOFT = timedelta(milliseconds=60)
GRACE = timedelta(milliseconds=150)


def timed(function, **options):
    """A world with `function` declared with a soft limit of `SOFT` and a grace of `GRACE`."""
    return World(function, timeout=SOFT, cancel_grace=GRACE, **options)


def test_a_body_past_its_soft_limit_is_cancelled_and_its_run_fails_with_a_timeout():
    saw = []

    async def slow(request: Greeting) -> Greeting:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            saw.append("cancelled")
            raise
        return request

    world = timed(slow)

    async def body():
        started = time.monotonic()
        with pytest.raises(TaskTimeoutError):
            await world.call("slow", Greeting())
        return time.monotonic() - started

    elapsed = run(world.working(body))

    assert saw == ["cancelled"]
    # A body that stops as asked does not make anyone wait out the grace.
    assert SOFT.total_seconds() <= elapsed < (SOFT + GRACE).total_seconds()
    assert [event[0] for event in world.runtime.events] == ["started", "fail"]
    assert world.runtime.events[1][2] == "TaskTimeoutError"


def test_a_body_that_ignores_cancellation_fails_at_the_hard_limit_and_is_abandoned():
    state = {"still_running": False}
    release = asyncio.Event()

    async def stubborn(request: Greeting) -> Greeting:
        state["still_running"] = True
        while True:
            try:
                await release.wait()
                break
            except asyncio.CancelledError:
                continue  # refuses to stop
        state["still_running"] = False
        return request

    world = timed(stubborn)

    async def body():
        started = time.monotonic()
        with pytest.raises(TaskTimeoutError):
            await world.call("stubborn", Greeting())
        elapsed = time.monotonic() - started, state["still_running"]
        release.set()
        return elapsed

    elapsed, still_running = run(world.working(body))

    assert elapsed >= (SOFT + GRACE).total_seconds()
    assert still_running, "the body was abandoned, not killed"


def test_an_abandoned_body_keeps_its_place_until_it_exits():
    starts = []
    release = threading.Event()

    def stubborn(request: Greeting) -> Greeting:
        starts.append(request.text)
        release.wait(5)
        return request

    world = World(stubborn, concurrency=1, timeout=SOFT, cancel_grace=GRACE)

    async def body():
        first = world.call("stubborn", Greeting(text="first"))
        second = world.call("stubborn", Greeting(text="second"))
        with pytest.raises(TaskTimeoutError):
            await first
        await asyncio.sleep(0.2)
        seen_before_exit = list(starts)
        release.set()
        assert (await second).text == "second"
        return seen_before_exit

    seen_before_exit = run(world.working(body))

    # The abandoned first body still held the only place, so the second
    # task had not started while it ran.
    assert seen_before_exit == ["first"]


def test_a_retry_after_a_hard_timeout_waits_for_the_abandoned_body_to_exit():
    release = threading.Event()
    calls = []

    def stubborn_then_fine(request: Greeting) -> Greeting:
        if not calls:
            calls.append("first body started")
            release.wait(5)
            calls.append("first body exited")
            return request
        calls.append("second body ran")
        return request

    world = World(stubborn_then_fine, retries=1, timeout=SOFT, cancel_grace=GRACE)

    async def body():
        handle = world.call("stubborn_then_fine", Greeting(text="x"))
        await until(lambda: any(event[0] == "fail" for event in world.runtime.events), "a fail event")
        await asyncio.sleep(0.2)
        assert not handle.done(), "the lineage stays pending while its retry waits"
        assert len(calls) == 1, "no second body while the abandoned one runs"
        release.set()
        return await handle

    assert run(world.working(body)).text == "x"
    assert calls == ["first body started", "first body exited", "second body ran"]


def test_a_retry_handed_back_to_a_worker_serving_a_shard_waits_for_its_abandoned_body_to_exit():
    release = threading.Event()
    calls = []

    def stubborn_then_fine(request: Greeting) -> Greeting:
        if not calls:
            calls.append("first body started")
            release.wait(5)
            calls.append("first body exited")
            return request
        calls.append("second body ran")
        return request

    world = World(stubborn_then_fine, retries=1, timeout=SOFT, cancel_grace=GRACE)
    # Runs the shard's leader hands it, which settle no handle here.
    world.runtime.delivers_results = False

    async def body():
        world.call("stubborn_then_fine", Greeting(text="x"))
        await until(lambda: any(event[0] == "fail" for event in world.runtime.events), "a fail event")
        await asyncio.sleep(0.2)
        assert len(calls) == 1, "no second body while the abandoned one runs"
        release.set()
        await until(lambda: len(calls) == 3, "the retry to run")

    run(world.working(body))
    assert calls == ["first body started", "first body exited", "second body ran"]


def test_what_an_abandoned_body_later_returns_or_raises_is_discarded(caplog):
    release = threading.Event()

    def raises_late(request: Greeting) -> Greeting:
        release.wait(5)
        raise ValueError("too late to matter")

    world = World(raises_late, timeout=SOFT, cancel_grace=GRACE)

    async def body():
        with pytest.raises(TaskTimeoutError):
            await world.call("raises_late", Greeting())
        release.set()
        await asyncio.wait_for(world.session.wait_until_running_finish(), WAIT)

    run(world.working(body))

    assert not any(event[0] == "complete" for event in world.runtime.events)
    assert "never retrieved" not in caplog.text


def test_cancel_grace_comes_from_the_process_unless_the_task_chose_its_own():
    def stubborn_until_released():
        release = asyncio.Event()

        async def stubborn(request: Greeting) -> Greeting:
            while True:
                try:
                    await release.wait()
                    return request
                except asyncio.CancelledError:
                    continue

        return stubborn, release

    async def elapsed_until_timeout(task_options, process_grace):
        function, release = stubborn_until_released()
        world = World(function, timeout=SOFT, **task_options)
        world.configuration.configure(cancel_grace=process_grace)

        async def body():
            started = time.monotonic()
            with pytest.raises(TaskTimeoutError):
                await world.call("stubborn", Greeting())
            elapsed = time.monotonic() - started
            release.set()
            return elapsed

        return await world.working(body)

    quick = run(elapsed_until_timeout({}, timedelta(milliseconds=50)))
    slow = run(
        elapsed_until_timeout(
            {"cancel_grace": timedelta(milliseconds=250)}, timedelta(milliseconds=50)
        )
    )

    assert quick < 0.2 <= slow


def test_a_running_task_that_fails_after_stopping_does_not_wait_for_a_retry_nobody_will_run():
    release = threading.Event()
    calls = []

    def fails_once_released(request: Greeting) -> Greeting:
        calls.append(True)
        release.wait(5)
        raise ValueError("failed while the run was stopping")

    world = World(fails_once_released, retries=2)

    async def body():
        handle = world.call("fails_once_released", Greeting())
        while not calls:
            await asyncio.sleep(0.001)
        world.session.stop_claiming()
        release.set()
        with pytest.raises(RunStoppedError):
            await asyncio.wait_for(handle, WAIT)
        await asyncio.wait_for(world.session.wait_until_running_finish(), WAIT)

    run(world.working(body))

    assert len(calls) == 1


def test_a_pending_task_can_be_cancelled_and_its_handle_says_so():
    world = World(echo)
    world.runtime.hold_claims = True

    async def body():
        await world.call("echo", Greeting())
        waiting = world.call("echo", Greeting(text="never claimed"))
        await asyncio.sleep(0.02)
        assert waiting.cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(waiting, WAIT)
        await asyncio.wait_for(world.session.wait_until_idle(), WAIT)

    run(with_events(world, body))

    assert world.session.tasks.is_empty()


def test_a_running_body_is_cancelled_and_nothing_it_does_is_reported():
    saw = []

    async def long(request: Greeting) -> Greeting:
        saw.append("started")
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            saw.append("cancelled")
            raise
        return request

    world = World(long)

    async def body():
        handle = world.call("long", Greeting())
        while not saw:
            await asyncio.sleep(0.001)
        assert handle.cancel() is True
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)
        await asyncio.wait_for(world.session.wait_until_running_finish(), WAIT)

    run(with_events(world, body))

    assert saw == ["started", "cancelled"]
    assert not any(event[0] in ("complete", "fail") for event in world.runtime.events)
    assert world.session.tasks.is_empty()


def test_a_cancelled_body_that_ignores_cancellation_still_certifies_nothing():
    release = threading.Event()

    def stubborn(request: Greeting) -> Greeting:
        release.wait(5)
        return request

    world = World(stubborn)

    async def body():
        handle = world.call("stubborn", Greeting())
        await until(lambda: any(event[0] == "started" for event in world.runtime.events), "a started event")
        handle.cancel()
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)
        release.set()
        await asyncio.wait_for(world.session.wait_until_running_finish(), WAIT)

    run(with_events(world, body))

    assert not any(event[0] == "complete" for event in world.runtime.events)


def test_cancelling_a_task_that_already_finished_changes_nothing():
    world = World(echo)

    async def body():
        handle = world.call("echo", Greeting(text="done"))
        result = await handle
        assert handle.cancel() is False
        return await handle, result

    again, result = run(with_events(world, body))

    assert again == result == Greeting(text="done")


def test_a_task_cancelled_after_it_is_claimed_but_before_it_is_reported_started_says_cancelled():
    world = World(echo)
    handles = []
    # The cancel lands after the worker claimed the run and before it told the
    # leader the run started, which the leader then refuses.

    def cancel_before_started(run_id):
        # The fix depends on the leader answering "cancelled" here.
        assert handles[0].cancel() is True

    world.runtime.before_started = cancel_before_started

    async def body():
        handles.append(world.call("echo", Greeting(text="hello")))
        with pytest.raises(TaskCancelledError):
            await handles[0]

    run(with_events(world, body))


def test_a_task_can_be_cancelled_from_another_thread():
    world = World(echo)
    world.runtime.hold_claims = True
    outcome = []

    async def body():
        await world.call("echo", Greeting())
        waiting = world.call("echo", Greeting())
        await asyncio.sleep(0.02)
        thread = threading.Thread(target=lambda: outcome.append(waiting.cancel()))
        thread.start()
        thread.join()
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(waiting, WAIT)

    run(with_events(world, body))

    assert outcome == [True]


def test_a_cancelled_task_is_not_retried():
    release = threading.Event()
    calls = []

    def fails_when_released(request: Greeting) -> Greeting:
        calls.append(True)
        release.wait(5)
        raise ValueError("failed after being cancelled")

    world = World(fails_when_released, retries=3)

    async def body():
        handle = world.call("fails_when_released", Greeting())
        while not calls:
            await asyncio.sleep(0.001)
        handle.cancel()
        while not handle.done():
            # The leader's cancellation has reached the session, so the body is
            # released into a run that is already known to be cancelled.
            await asyncio.sleep(0.001)
        release.set()
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)
        await asyncio.wait_for(world.session.wait_until_running_finish(), WAIT)

    run(with_events(world, body))

    assert len(calls) == 1


def test_an_async_body_that_swallows_the_cancellation_and_returns_certifies_nothing():
    swallowed = []
    running = []

    async def swallows(request: Greeting) -> Greeting:
        running.append(True)
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            swallowed.append(True)
        return request

    world = World(swallows)

    async def body():
        handle = world.call("swallows", Greeting())
        await until(lambda: running, "the task running")
        handle.cancel()
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)
        await asyncio.wait_for(world.session.wait_until_running_finish(), WAIT)

    run(with_events(world, body))

    assert swallowed == [True]
    assert not any(event[0] == "complete" for event in world.runtime.events)


def test_a_callback_runs_after_the_leader_has_certified_the_result():
    world = World(echo)
    order = []
    world.runtime.on_complete = lambda run_id: order.append("certified")

    async def body():
        handle = world.call("echo", Greeting(text="hi"))
        handle.callback(lambda result: order.append(("callback", result.text)))
        await handle
        await world.session.wait_until_idle()

    run(world.working(body))

    assert order == ["certified", ("callback", "hi")]


def test_synchronous_and_async_callbacks_both_run_and_a_synchronous_one_may_block():
    world = World(echo)
    seen = []
    threads = []

    def blocking(result):
        threads.append(threading.get_ident())
        time.sleep(0.01)
        seen.append("sync")

    async def asynchronous(result):
        await asyncio.sleep(0.01)
        seen.append("async")

    async def body():
        loop_thread = threading.get_ident()
        handle = world.call("echo", Greeting())
        handle.callback(blocking).callback(asynchronous)
        await handle
        await world.session.wait_until_idle()
        return loop_thread

    loop_thread = run(world.working(body))

    assert sorted(seen) == ["async", "sync"]
    assert threads and threads[0] != loop_thread


def test_a_callback_that_fails_is_logged_by_type_only_changes_nothing_and_stops_no_other(caplog):
    world = World(echo)
    after = []

    def broken(result):
        raise KeyError(f"leaks {result.text}")

    async def body():
        handle = world.call("echo", Greeting(text="private"))
        handle.callback(broken).callback(lambda result: after.append(result.text))
        result = await handle
        await world.session.wait_until_idle()
        return result

    assert run(world.working(body)).text == "private"
    assert after == ["private"]
    assert "private" not in caplog.text
    assert "KeyError" in caplog.text


def test_a_callback_is_kept_alive_by_the_run_even_if_the_handle_is_dropped():
    world = World(echo)
    seen = []

    async def body():
        world.call("echo", Greeting(text="unwatched")).callback(lambda r: seen.append(r.text))
        gc.collect()
        await world.session.wait_until_idle()

    run(world.working(body))

    assert seen == ["unwatched"]


def test_finishing_waits_for_a_callback_that_is_still_running():
    world = World(echo)
    finished = []

    async def slow_callback(result):
        await asyncio.sleep(0.05)
        finished.append(True)

    async def body():
        world.call("echo", Greeting()).callback(slow_callback)
        await world.session.wait_until_idle()

    run(world.working(body))

    assert finished == [True]


def test_a_callback_is_not_called_for_a_task_that_failed_expired_or_was_cancelled():
    world = World(echo, explode)
    world.runtime.hold_claims = True
    seen = []

    async def body():
        failing = world.call("explode", Greeting(text="x")).callback(seen.append)
        with pytest.raises(ValueError):
            await failing
        await asyncio.sleep(0.02)
        cancelled = world.call("echo", Greeting()).callback(seen.append)
        cancelled.cancel()
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(cancelled, WAIT)
        await world.session.wait_until_idle()

    run(with_events(world, body))

    assert seen == []


def concatenate(older: Greeting, newer: Greeting) -> Greeting:
    return Greeting(text=older.text + newer.text)


def coalescing_world(function=echo, **options):
    return World(function, kind=TaskKind.COALESCING, **options)


async def generations(world, name, *texts):
    """Submits one generation of the default coalescing key per text, without
    giving the worker a chance to claim any of them in between, and awaits the
    supersession of all but the newest. Returns the newest generation's handle."""
    handles = [world.call(name, Greeting(text=text)) for text in texts]
    for older in handles[:-1]:
        with pytest.raises(TaskSupersededError):
            await asyncio.wait_for(older, WAIT)
    return handles[-1]


def test_the_worker_folds_the_superseded_payloads_oldest_first_before_running_the_task():
    world = coalescing_world(merge=concatenate)

    async def body():
        return await (await generations(world, "echo", "a", "b", "c"))

    assert run(with_events(world, body)).text == "abc"


def test_a_coalescing_task_declared_with_drop_oldest_drops_its_oldest_payloads_to_fit():
    # Each Greeting encodes to 2 bytes plus its text, so the texts below are
    # 61, 62, 63 and then 50 bytes: the fourth only fits once the oldest
    # retained payload (61) is dropped, 186 + 50 > 200 >= 175.
    world = World(
        echo, kind=TaskKind.COALESCING, merge=concatenate, drop_oldest=True,
        memory_soft_limit=100, memory_hard_limit=200,
    )

    async def body():
        newest = await generations(world, "echo", "a" * 59, "b" * 60, "c" * 61, "d" * 48)
        return await asyncio.wait_for(newest, WAIT)

    assert run(with_events(world, body)).text == "b" * 60 + "c" * 61 + "d" * 48


def test_without_a_reducer_the_newest_payload_wins():
    world = coalescing_world()

    async def body():
        return await (await generations(world, "echo", "a", "b", "c"))

    assert run(with_events(world, body)).text == "c"


def test_a_generation_that_absorbed_nothing_runs_on_its_own_input_and_needs_no_reducer_call():
    calls = []

    def recording(older: Greeting, newer: Greeting) -> Greeting:
        calls.append((older.text, newer.text))
        return newer

    world = coalescing_world(merge=recording)

    async def body():
        return await world.call("echo", Greeting(text="alone"))

    assert run(world.working(body)).text == "alone"
    assert calls == []


def test_a_reducer_that_raises_fails_the_task_and_the_body_never_runs():
    ran = []

    def body_(request: Greeting) -> Greeting:
        ran.append(True)
        return request

    def broken(older: Greeting, newer: Greeting) -> Greeting:
        raise ValueError("cannot merge")

    world = coalescing_world(body_, merge=broken)

    async def body():
        newest = await generations(world, "body_", "a", "b")
        with pytest.raises(ValueError, match="cannot merge"):
            await asyncio.wait_for(newest, WAIT)

    run(with_events(world, body))

    assert ran == []
    assert world.runtime.events[-1][:1] == ("fail",)


def test_a_superseded_generation_fails_its_handle_and_says_by_which():
    world = coalescing_world()
    world.runtime.hold_claims = True

    async def body():
        # The first generation takes the only claim; of the two that follow,
        # the newer supersedes the older while neither can be claimed.
        await world.call("echo", Greeting())
        older = world.call("echo", Greeting(text="older"))
        world.call("echo", Greeting(text="newer"))
        with pytest.raises(TaskSupersededError) as raised:
            await asyncio.wait_for(older, WAIT)
        return raised.value, world.runtime.submitted[2][0]

    error, newer_id = run(with_events(world, body))

    assert error.superseded_by == newer_id


def test_a_task_settled_while_its_run_is_starting_can_still_have_its_body_stopped():
    stopped = []
    started = asyncio.Event()

    async def long(request: Greeting) -> Greeting:
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            stopped.append(True)
            raise
        return request

    world = World(long)
    handles = []
    # Settled by another route after the claim, before the body starts.
    world.runtime.before_started = lambda _run_id: world.runtime.inject_event(
        EventKind.EXPIRED, task_id=handles[0].task_id
    )

    async def body():
        handles.append(world.call("long", Greeting()))
        with pytest.raises(TaskExpiredError):
            await asyncio.wait_for(handles[0], WAIT)
        await asyncio.wait_for(started.wait(), WAIT)
        world.runtime.inject_event(EventKind.CANCELLED, task_id=handles[0].task_id)
        await asyncio.wait_for(world.session.wait_until_running_finish(), WAIT)
        # Settled once: the later cancel notice changed nothing for the handle.
        with pytest.raises(TaskExpiredError):
            await asyncio.wait_for(handles[0], WAIT)

    run(with_events(world, body))

    assert stopped == [True]
    assert world.session.tasks.is_empty()


def test_a_cancel_this_process_asked_for_suppresses_the_runs_outcome_before_the_leaders_notice():
    release = threading.Event()

    def stubborn(request: Greeting) -> Greeting:
        release.wait(5)
        return request

    world = World(stubborn)

    async def body():
        handle = world.call("stubborn", Greeting())
        while not any(event[0] == "started" for event in world.runtime.events):
            await asyncio.sleep(0.001)
        assert handle.cancel() is True
        release.set()
        await asyncio.wait_for(world.session.wait_until_running_finish(), WAIT)
        return handle.done()

    settled = run(world.working(body))

    assert not any(event[0] in ("complete", "fail") for event in world.runtime.events)
    assert settled is False  # only the leader's notice settles a cancelled task


def test_a_stale_run_that_ends_after_its_retry_began_leaves_the_retry_remembered():
    """An abandoned run and its retry both wait for the abandoned body to
    exit, and the stale run ends first: it must not end the retry's run."""
    world = World(echo)
    tasks = world.session.tasks

    async def body():
        handle = world.call("echo", Greeting())
        stale = tasks.claimed(handle.task_id)
        tasks.retry_queued(handle.task_id)
        retry = tasks.claimed(handle.task_id)
        tasks.run_ended(stale)
        tasks.cancelled_by_leader(handle.task_id)
        with pytest.raises(TaskCancelledError):
            await asyncio.wait_for(handle, WAIT)
        assert retry.cancelled_by_leader
        assert not tasks.is_empty()  # still remembered while the retry runs
        tasks.run_ended(retry)
        assert tasks.is_empty()

    run(body())


def test_stopping_while_a_submission_is_in_flight_refuses_or_fails_it_and_never_leaves_it_open():
    world = World(echo)
    world.runtime.submit_lingers_for = 0.1  # the native call holds the table lock this long
    outcome = []

    def submit():
        try:
            outcome.append(world.call("echo", Greeting()))
        except RunStoppedError as refused:
            outcome.append(refused)

    async def body():
        submitter = threading.Thread(target=submit)
        submitter.start()
        await asyncio.sleep(0.02)  # usually inside the native call by now
        world.session.stop_claiming()
        while submitter.is_alive():
            await asyncio.sleep(0.005)
        [result] = outcome
        if not isinstance(result, RunStoppedError):  # submitted first, so the stop failed it
            with pytest.raises(RunStoppedError):
                await asyncio.wait_for(result, WAIT)

    run(body())

    assert world.session.tasks.is_empty()


def test_a_place_given_back_from_another_thread_wakes_the_worker_loop_on_its_own_thread():
    places = ConcurrencyPlaces(1)
    errors = []

    def give_back():
        try:
            places.blocked()
        except BaseException as error:  # a debug loop refuses a foreign-thread call
            errors.append(error)

    async def body():
        holder = asyncio.ensure_future(asyncio.Event().wait())
        places.occupy(holder)
        assert places.free() == 0
        waiting = asyncio.ensure_future(places.wait_for_free())
        await asyncio.sleep(0)  # the worker loop is waiting for a place now
        thread = threading.Thread(target=give_back)
        thread.start()
        thread.join()
        await asyncio.wait_for(waiting, WAIT)
        free = places.free()
        holder.cancel()
        return free

    free = asyncio.run(body(), debug=True)

    assert errors == []
    assert free == 1


def test_stopping_ends_a_worker_loop_that_is_waiting_for_a_free_place():
    started = asyncio.Event()
    release = asyncio.Event()

    async def hold(request: Greeting) -> Greeting:
        started.set()
        await release.wait()
        return request

    world = World(hold, concurrency=1)

    async def body():
        await asyncio.wait_for(world.native.wait_until_leader(), WAIT)
        worker = asyncio.ensure_future(world.session.work())
        handle = world.call("hold", Greeting())
        await asyncio.wait_for(started.wait(), WAIT)  # the only place is taken
        world.session.stop_claiming()
        await asyncio.wait_for(worker, WAIT)  # woke, saw the stop, and returned
        release.set()
        return await asyncio.wait_for(handle, WAIT)

    assert run(body()).text == ""


def test_a_callback_added_after_the_run_loop_closed_runs_at_once_and_leaves_nothing_to_wait_for(
    recwarn, caplog
):
    world = World(echo)
    called = []

    def broken(result):
        raise KeyError(f"leaks {result.text}")

    async def broken_later(result):
        raise ValueError(f"leaks {result.text}")

    async def body():
        handle = world.call("echo", Greeting(text="hush"))
        await handle
        return handle

    handle = run(world.working(body))  # the run's loop is closed once this returns
    handle.callback(broken).callback(lambda result: called.append(result.text)).callback(broken_later)

    assert called == ["hush"]  # run inline: there is no loop left to host it
    assert "hush" not in caplog.text
    assert "KeyError" in caplog.text and "ValueError" in caplog.text
    # A refused callback must not stay counted, or this never returns.
    run(asyncio.wait_for(world.session.wait_until_idle(), WAIT))
    gc.collect()
    assert not [w for w in recwarn if "was never awaited" in str(w.message)]


def quick_reconnect(request: Greeting) -> Greeting:
    return request


def queue_reconnect(request: Greeting) -> Greeting:
    return request


def tiny_reconnect(request: Greeting) -> Greeting:
    return request


def test_a_run_carries_its_tasks_reconnect_timeout_or_else_its_queues():
    world = World()
    timeouts = {"slow": 90, "tiny": 1e-9}
    world.configuration.configure(reconnect_timeouts=timeouts)
    # Changing the mapping after configuring it changes nothing, even once
    # another setting is configured.
    timeouts["slow"] = 1
    world.configuration.configure(result_ttl=60)
    own = Task(
        quick_reconnect,
        registry=world.registry,
        serializers=world.serializers,
        name="tests.quick_reconnect",
        queue="slow",
        reconnect_timeout=timedelta(seconds=5),
    )
    from_queue = Task(
        queue_reconnect,
        registry=world.registry,
        serializers=world.serializers,
        name="tests.queue_reconnect",
        queue="slow",
    )
    # Under a millisecond, which still waits at least one.
    tiny = Task(
        tiny_reconnect,
        registry=world.registry,
        serializers=world.serializers,
        name="tests.tiny_reconnect",
        queue="tiny",
    )
    for task in (own, from_queue, tiny):
        world.session.submit(task.definition, Greeting(text="hi"))

    async def claimed():
        await asyncio.wait_for(world.native.wait_until_leader(), WAIT)
        return await asyncio.wait_for(world.native.claim_pending(10), WAIT)

    claims = run(claimed())
    assert {claim.definition_id: claim.reconnect_timeout_ms for claim in claims} == {
        "tests.quick_reconnect": 5_000,
        "tests.queue_reconnect": 90_000,
        "tests.tiny_reconnect": 1,
    }
