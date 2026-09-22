"""The faulting adapter over the native runtime: it must forward every call,
record what the session asked, and only misbehave when told to."""

import asyncio

import pytest

from kabudachi import _native
from kabudachi.errors import BackpressureError
from faulting_runtime import FaultingNative, FaultingRuntime

WAIT = 5


def new_runtime(**options):
    native = _native.NativeRuntime("worker-1", "incarnation-1", **options)
    return FaultingRuntime(native)


@pytest.fixture
def runtime():
    faulting = new_runtime()
    yield faulting
    faulting.shutdown()


def run(coroutine):
    return asyncio.run(asyncio.wait_for(coroutine, WAIT))


def test_a_submission_is_forwarded_and_recorded(runtime):
    task_id = runtime.submit("billing.charge", 3, b"input", "default", retries=2)

    assert runtime.submitted == [(task_id, "billing.charge", 3, b"input", "default")]
    assert runtime.submit_options == [
        {"delay_ms": None, "expires_in_ms": None, "coalescing_key": None}
    ]
    assert runtime.retries == {task_id: 2}

    async def claim():
        return await runtime.claim_pending(10)

    [claimed] = run(claim())
    assert claimed.task_id == task_id


def test_a_rejected_submission_is_not_recorded():
    runtime = new_runtime(memory_soft_limit=1, memory_hard_limit=4)
    try:
        with pytest.raises(BackpressureError):
            runtime.submit("billing.charge", 3, b"too large for the limit", "default")
        assert runtime.submitted == []
    finally:
        runtime.shutdown()


def test_the_lifecycle_calls_are_forwarded_and_recorded(runtime):
    runtime.submit("billing.charge", 3, b"input", "default")

    async def scenario():
        await runtime.wait_until_leader()
        [claim] = await runtime.claim_pending(10)
        runtime.report_started(claim.task_run_id)
        certification = runtime.complete(claim.task_run_id, b"digest")
        return claim, certification

    claim, certification = run(scenario())

    assert certification.task_id == claim.task_id
    assert certification.result_digest == b"digest"
    assert runtime.events == [("started", claim.task_run_id), ("complete", claim.task_run_id)]


def test_an_altered_digest_is_what_the_caller_is_told_was_certified(runtime):
    runtime.altered_digest = b"other"
    runtime.submit("billing.charge", 3, b"input", "default")

    async def scenario():
        await runtime.wait_until_leader()
        [claim] = await runtime.claim_pending(10)
        runtime.report_started(claim.task_run_id)
        return runtime.complete(claim.task_run_id, b"digest")

    assert run(scenario()).result_digest == b"other"


def test_a_refused_completion_never_reaches_the_native_runtime(runtime):
    runtime.refuse_completion = True
    runtime.submit("billing.charge", 3, b"input", "default")

    async def scenario():
        await runtime.wait_until_leader()
        [claim] = await runtime.claim_pending(10)
        runtime.report_started(claim.task_run_id)
        with pytest.raises(RuntimeError):
            runtime.complete(claim.task_run_id, b"digest")
        return claim

    claim = run(scenario())

    assert runtime.task_run_state(claim.task_run_id) == "Running"


def test_holding_claims_hands_out_the_first_batch_only(runtime):
    runtime.hold_claims = True
    runtime.submit("billing.charge", 3, b"one", "default")

    async def scenario():
        await runtime.wait_until_leader()
        first = await runtime.claim_pending(10)
        runtime.submit("billing.charge", 3, b"two", "default")
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(runtime.claim_pending(10), 0.1)
        return first

    assert len(run(scenario())) == 1


def test_a_claim_or_event_error_is_raised_instead_of_forwarding(runtime):
    runtime.claim_error = ValueError("claim")
    runtime.event_error = ValueError("events")

    async def scenario():
        with pytest.raises(ValueError, match="claim"):
            await runtime.claim_pending(10)
        with pytest.raises(ValueError, match="events"):
            await runtime.next_events()

    run(scenario())


def test_an_injected_event_is_delivered_by_next_events(runtime):
    async def scenario():
        runtime.inject_event("expired", task_id="task-that-is-not-ours")
        return await runtime.next_events()

    [event] = run(scenario())

    assert (event.kind, event.task_id, event.was_running, event.superseded_by) == (
        "expired",
        "task-that-is-not-ours",
        False,
        None,
    )


def test_a_native_event_is_not_lost_when_an_injected_one_arrives_first(runtime):
    async def scenario():
        await runtime.wait_until_leader()
        # Never claimed, so the real scheduler expires it.
        runtime.submit("billing.charge", 3, b"input", "default", expires_in_ms=50)
        waiting = asyncio.ensure_future(runtime.next_events())
        await asyncio.sleep(0)
        pending_native = runtime._native_events
        runtime.inject_event("slow_down")
        seen = [event.kind for event in await waiting]
        # The injected event won the race, but the native call it left behind must
        # go on running: cancelling it would lose whatever it had already taken.
        assert not pending_native.cancelled()
        while "expired" not in seen:
            seen += [event.kind for event in await runtime.next_events()]
        return seen

    seen = run(scenario())

    assert "slow_down" in seen and "expired" in seen


def test_the_faulting_native_spies_on_construction_leadership_and_shutdown():
    FaultingNative.instances = []
    native = FaultingNative("worker-9", "incarnation-9", worker_threads=1)

    assert FaultingNative.instances == [native]
    assert native.options == {"worker_threads": 1}

    async def scenario():
        await native.wait_until_leader()
        native.leader_error = RuntimeError("no leader")
        with pytest.raises(RuntimeError, match="no leader"):
            await native.wait_until_leader()

    run(scenario())
    native.shutdown()
    assert native.shutdowns == 1


def test_before_started_runs_after_the_call_is_recorded_and_before_it_is_forwarded(runtime):
    seen = []
    runtime.submit("billing.charge", 3, b"input", "default")

    def before(task_run_id):
        seen.append((task_run_id, list(runtime.events), runtime.task_run_state(task_run_id)))

    runtime.before_started = before

    async def scenario():
        await runtime.wait_until_leader()
        [claim] = await runtime.claim_pending(10)
        runtime.report_started(claim.task_run_id)
        return claim

    claim = run(scenario())

    assert seen == [(claim.task_run_id, [("started", claim.task_run_id)], "Claimed")]
    assert runtime.task_run_state(claim.task_run_id) == "Running"
