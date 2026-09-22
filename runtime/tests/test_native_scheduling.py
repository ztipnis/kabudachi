"""Submitting, claiming, running and certifying tasks through the native
runtime, without any of the Python task API on top."""

import asyncio
import time

import pytest

from kabudachi import _native

WAIT_LIMIT_SECONDS = 5
DIGEST = b"digest-of-the-result"


def new_runtime(**options):
    return _native.NativeRuntime("worker-1", "incarnation-1", **options)


@pytest.fixture
def runtime():
    native = new_runtime()
    yield native
    native.shutdown()


def submit(native, payload=b"input-bytes", queue="default"):
    return native.submit("billing.charge", 3, payload, queue)


async def leader(native):
    await asyncio.wait_for(native.wait_until_leader(), WAIT_LIMIT_SECONDS)


async def claim(native, limit=10):
    return await asyncio.wait_for(native.claim_pending(limit), WAIT_LIMIT_SECONDS)


def test_a_submitted_task_can_be_claimed(runtime):
    task_id = submit(runtime)

    claims = asyncio.run(claim(runtime))

    assert len(claims) == 1
    [only] = claims
    assert only.task_id == task_id
    assert only.definition_id == "billing.charge"
    assert only.source_version == 3
    assert only.serialized_input == b"input-bytes"
    assert only.queue == "default"
    assert only.task_run_id


def test_a_claim_waits_until_there_is_work(runtime):
    async def main():
        pending = asyncio.ensure_future(runtime.claim_pending(10))
        await asyncio.sleep(0.1)
        assert not pending.done()

        task_id = submit(runtime)
        [claimed] = await asyncio.wait_for(pending, WAIT_LIMIT_SECONDS)
        return task_id, claimed.task_id

    submitted, claimed = asyncio.run(main())

    assert claimed == submitted


def test_a_claim_takes_no_more_than_the_limit_and_keeps_the_order(runtime):
    submitted = [submit(runtime, payload=str(n).encode()) for n in range(5)]

    async def main():
        first = await claim(runtime, limit=2)
        rest = await claim(runtime, limit=10)
        return first, rest

    first, rest = asyncio.run(main())

    assert [c.task_id for c in first] == submitted[:2]
    assert [c.task_id for c in rest] == submitted[2:]


def test_a_task_is_claimed_only_once(runtime):
    submit(runtime)

    async def main():
        first = await claim(runtime)
        second = asyncio.ensure_future(runtime.claim_pending(10))
        await asyncio.sleep(0.1)
        still_waiting = not second.done()
        second.cancel()
        return first, still_waiting

    first, still_waiting = asyncio.run(main())

    assert len(first) == 1
    assert still_waiting


def test_a_claimed_task_can_be_started_and_completed(runtime):
    submit(runtime)
    [claimed] = asyncio.run(claim(runtime))

    runtime.report_started(claimed.task_run_id)
    assert runtime.task_run_state(claimed.task_run_id) == "Running"
    certification = runtime.complete(claimed.task_run_id, DIGEST)

    assert certification.task_id == claimed.task_id
    assert certification.task_run_id == claimed.task_run_id
    assert certification.result_digest == DIGEST
    assert runtime.task_run_state(claimed.task_run_id) == "Succeeded"


def test_a_claimed_task_is_in_the_claimed_state(runtime):
    submit(runtime)

    [claimed] = asyncio.run(claim(runtime))

    assert runtime.task_run_state(claimed.task_run_id) == "Claimed"


def test_a_run_that_never_started_cannot_be_completed(runtime):
    submit(runtime)
    [claimed] = asyncio.run(claim(runtime))

    with pytest.raises(RuntimeError, match="not in the expected state"):
        runtime.complete(claimed.task_run_id, DIGEST)

    assert runtime.task_run_state(claimed.task_run_id) == "Claimed"


def test_a_run_cannot_be_completed_twice(runtime):
    submit(runtime)
    [claimed] = asyncio.run(claim(runtime))
    runtime.report_started(claimed.task_run_id)
    runtime.complete(claimed.task_run_id, DIGEST)

    with pytest.raises(RuntimeError, match="not in the expected state"):
        runtime.complete(claimed.task_run_id, b"different")


def test_an_unknown_run_is_refused(runtime):
    asyncio.run(leader(runtime))

    with pytest.raises(RuntimeError, match="no such run"):
        runtime.report_started("no-such-run")
    with pytest.raises(RuntimeError, match="no such run"):
        runtime.complete("no-such-run", DIGEST)
    assert runtime.task_run_state("no-such-run") is None


def test_shutdown_fails_a_claim_that_is_waiting_for_work():
    native = new_runtime()

    async def main():
        pending = asyncio.ensure_future(native.claim_pending(10))
        await asyncio.sleep(0.05)
        native.shutdown()
        with pytest.raises(RuntimeError, match="shut down"):
            await asyncio.wait_for(pending, WAIT_LIMIT_SECONDS)

    asyncio.run(main())


def test_submitting_after_shutdown_is_refused():
    native = new_runtime()
    native.shutdown()

    with pytest.raises(RuntimeError, match="shut down"):
        submit(native)


def test_claiming_after_shutdown_is_refused():
    native = new_runtime()
    native.shutdown()

    async def main():
        native.claim_pending(10)

    with pytest.raises(RuntimeError, match="shut down"):
        asyncio.run(main())


def test_a_claim_limit_of_zero_is_refused(runtime):
    async def main():
        runtime.claim_pending(0)

    with pytest.raises(ValueError, match="limit"):
        asyncio.run(main())


def test_a_claim_waits_for_leadership_before_handing_out_work():
    native = new_runtime(election_tick_ms=60_000)
    try:
        submit(native)

        async def main():
            pending = asyncio.ensure_future(native.claim_pending(10))
            await asyncio.sleep(0.2)
            return pending.done()

        assert asyncio.run(main()) is False
    finally:
        native.shutdown()


def test_many_tasks_are_all_claimed_exactly_once(runtime):
    submitted = {submit(runtime, payload=str(n).encode()) for n in range(200)}

    async def main():
        seen = []
        while len(seen) < 200:
            seen.extend(c.task_id for c in await claim(runtime, limit=7))
        return seen

    seen = asyncio.run(main())

    assert len(seen) == 200
    assert set(seen) == submitted


def test_submitting_from_another_thread_wakes_a_waiting_claim(runtime):
    import threading

    async def main():
        pending = asyncio.ensure_future(runtime.claim_pending(10))
        await asyncio.sleep(0.05)
        threading.Thread(target=lambda: submit(runtime)).start()
        return await asyncio.wait_for(pending, WAIT_LIMIT_SECONDS)

    assert len(asyncio.run(main())) == 1


def test_a_report_before_leadership_is_refused():
    native = new_runtime(election_tick_ms=60_000)
    try:
        with pytest.raises(RuntimeError, match="not the leader"):
            native.report_started("any-run")
        with pytest.raises(RuntimeError, match="not the leader"):
            native.complete("any-run", DIGEST)
    finally:
        native.shutdown()


def test_a_claim_carries_the_queue_and_version_it_was_submitted_with(runtime):
    runtime.submit("reports.render", 7, b"payload", "gpu")

    [claimed] = asyncio.run(claim(runtime))

    assert claimed.definition_id == "reports.render"
    assert claimed.source_version == 7
    assert claimed.queue == "gpu"
    assert claimed.serialized_input == b"payload"


def test_a_run_cannot_be_certified_after_shutdown():
    native = new_runtime()
    submit(native)
    [claimed] = asyncio.run(claim(native))
    native.report_started(claimed.task_run_id)

    native.shutdown()

    with pytest.raises(RuntimeError, match="not the leader"):
        native.complete(claimed.task_run_id, DIGEST)


def test_concurrent_claims_never_hand_out_the_same_task_twice(runtime):
    submitted = {submit(runtime, payload=str(n).encode()) for n in range(300)}
    claimed = []

    async def worker_loop():
        while True:
            batch = await runtime.claim_pending(5)
            claimed.extend(c.task_id for c in batch)

    async def main():
        workers = [asyncio.ensure_future(worker_loop()) for _ in range(6)]
        deadline = time.monotonic() + WAIT_LIMIT_SECONDS
        while len(claimed) < 300 and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    asyncio.run(main())

    assert len(claimed) == 300
    assert set(claimed) == submitted


def started_run(native):
    submit(native)
    [claimed] = asyncio.run(claim(native))
    native.report_started(claimed.task_run_id)
    return claimed.task_run_id


def test_a_running_task_can_be_reported_failed(runtime):
    run_id = started_run(runtime)

    runtime.fail(run_id, "ValueError")

    assert runtime.task_run_state(run_id) == "Failed"


def test_a_run_that_never_started_cannot_be_reported_failed(runtime):
    submit(runtime)
    [claimed] = asyncio.run(claim(runtime))

    with pytest.raises(RuntimeError):
        runtime.fail(claimed.task_run_id, "ValueError")


def test_a_failed_run_cannot_then_be_completed(runtime):
    run_id = started_run(runtime)
    runtime.fail(run_id, "ValueError")

    with pytest.raises(RuntimeError):
        runtime.complete(run_id, DIGEST)


def test_a_finished_task_is_forgotten_once_the_result_ttl_has_passed():
    native = new_runtime(result_ttl_ms=50)
    try:
        run_id = started_run(native)
        native.complete(run_id, DIGEST)
        assert native.task_run_state(run_id) == "Succeeded"

        # Nothing uses the scheduler now, so it is the timer that forgets it.
        deadline = time.monotonic() + 3
        while native.task_run_state(run_id) is not None and time.monotonic() < deadline:
            time.sleep(0.02)

        assert native.task_run_state(run_id) is None
    finally:
        native.shutdown()


def test_finished_tasks_are_kept_by_default():
    native = new_runtime()
    try:
        run_id = started_run(native)
        native.complete(run_id, DIGEST)

        time.sleep(0.1)
        submit(native)

        assert native.task_run_state(run_id) == "Succeeded"
    finally:
        native.shutdown()


def test_a_failed_run_with_retries_left_is_claimed_again_as_the_next_attempt(runtime):
    task_id = runtime.submit("billing.charge", 3, b"input", "default", retries=2)

    async def main():
        attempts = []
        for _ in range(3):
            [claimed] = await claim(runtime)
            runtime.report_started(claimed.task_run_id)
            attempts.append((claimed.task_id, claimed.attempt_number))
            will_retry = runtime.fail(claimed.task_run_id, "ValueError")
            assert will_retry == (len(attempts) < 3)
        return attempts

    attempts = asyncio.run(main())

    assert attempts == [(task_id, 1), (task_id, 2), (task_id, 3)]


def test_a_task_without_retries_is_not_retried(runtime):
    run_id = started_run(runtime)

    assert runtime.fail(run_id, "ValueError") is False


def test_a_retry_that_succeeds_is_certified(runtime):
    runtime.submit("billing.charge", 3, b"input", "default", retries=1)

    async def main():
        [first] = await claim(runtime)
        runtime.report_started(first.task_run_id)
        runtime.fail(first.task_run_id, "ValueError")
        [second] = await claim(runtime)
        runtime.report_started(second.task_run_id)
        return first, second, runtime.complete(second.task_run_id, DIGEST)

    first, second, certification = asyncio.run(main())

    assert certification.task_run_id == second.task_run_id != first.task_run_id
    assert runtime.task_run_state(first.task_run_id) == "Failed"
    assert runtime.task_run_state(second.task_run_id) == "Succeeded"


async def next_events(native, timeout=WAIT_LIMIT_SECONDS):
    return await asyncio.wait_for(native.next_events(), timeout)


def test_a_delayed_task_is_not_claimed_before_it_is_due(runtime):
    started = time.monotonic()
    runtime.submit("billing.charge", 3, b"input", "default", delay_ms=300)

    async def main():
        [claimed] = await claim(runtime)
        return claimed

    claimed = asyncio.run(main())

    assert time.monotonic() - started >= 0.3
    assert claimed.attempt_number == 1


def test_a_delayed_task_is_in_the_scheduled_state_until_due(runtime):
    runtime.submit("billing.charge", 3, b"input", "default", delay_ms=60_000)

    async def main():
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(runtime.claim_pending(1), 0.2)

    asyncio.run(main())


def test_a_delayed_task_can_be_looked_up_as_scheduled_and_then_queued(runtime):
    task_id = runtime.submit("billing.charge", 3, b"input", "default", delay_ms=150)
    run_ids = runtime.task_run_ids(task_id)
    assert [runtime.task_run_state(run_id) for run_id in run_ids] == ["Scheduled"]

    time.sleep(0.5)

    assert [runtime.task_run_state(run_id) for run_id in run_ids] == ["Queued"]


def test_a_pending_task_expires_on_its_own_and_says_so(runtime):
    task_id = runtime.submit("billing.charge", 3, b"input", "default", expires_in_ms=100)

    async def main():
        await leader(runtime)
        return await next_events(runtime)

    [event] = asyncio.run(main())

    assert event.kind == "expired"
    assert event.task_id == task_id
    assert runtime.task_run_state(event.task_run_id) == "Expired"


def test_an_expired_task_is_never_claimed(runtime):
    runtime.submit("billing.charge", 3, b"input", "default", expires_in_ms=50)

    async def main():
        await leader(runtime)
        await next_events(runtime)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(runtime.claim_pending(1), 0.3)

    asyncio.run(main())


def test_next_events_waits_until_something_happens(runtime):
    async def main():
        pending = asyncio.ensure_future(runtime.next_events())
        await asyncio.sleep(0.2)
        assert not pending.done()
        runtime.submit("billing.charge", 3, b"input", "default", expires_in_ms=50)
        return await asyncio.wait_for(pending, WAIT_LIMIT_SECONDS)

    [event] = asyncio.run(main())

    assert event.kind == "expired"


def test_shutdown_fails_a_wait_for_events():
    native = new_runtime()

    async def main():
        pending = asyncio.ensure_future(native.next_events())
        await asyncio.sleep(0.1)
        native.shutdown()
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(pending, WAIT_LIMIT_SECONDS)

    asyncio.run(main())


def test_a_task_that_is_claimed_in_time_does_not_expire(runtime):
    runtime.submit("billing.charge", 3, b"input", "default", expires_in_ms=500)

    async def main():
        [claimed] = await claim(runtime)
        await asyncio.sleep(0.7)
        runtime.report_started(claimed.task_run_id)
        runtime.complete(claimed.task_run_id, DIGEST)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(runtime.next_events(), 0.2)

    asyncio.run(main())


def test_a_pending_task_can_be_cancelled_and_says_so(runtime):
    task_id = submit(runtime)

    async def main():
        await leader(runtime)
        outcome = runtime.cancel(task_id)
        [event] = await next_events(runtime)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(runtime.claim_pending(1), 0.2)
        return outcome, event

    outcome, event = asyncio.run(main())

    assert outcome == "cancelled"
    assert (event.kind, event.task_id, event.was_running) == ("cancelled", task_id, False)
    assert runtime.task_run_state(event.task_run_id) == "Cancelled"


def test_a_running_task_can_be_cancelled_and_its_report_is_then_refused(runtime):
    task_id = submit(runtime)

    async def main():
        [claimed] = await claim(runtime)
        runtime.report_started(claimed.task_run_id)
        outcome = runtime.cancel(task_id)
        [event] = await next_events(runtime)
        return claimed, outcome, event

    claimed, outcome, event = asyncio.run(main())

    assert outcome == "cancelled"
    assert event.was_running is True
    with pytest.raises(RuntimeError):
        runtime.complete(claimed.task_run_id, DIGEST)


def test_a_finished_or_unknown_task_cannot_be_cancelled(runtime):
    run_id = started_run(runtime)
    finished = runtime.complete(run_id, DIGEST).task_id

    assert runtime.cancel(finished) == "finished"
    assert runtime.cancel("no-such-task") == "unknown"


def generation(native, payload, key=""):
    return native.submit("index.refresh", 0, payload, "default", coalescing_key=key)


def test_a_newer_generation_supersedes_the_older_and_carries_its_payload(runtime):
    older = generation(runtime, b"a")
    newer = generation(runtime, b"b")

    async def main():
        [event] = await next_events(runtime)
        [claimed] = await claim(runtime)
        return event, claimed

    event, claimed = asyncio.run(main())

    assert (event.kind, event.task_id, event.superseded_by) == ("superseded", older, newer)
    assert runtime.task_run_state(event.task_run_id) == "Superseded"
    assert claimed.task_id == newer
    assert claimed.serialized_input == b"b"
    assert claimed.chain == [b"a"]


def test_a_task_without_a_key_has_no_chain_and_is_never_superseded(runtime):
    first = submit(runtime, payload=b"a")
    second = submit(runtime, payload=b"b")

    claims = asyncio.run(claim(runtime))

    assert [c.task_id for c in claims] == [first, second]
    assert all(c.chain == [] for c in claims)


def test_only_one_generation_of_a_key_runs_at_a_time(runtime):
    generation(runtime, b"a")

    async def main():
        [first] = await claim(runtime)
        runtime.report_started(first.task_run_id)
        second_id = generation(runtime, b"b")
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(runtime.claim_pending(1), 0.3)
        waiting = asyncio.ensure_future(runtime.claim_pending(1))
        await asyncio.sleep(0.1)
        assert not waiting.done()
        runtime.complete(first.task_run_id, DIGEST)
        [second] = await asyncio.wait_for(waiting, WAIT_LIMIT_SECONDS)
        return second_id, second

    second_id, second = asyncio.run(main())

    assert second.task_id == second_id
    assert second.chain == []


def limited_runtime():
    return new_runtime(memory_soft_limit=100, memory_hard_limit=200)


def test_slow_down_is_raised_past_the_soft_limit_and_cleared_once_memory_falls():
    native = limited_runtime()
    try:

        async def main():
            await leader(native)
            first = native.submit("bulk.load", 0, b"x" * 60, "default")
            second = native.submit("bulk.load", 0, b"x" * 60, "default")
            [raised] = await next_events(native)
            native.cancel(first)
            native.cancel(second)
            events = await next_events(native)
            return raised, events

        raised, events = asyncio.run(main())

        assert raised.kind == "slow_down"
        assert [e.kind for e in events if e.kind.startswith("slow_down")] == ["slow_down_cleared"]
    finally:
        native.shutdown()


def test_completing_a_task_clears_slow_down_and_says_so():
    native = limited_runtime()
    try:

        async def main():
            await leader(native)
            native.submit("bulk.load", 0, b"x" * 150, "default")
            [raised] = await next_events(native)
            [claimed] = await claim(native)
            native.report_started(claimed.task_run_id)
            native.complete(claimed.task_run_id, DIGEST)
            cleared = await next_events(native)
            return raised, cleared

        raised, cleared = asyncio.run(main())

        assert raised.kind == "slow_down"
        assert [event.kind for event in cleared] == ["slow_down_cleared"]
    finally:
        native.shutdown()


def test_backpressure_error_is_raised_past_the_hard_limit():
    from kabudachi.errors import BackpressureError

    native = limited_runtime()
    try:
        native.submit("bulk.load", 0, b"x" * 150, "default")

        with pytest.raises(BackpressureError):
            native.submit("bulk.load", 0, b"x" * 51, "default")
    finally:
        native.shutdown()


def test_a_coalescing_task_that_opted_in_drops_its_oldest_payloads_to_fit():
    native = limited_runtime()
    try:
        for size in (61, 62, 63):
            native.submit("index.refresh", 0, b"x" * size, "default", coalescing_key="k")

        native.submit(
            "index.refresh", 0, b"y" * 50, "default", coalescing_key="k", drop_oldest=True
        )

        async def main():
            [claimed] = await claim(native)
            return claimed

        claimed = asyncio.run(main())
        assert [len(payload) for payload in claimed.chain] == [62, 63]
    finally:
        native.shutdown()


def test_a_task_completed_with_a_continuation_holds_its_coalescing_key_until_it_ends(runtime):
    first = generation(runtime, b"a")

    async def main():
        [claimed] = await claim(runtime)
        runtime.report_started(claimed.task_run_id)
        newer = generation(runtime, b"b")
        runtime.complete(claimed.task_run_id, DIGEST, continues=True)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(runtime.claim_pending(1), 0.3)
        assert runtime.end_continuation(first) is True
        assert runtime.end_continuation(first) is False
        [second] = await claim(runtime)
        return newer, second

    newer, second = asyncio.run(main())

    assert second.task_id == newer
