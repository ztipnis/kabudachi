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
    assert only.attempt_number == 1


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


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(lambda native: submit(native), id="submit"),
        pytest.param(lambda native: native.report_started("some-run"), id="report_started"),
        pytest.param(lambda native: native.complete("some-run", DIGEST), id="complete"),
        pytest.param(lambda native: native.fail("some-run", "ValueError"), id="fail"),
        pytest.param(lambda native: native.cancel("some-task"), id="cancel"),
        pytest.param(lambda native: native.end_continuation("some-task"), id="end_continuation"),
        pytest.param(lambda native: native.task_run_state("some-run"), id="task_run_state"),
        pytest.param(lambda native: native.task_run_ids("some-task"), id="task_run_ids"),
    ],
)
def test_every_scheduler_operation_after_shutdown_is_refused(operation):
    native = new_runtime()
    native.shutdown()

    with pytest.raises(RuntimeError, match="shut down"):
        operation(native)


def test_a_claim_limit_of_zero_is_refused(runtime):
    async def main():
        runtime.claim_pending(0)

    with pytest.raises(ValueError, match="limit"):
        asyncio.run(main())


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


def test_shutdown_fails_a_wait_for_events():
    native = new_runtime()

    async def main():
        pending = asyncio.ensure_future(native.next_events())
        await asyncio.sleep(0.1)
        native.shutdown()
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(pending, WAIT_LIMIT_SECONDS)

    asyncio.run(main())


def generation(native, payload, key=""):
    return native.submit("index.refresh", 0, payload, "default", coalescing_key=key)


def limited_runtime():
    return new_runtime(memory_soft_limit=100, memory_hard_limit=200)


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
