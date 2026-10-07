"""The native extension as Python sees it: its version and digest, a Tokio
runtime that elects the lone worker leader behind an awaitable bridge that
resolves on the asyncio loop, and submitting, claiming, running and certifying
tasks through it without any of the Python task API on top."""

import asyncio
import gc
import sys
import threading
import time

import pytest

import kabudachi
from kabudachi import _native
from kabudachi._native import CancelOutcome, RunState

WAIT_LIMIT_SECONDS = 5


def new_runtime(**options):
    return _native.NativeRuntime("worker-1", "incarnation-1", **options)


@pytest.fixture
def runtime():
    native = new_runtime()
    yield native
    native.shutdown()


def test_native_version_matches_python_version():
    assert _native.version() == kabudachi.__version__


def test_a_result_digest_is_blake3():
    # BLAKE3 of the empty input, from the BLAKE3 reference test vectors.
    assert _native.result_digest(b"") == bytes.fromhex(
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
    )


# --- the bridge and the worker ------------------------------

# Well under the native runtime's one second shutdown grace, which a shutdown
# that had to give up on something would run into.
PROMPT_SECONDS = 0.9
# Longer than the shutdown grace plus the shutdown timeout.
GIL_HOLD_SECONDS = 2.5


async def leader_within_limit(native):
    await asyncio.wait_for(native.wait_until_leader(), WAIT_LIMIT_SECONDS)


def test_many_waiters_are_all_released(runtime):
    async def main():
        waiters = [runtime.wait_until_leader() for _ in range(50)]
        await asyncio.wait_for(asyncio.gather(*waiters), WAIT_LIMIT_SECONDS)

    asyncio.run(main())


def test_cancelling_a_wait_reports_no_error_when_leadership_arrives_later(runtime):
    problems = []

    async def main():
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: problems.append(context)
        )
        waiter = asyncio.ensure_future(runtime.wait_until_leader())
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        await leader_within_limit(runtime)
        await asyncio.sleep(0.05)

    asyncio.run(main())

    assert problems == []


def test_a_result_that_arrives_after_cancellation_is_dropped_quietly(runtime):
    problems = []

    async def main():
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda loop, context: problems.append(context))
        await leader_within_limit(runtime)

        waiter = runtime.wait_until_leader()
        # Hold the loop still while the native side finishes and queues its
        # result, then cancel before the loop gets to run it.
        time.sleep(0.1)
        waiter.cancel()
        await asyncio.sleep(0.05)

        assert waiter.cancelled()

    asyncio.run(main())

    assert problems == []


def test_shutdown_fails_every_pending_wait():
    native = new_runtime(suspect_timeout_ms=60_000)

    async def main():
        waiters = [asyncio.ensure_future(native.wait_until_leader()) for _ in range(100)]
        await asyncio.sleep(0.05)
        started = time.monotonic()
        native.shutdown()
        elapsed = time.monotonic() - started
        outcomes = await asyncio.wait_for(
            asyncio.gather(*waiters, return_exceptions=True), WAIT_LIMIT_SECONDS
        )
        return outcomes, elapsed

    outcomes, elapsed = asyncio.run(main())

    assert elapsed < PROMPT_SECONDS
    assert len(outcomes) == 100
    assert all(isinstance(outcome, RuntimeError) for outcome in outcomes)


def test_no_wait_is_lost_when_shutdown_races_new_waits():
    for delay_seconds in (0, 0.001, 0.002, 0.005, 0.01):
        native = new_runtime(suspect_timeout_ms=60_000)
        issued = []
        stranded = []
        started = threading.Event()

        def keep_waiting():
            async def spin():
                while True:
                    try:
                        issued.append(asyncio.ensure_future(native.wait_until_leader()))
                    except RuntimeError:
                        break
                    started.set()
                    await asyncio.sleep(0)
                _, pending = await asyncio.wait(issued, timeout=WAIT_LIMIT_SECONDS)
                stranded.extend(pending)
                for future in issued:
                    if future.done() and not future.cancelled():
                        future.exception()

            asyncio.run(spin())

        thread = threading.Thread(target=keep_waiting)
        thread.start()
        started.wait(WAIT_LIMIT_SECONDS)
        time.sleep(delay_seconds)
        native.shutdown()
        thread.join(WAIT_LIMIT_SECONDS)
        assert not thread.is_alive(), "a waiter was left blocked in the native call"

        assert issued
        assert stranded == [], delay_seconds


def test_waits_whose_delivery_outlasts_the_shutdown_grace_still_fail():
    waits = 1000
    native = new_runtime(suspect_timeout_ms=60_000)
    issued = []
    ready = threading.Event()
    loop_done = threading.Event()
    hog_done = threading.Event()
    hold_problems = []
    outcomes = []

    def keep_waiting():
        async def main():
            issued.extend(
                asyncio.ensure_future(native.wait_until_leader()) for _ in range(waits)
            )
            ready.set()
            # Start the clock only once the GIL hold is over, so a slow host
            # cannot run the limit out while the hold is still in progress.
            await asyncio.get_running_loop().run_in_executor(None, hog_done.wait)
            done, pending = await asyncio.wait(issued, timeout=WAIT_LIMIT_SECONDS)
            outcomes.extend(future.exception() for future in done)
            outcomes.extend(None for _ in pending)

        asyncio.run(main())
        loop_done.set()

    def hold_the_gil():
        # Once shutdown has begun, keep the GIL for a fixed wall-clock time,
        # longer than the shutdown grace plus the shutdown timeout, so no
        # result can be handed to the loop in time whatever the host speed.
        previous_interval = sys.getswitchinterval()
        try:
            stop_by = time.monotonic() + WAIT_LIMIT_SECONDS
            while native.worker_state() != "Stopped":
                if time.monotonic() > stop_by:
                    hold_problems.append("the worker never reached Stopped")
                    return
            sys.setswitchinterval(30)
            deadline = time.monotonic() + GIL_HOLD_SECONDS
            while time.monotonic() < deadline:
                pass
        finally:
            sys.setswitchinterval(previous_interval)
            hog_done.set()

    loop_thread = threading.Thread(target=keep_waiting)
    loop_thread.start()
    assert ready.wait(WAIT_LIMIT_SECONDS)
    hog = threading.Thread(target=hold_the_gil)
    hog.start()
    native.shutdown()
    hog.join(WAIT_LIMIT_SECONDS * 2)
    assert not hog.is_alive()
    assert hold_problems == []
    assert loop_done.wait(WAIT_LIMIT_SECONDS * 2)
    loop_thread.join()

    assert len(outcomes) == waits
    assert all(isinstance(outcome, RuntimeError) for outcome in outcomes), [
        outcome for outcome in outcomes if not isinstance(outcome, RuntimeError)
    ][:3]


def test_cancelled_and_finished_waits_stop_counting():
    native = new_runtime(suspect_timeout_ms=500)
    try:

        async def main():
            waiters = [asyncio.ensure_future(native.wait_until_leader()) for _ in range(20)]
            counted_while_pending = native.in_flight_waits()
            assert counted_while_pending == 20
            for waiter in waiters[:10]:
                waiter.cancel()
            await asyncio.wait_for(
                asyncio.gather(*waiters, return_exceptions=True), WAIT_LIMIT_SECONDS
            )
            await asyncio.sleep(0.05)
            return counted_while_pending, native.in_flight_waits()

        counted_while_pending, counted_after = asyncio.run(main())
    finally:
        native.shutdown()

    assert counted_after == 0


def test_a_runtime_that_is_never_shut_down_can_be_discarded():
    native = new_runtime()
    started = time.monotonic()

    del native
    gc.collect()

    assert time.monotonic() - started < PROMPT_SECONDS


def test_concurrent_shutdowns_both_finish_with_the_worker_stopped():
    native = new_runtime()
    states = []

    def shut_down():
        native.shutdown()
        states.append(native.worker_state())

    threads = [threading.Thread(target=shut_down) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert states == ["Stopped"] * 4


def test_a_result_for_a_closed_loop_is_dropped_without_a_report():
    native = new_runtime(suspect_timeout_ms=100)
    reports = []
    previous_hook = sys.unraisablehook
    sys.unraisablehook = reports.append
    try:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(_start_a_wait(native))
        loop.close()

        deadline = time.monotonic() + WAIT_LIMIT_SECONDS
        while native.in_flight_waits() != 0 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert native.in_flight_waits() == 0
    finally:
        sys.unraisablehook = previous_hook
        native.shutdown()

    assert reports == []


async def _start_a_wait(native):
    native.wait_until_leader()


# --- scheduling ----------------------------------------

DIGEST = _native.result_digest(b"the-result")


def submit(native, payload=b"input-bytes", queue="default"):
    return native.submit("billing.charge", 3, payload, queue, "task", None)


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


def test_submissions_made_before_the_node_leads_are_claimed_in_order_after_the_grant():
    # Leadership is 1 s away, so everything up to `wait_until_leader` is made
    # while the node does not lead.
    native = new_runtime(suspect_timeout_ms=1000, memory_soft_limit=50, memory_hard_limit=100)
    try:
        dropped = submit(native, payload=b"a" * 60)
        assert native.cancel(dropped) == CancelOutcome.CANCELLED
        first = submit(native, payload=b"b" * 30)
        second = submit(native, payload=b"c" * 30)
        with pytest.raises(_native.BackpressureError):
            submit(native, payload=b"d" * 50)
        with pytest.raises(RuntimeError, match="not the leader"):
            native.end_continuation("x")
        assert native.worker_state() != "Leader"

        async def claim_once_leading():
            await leader_within_limit(native)
            return await claim(native)

        claims = asyncio.run(claim_once_leading())
        assert [claimed.task_id for claimed in claims] == [first, second]

        later = submit(native, payload=b"e" * 40)
        [after] = asyncio.run(claim(native))
        assert after.task_id == later
    finally:
        native.shutdown()


def test_a_submission_the_record_cannot_hold_at_the_grant_waits_only_with_its_own_key():
    # Each payload fits a record alone, but a record that also carries the
    # superseded generation's input does not, so the grant records the first
    # and leaves the second queued until the first has ended. Submissions made
    # after it under another key, or under none, are recorded all the same,
    # while the leader still counts what is held against the hard limit.
    half = b"x" * 557_056
    native = new_runtime(
        suspect_timeout_ms=1000, memory_soft_limit=1_800_000, memory_hard_limit=1_900_000
    )
    try:
        first = generation(native, half)
        second = generation(native, half)
        other_key = generation(native, b"small", key="other")
        plain = submit(native)

        async def main():
            await leader_within_limit(native)
            # The node leads and `second` is still queued behind `first`. What
            # it holds counts against the hard limit for a submission that is
            # recorded at once, a submission of another key is recorded at
            # once, and a later one of the held key waits behind `second`.
            with pytest.raises(_native.BackpressureError):
                generation(native, b"y" * 1_000_000, key="late-other")
            late_other = generation(native, b"small", key="late-other")
            third = generation(native, b"third")
            claims = await claim(native)
            assert {each.task_id for each in claims} == {first, other_key, plain, late_other}
            [claimed] = [each for each in claims if each.task_id == first]
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(native.claim_pending(1), 0.3)
            native.report_started(claimed.task_run_id)
            native.complete(claimed.task_run_id, DIGEST)
            # `second` is recorded first and `third` then absorbs it.
            [next_claimed] = await claim(native)
            assert next_claimed.task_id == third
            assert next_claimed.chain == [half]

        asyncio.run(main())
    finally:
        native.shutdown()


def test_an_unknown_run_is_refused(runtime):
    asyncio.run(leader_within_limit(runtime))

    with pytest.raises(RuntimeError, match="no such run"):
        runtime.report_started("no-such-run")
    with pytest.raises(RuntimeError, match="no such run"):
        runtime.complete("no-such-run", DIGEST)
    with pytest.raises(RuntimeError, match="no such run"):
        runtime.report_failure("no-such-run", "E")
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
        pytest.param(lambda native: native.report_failure("some-run", "ValueError"), id="report_failure"),
        pytest.param(lambda native: native.cancel("some-task"), id="cancel"),
        pytest.param(lambda native: native.end_continuation("some-task"), id="end_continuation"),
        pytest.param(lambda native: native.task_run_state("some-run"), id="task_run_state"),
        pytest.param(lambda native: native.task_run_ids("some-task"), id="task_run_ids"),
        pytest.param(lambda native: asyncio.run(_start_a_wait(native)), id="wait_until_leader"),
    ],
)
def test_every_scheduler_operation_after_shutdown_is_refused(operation):
    native = new_runtime()
    native.shutdown()

    with pytest.raises(RuntimeError, match="shut down"):
        operation(native)


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


def test_a_digest_of_the_wrong_length_is_refused(runtime):
    run_id = started_run(runtime)

    with pytest.raises(ValueError):
        runtime.complete(run_id, b"short")

    assert runtime.task_run_state(run_id) == RunState.RUNNING


def test_cancelling_answers_cancelled_then_already_finished_and_unknown_for_no_such_task(runtime):
    asyncio.run(leader_within_limit(runtime))
    task_id = submit(runtime)

    assert runtime.cancel(task_id) == CancelOutcome.CANCELLED
    assert runtime.cancel(task_id) == CancelOutcome.ALREADY_FINISHED
    assert runtime.cancel("no-such-task") == CancelOutcome.UNKNOWN_TASK


def test_a_finished_task_is_forgotten_once_the_result_ttl_has_passed():
    native = new_runtime(result_ttl_ms=50)
    try:
        run_id = started_run(native)
        native.complete(run_id, DIGEST)
        assert native.task_run_state(run_id) == RunState.SUCCEEDED

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
        await asyncio.sleep(0.02)
        native.shutdown()
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(pending, WAIT_LIMIT_SECONDS)

    asyncio.run(main())


def generation(native, payload, key=""):
    return native.submit("index.refresh", 0, payload, "default", "coalescing", key)


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
