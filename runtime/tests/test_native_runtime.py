"""The native runtime as Python sees it: a Tokio runtime that elects the lone
worker leader, and an awaitable bridge that resolves on the asyncio loop."""

import asyncio
import gc
import sys
import threading
import time

import pytest

from kabudachi import _native

WAIT_LIMIT_SECONDS = 5
# Well under the native runtime's one second shutdown grace, which a shutdown
# that had to give up on something would run into.
PROMPT_SECONDS = 0.9


def new_runtime(**options):
    return _native.NativeRuntime("worker-1", "incarnation-1", **options)


@pytest.fixture
def runtime():
    native = new_runtime()
    yield native
    native.shutdown()


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


def test_waiting_outside_an_event_loop_is_an_error(runtime):
    with pytest.raises(RuntimeError, match="event loop"):
        runtime.wait_until_leader()


def test_shutdown_stops_the_worker():
    native = new_runtime()

    native.shutdown()

    assert native.worker_state() == "Stopped"


def test_waiting_after_shutdown_is_an_error():
    native = new_runtime()
    native.shutdown()

    async def main():
        native.wait_until_leader()

    with pytest.raises(RuntimeError, match="shut down"):
        asyncio.run(main())


def test_a_runtime_needs_at_least_one_thread():
    with pytest.raises(ValueError, match="worker_threads"):
        new_runtime(worker_threads=0)


def test_a_single_worker_thread_is_enough():
    native = new_runtime(worker_threads=1)
    try:
        asyncio.run(leader_within_limit(native))
    finally:
        native.shutdown()


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


def test_cancelled_waits_stop_counting():
    native = new_runtime(suspect_timeout_ms=60_000)
    try:

        async def main():
            waiters = [asyncio.ensure_future(native.wait_until_leader()) for _ in range(20)]
            await asyncio.sleep(0.05)
            assert native.in_flight_waits() == 20
            for waiter in waiters:
                waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
            await asyncio.sleep(0.05)
            return native.in_flight_waits()

        assert asyncio.run(main()) == 0
    finally:
        native.shutdown()


def test_a_finished_wait_stops_counting(runtime):
    async def main():
        await leader_within_limit(runtime)
        await asyncio.sleep(0.05)

    asyncio.run(main())

    assert runtime.in_flight_waits() == 0


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

        time.sleep(0.4)
    finally:
        sys.unraisablehook = previous_hook
        native.shutdown()

    assert reports == []


async def _start_a_wait(native):
    native.wait_until_leader()
