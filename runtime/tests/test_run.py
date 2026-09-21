"""`kabudachi.run(main)` end to end: a real native runtime, a real worker
loop, tasks called from `main` and their results awaited."""

import asyncio
import threading
import time
from datetime import timedelta

import pytest

import kabudachi
from kabudachi import config as config_module
from kabudachi import registry as registry_module
from kabudachi import runner as runner_module
from kabudachi import session as session_module
from kabudachi.config import Configuration
from kabudachi.errors import (
    RuntimeNotStartedError,
    TaskCancelledError,
    TaskDefinitionError,
    TaskExpiredError,
    TaskSupersededError,
    TaskTimeoutError,
)
from kabudachi.handle import TaskHandle
from kabudachi.registry import TaskRegistry
from proto_messages import Greeting, Receipt


@pytest.fixture(autouse=True)
def fresh_process_state(monkeypatch):
    """Each test declares its own tasks and settings, and starts with none left over."""
    monkeypatch.setattr(registry_module, "_default_registry", TaskRegistry())
    monkeypatch.setattr(config_module, "_process_configuration", Configuration())


def declare(function, **options):
    return kabudachi.task(name=f"tests.{function.__name__}", **options)(function)


def declare_with_retries(retries):
    return lambda function: declare(function, retries=retries)


def test_run_returns_what_main_returns():
    async def main():
        return "the result"

    assert kabudachi.run(main) == "the result"


def test_a_synchronous_task_is_run_and_its_result_awaited():
    @declare
    def greet(request: Greeting) -> Greeting:
        return Greeting(text=f"hello {request.text}", times=request.times + 1)

    async def main():
        return await greet(Greeting(text="world", times=1))

    assert kabudachi.run(main) == Greeting(text="hello world", times=2)


def test_an_async_task_is_run_and_its_result_awaited():
    @declare
    async def greet(request: Greeting) -> Greeting:
        await asyncio.sleep(0.01)
        return Greeting(text=request.text.upper())

    async def main():
        return await greet(Greeting(text="quiet"))

    assert kabudachi.run(main).text == "QUIET"


def test_a_task_that_returns_nothing_gives_none():
    seen = []

    @declare
    def note(request: Greeting) -> None:
        seen.append(request.text)

    async def main():
        return await note(Greeting(text="noted"))

    assert kabudachi.run(main) is None
    assert seen == ["noted"]


def test_a_list_of_messages_goes_through_a_task_and_back():
    @declare
    def double(requests: list[Greeting]) -> list[Greeting]:
        return [Greeting(text=r.text, times=r.times * 2) for r in requests]

    async def main():
        return await double([Greeting(text="a", times=1), Greeting(text="b", times=2)])

    assert [g.times for g in kabudachi.run(main)] == [2, 4]


def test_many_tasks_called_together_each_get_their_own_result():
    @declare
    def square(request: Greeting) -> Greeting:
        return Greeting(times=request.times**2)

    async def main():
        handles = [square(Greeting(times=n)) for n in range(300)]
        return [g.times for g in await asyncio.gather(*handles)]

    assert kabudachi.run(main) == [n * n for n in range(300)]


def test_calling_a_task_returns_a_handle_before_it_has_run():
    @declare
    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(0.05)
        return request

    async def main():
        handle = slow(Greeting(text="x"))
        before = handle.done()
        await handle
        return before, handle.done(), handle.task_id, handle.shard_id

    before, after, task_id, shard = kabudachi.run(main)

    assert (before, after, shard) == (False, True, "local")
    assert task_id


def test_a_task_that_raises_fails_its_handle_and_others_still_work():
    @declare
    def fragile(request: Greeting) -> Greeting:
        if request.text == "bad":
            raise ValueError("this one is bad")
        return request

    async def main():
        return await asyncio.gather(
            fragile(Greeting(text="bad")),
            fragile(Greeting(text="good")),
            return_exceptions=True,
        )

    bad, good = kabudachi.run(main)

    assert isinstance(bad, ValueError) and "bad" in str(bad)
    assert good == Greeting(text="good")


def test_no_more_tasks_run_at_once_than_configured():
    active = 0
    peak = 0

    @declare
    async def slow(request: Greeting) -> Greeting:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.03)
        active -= 1
        return request

    kabudachi.configure(concurrency=2)

    async def main():
        await asyncio.gather(*(slow(Greeting()) for _ in range(10)))

    kabudachi.run(main)

    assert peak == 2


def test_blocking_synchronous_tasks_do_not_stall_each_other():
    @declare
    def blocking(request: Greeting) -> Greeting:
        time.sleep(0.3)
        return request

    async def main():
        started = time.monotonic()
        await asyncio.gather(*(blocking(Greeting()) for _ in range(4)))
        return time.monotonic() - started

    assert kabudachi.run(main) < 0.9


def test_a_task_can_call_another_task_and_wait_for_it():
    @declare
    async def inner(request: Greeting) -> Greeting:
        return Greeting(times=request.times + 1)

    @declare
    async def outer(request: Greeting) -> Greeting:
        return await inner(request)

    async def main():
        return await outer(Greeting(times=1))

    assert kabudachi.run(main).times == 2


def test_a_synchronous_task_can_call_another_task():
    handles = []

    @declare
    async def inner(request: Greeting) -> Greeting:
        return Greeting(times=99)

    @declare
    def outer(request: Greeting) -> Receipt:
        handles.append(inner(request))
        return Receipt(ok=True)

    async def main():
        await outer(Greeting())
        return await handles[0]

    assert kabudachi.run(main).times == 99


def test_run_waits_for_tasks_main_started_but_did_not_await():
    done = []

    @declare
    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(0.1)
        done.append(request.text)
        return request

    async def main():
        for n in range(3):
            slow(Greeting(text=str(n)))

    kabudachi.run(main)

    assert sorted(done) == ["0", "1", "2"]


def test_an_error_in_main_propagates_and_run_does_not_wait_for_queued_tasks():
    started = []
    finished = []

    @declare
    async def slow(request: Greeting) -> Greeting:
        started.append(request.text)
        await asyncio.sleep(0.2)
        finished.append(request.text)
        return request

    kabudachi.configure(concurrency=1)

    async def main():
        for n in range(5):
            slow(Greeting(text=str(n)))
        while not started:
            await asyncio.sleep(0.005)
        raise KeyError("main failed")

    with pytest.raises(KeyError, match="main failed"):
        kabudachi.run(main)

    assert started == ["0"]
    assert finished == ["0"]


def test_a_task_cannot_be_called_outside_run():
    @declare
    def greet(request: Greeting) -> Greeting:
        return request

    with pytest.raises(RuntimeNotStartedError):
        greet(Greeting())


def test_a_task_cannot_be_called_after_run_has_finished():
    @declare
    def greet(request: Greeting) -> Greeting:
        return request

    async def main():
        return None

    kabudachi.run(main)

    with pytest.raises(RuntimeNotStartedError):
        greet(Greeting())


def test_run_can_be_used_again_after_it_finishes():
    @declare
    def greet(request: Greeting) -> Greeting:
        return request

    async def main():
        return (await greet(Greeting(text="again"))).text

    assert kabudachi.run(main) == "again"
    assert kabudachi.run(main) == "again"


def test_run_after_main_failed_still_works():
    async def broken():
        raise RuntimeError("first run fails")

    async def fine():
        return "second run works"

    with pytest.raises(RuntimeError, match="first run fails"):
        kabudachi.run(broken)

    assert kabudachi.run(fine) == "second run works"


def test_run_cannot_start_inside_a_running_event_loop():
    async def main():
        async def inner():
            return None

        kabudachi.run(inner)

    with pytest.raises(RuntimeError, match="from a running event loop"):
        asyncio.run(main())


def test_run_cannot_be_nested_and_does_not_start_a_second_runtime(monkeypatch):
    created = []
    real = runner_module._native.NativeRuntime

    def counting_runtime(*arguments, **options):
        created.append(arguments)
        return real(*arguments, **options)

    counting_native = type("Native", (), {"NativeRuntime": staticmethod(counting_runtime)})
    monkeypatch.setattr(runner_module, "_native", counting_native)

    async def inner():
        return None

    async def main():
        with pytest.raises(RuntimeError, match="already running"):
            await asyncio.get_running_loop().run_in_executor(None, kabudachi.run, inner)
        return "outer finished"

    assert kabudachi.run(main) == "outer finished"
    assert len(created) == 1


def test_run_refuses_to_start_if_a_task_cannot_work():
    calls = []

    @declare
    def greet(request: Greeting) -> Greeting:
        return request

    kabudachi.task(name="tests.orphan", serializer="not-registered")(greet.definition.func)

    async def main():
        calls.append("main ran")

    with pytest.raises(TaskDefinitionError, match="not-registered"):
        kabudachi.run(main)

    assert calls == []


def test_no_session_is_left_active_after_run():
    async def main():
        return None

    kabudachi.run(main)

    assert session_module.active_session() is None


def test_the_awaitable_handle_is_what_a_call_returns():
    @declare
    def greet(request: Greeting) -> Greeting:
        return request

    async def main():
        handle = greet(Greeting())
        kind = type(handle)
        await handle
        return kind

    assert kabudachi.run(main) is TaskHandle


def test_a_worker_thread_can_call_a_task_and_hand_back_its_handle():
    box = []

    @declare
    def greet(request: Greeting) -> Greeting:
        return request

    async def main():
        thread = threading.Thread(target=lambda: box.append(greet(Greeting(text="threaded"))))
        thread.start()
        thread.join()
        return (await box[0]).text

    assert kabudachi.run(main) == "threaded"


def test_synchronous_tasks_run_no_more_at_once_than_the_configured_concurrency():
    lock = threading.Lock()
    active = 0
    peak = 0

    @declare
    def blocking(request: Greeting) -> Greeting:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return request

    kabudachi.configure(concurrency=2)

    async def main():
        return await asyncio.gather(*(blocking(Greeting(text=str(n))) for n in range(8)))

    results = kabudachi.run(main)

    assert [result.text for result in results] == [str(n) for n in range(8)]
    assert peak == 2


def test_local_runs_beside_a_running_worker_without_using_it():
    @declare
    def greet(request: Greeting) -> Greeting:
        return Greeting(text=f"hello {request.text}")

    async def main():
        return greet.local(Greeting(text="here"))

    assert kabudachi.run(main).text == "hello here"


def test_a_task_that_fails_is_retried_through_the_real_runtime_and_finally_succeeds():
    calls = []

    @declare_with_retries(2)
    def flaky(request: Greeting) -> Greeting:
        calls.append(request.text)
        if len(calls) < 3:
            raise ValueError("not yet")
        return Greeting(text=f"done after {len(calls)}")

    async def main():
        return await flaky(Greeting(text="x"))

    assert kabudachi.run(main).text == "done after 3"


def test_a_task_that_keeps_failing_raises_its_last_error_from_the_handle():
    @declare_with_retries(1)
    def broken(request: Greeting) -> Greeting:
        raise KeyError("always")

    async def main():
        with pytest.raises(KeyError):
            await broken(Greeting())
        return "handled"

    assert kabudachi.run(main) == "handled"


def test_a_delayed_task_does_not_start_before_its_delay_has_passed():
    started_at = []

    @declare
    def stamp(request: Greeting) -> Greeting:
        started_at.append(time.monotonic())
        return request

    async def main():
        submitted = time.monotonic()
        await stamp.options(delay=timedelta(milliseconds=300))(Greeting())
        return started_at[0] - submitted

    assert kabudachi.run(main) >= 0.3


def test_a_task_that_cannot_start_before_it_expires_fails_with_task_expired_error():
    ran = []

    @declare
    async def occupy(request: Greeting) -> Greeting:
        await asyncio.sleep(0.6)
        return request

    @declare
    async def too_late(request: Greeting) -> Greeting:
        ran.append(True)
        return request

    kabudachi.configure(concurrency=1)

    async def main():
        busy = occupy(Greeting())
        await asyncio.sleep(0.1)
        late = too_late.options(expires=timedelta(milliseconds=150))(Greeting())
        with pytest.raises(TaskExpiredError):
            await late
        await busy
        return "finished"

    assert kabudachi.run(main) == "finished"
    assert ran == []


def test_a_task_past_its_timeout_fails_its_handle_with_task_timeout_error():
    cancelled = []

    @declare_timed(timedelta(milliseconds=80))
    async def slow(request: Greeting) -> Greeting:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        return request

    async def main():
        with pytest.raises(TaskTimeoutError):
            await slow(Greeting())
        return "handled"

    assert kabudachi.run(main) == "handled"
    assert cancelled == [True]


def test_a_task_that_times_out_is_retried_and_can_then_succeed():
    calls = []

    @declare_timed(timedelta(milliseconds=80), retries=1)
    async def slow_once(request: Greeting) -> Greeting:
        calls.append(True)
        if len(calls) == 1:
            await asyncio.sleep(30)
        return Greeting(text="second try")

    async def main():
        return await slow_once(Greeting())

    assert kabudachi.run(main).text == "second try"
    assert len(calls) == 2


def declare_timed(timeout, **options):
    return lambda function: declare(function, timeout=timeout, **options)


def test_a_running_task_can_be_cancelled_through_its_handle():
    cancelled = []

    @declare
    async def long(request: Greeting) -> Greeting:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        return request

    async def main():
        handle = long(Greeting())
        await asyncio.sleep(0.2)
        assert handle.cancel() is True
        with pytest.raises(TaskCancelledError):
            await handle
        return "handled"

    assert kabudachi.run(main) == "handled"
    assert cancelled == [True]


def test_a_task_that_has_not_started_can_be_cancelled_and_never_runs():
    ran = []

    @declare
    async def occupy(request: Greeting) -> Greeting:
        await asyncio.sleep(0.3)
        return request

    @declare
    async def never(request: Greeting) -> Greeting:
        ran.append(True)
        return request

    kabudachi.configure(concurrency=1)

    async def main():
        busy = occupy(Greeting())
        await asyncio.sleep(0.1)
        waiting = never(Greeting())
        assert waiting.cancel() is True
        with pytest.raises(TaskCancelledError):
            await waiting
        await busy
        return waiting.cancel()

    assert kabudachi.run(main) is False
    assert ran == []


def test_a_callback_runs_with_the_certified_result_before_run_returns():
    seen = []

    @declare
    def greet(request: Greeting) -> Greeting:
        return Greeting(text=f"hello {request.text}")

    async def main():
        greet(Greeting(text="callback")).callback(lambda result: seen.append(result.text))
        return "main is done"

    assert kabudachi.run(main) == "main is done"
    assert seen == ["hello callback"]


def declare_coalescing(function, **options):
    return kabudachi.coalescing_task(name=f"tests.{function.__name__}", **options)(function)


def concatenate(older: Greeting, newer: Greeting) -> Greeting:
    return Greeting(text=older.text + newer.text)


def test_pending_generations_are_superseded_and_the_newest_runs_on_the_folded_payloads():
    seen = []

    @declare
    async def occupy(request: Greeting) -> Greeting:
        await asyncio.sleep(0.3)
        return request

    @declare_coalescing_with(merge=concatenate)
    async def refresh(request: Greeting) -> Greeting:
        seen.append(request.text)
        return request

    kabudachi.configure(concurrency=1)

    async def main():
        busy = occupy(Greeting())
        await asyncio.sleep(0.1)
        handles = [refresh(Greeting(text=text)) for text in "abc"]
        outcomes = await asyncio.gather(*handles, return_exceptions=True)
        await busy
        return outcomes

    first, second, newest = kabudachi.run(main)

    assert isinstance(first, TaskSupersededError) and isinstance(second, TaskSupersededError)
    assert newest.text == "abc"
    assert seen == ["abc"]


def test_a_running_generation_is_never_cancelled_and_the_next_one_waits_for_it():
    order = []

    @declare_coalescing_with()
    async def refresh(request: Greeting) -> Greeting:
        order.append(f"start {request.text}")
        await asyncio.sleep(0.3)
        order.append(f"end {request.text}")
        return request

    async def main():
        running = refresh(Greeting(text="one"))
        await asyncio.sleep(0.1)
        newer = refresh(Greeting(text="two"))
        return await running, await newer

    one, two = kabudachi.run(main)

    assert (one.text, two.text) == ("one", "two")
    assert order == ["start one", "end one", "start two", "end two"]


def test_different_keys_run_side_by_side():
    active = 0
    peak = 0

    @declare_coalescing_with()
    async def refresh(request: Greeting) -> Greeting:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.2)
        active -= 1
        return request

    async def main():
        handles = [
            refresh.options(key=tenant)(Greeting(text=tenant)) for tenant in ("a", "b", "c")
        ]
        return await asyncio.gather(*handles)

    kabudachi.run(main)

    assert peak == 3


def test_an_ephemeral_task_runs_and_returns_like_a_task_in_one_process():
    @kabudachi.ephemeral_task(name="tests.ephemeral_greet")
    def ephemeral_greet(request: Greeting) -> Greeting:
        return Greeting(text=f"hi {request.text}")

    async def main():
        return await ephemeral_greet(Greeting(text="there"))

    assert kabudachi.run(main).text == "hi there"


def declare_coalescing_with(**options):
    return lambda function: declare_coalescing(function, **options)


def test_a_flow_runs_its_stages_one_after_another_through_the_real_runtime():
    @declare
    def shout(request: Greeting) -> Greeting:
        return Greeting(text=request.text.upper(), times=request.times)

    @declare
    def count(request: Greeting) -> Receipt:
        return Receipt(ok=request.times > 0)

    pipeline = kabudachi.flow(shout, shout.bind(times=5), count)

    async def main():
        return await pipeline(Greeting(text="hi"))

    assert kabudachi.run(main) == [
        Greeting(text="HI"),
        Greeting(text="HI", times=5),
        Receipt(ok=True),
    ]


def test_a_bound_task_is_called_like_a_task():
    @declare
    def shout(request: Greeting) -> Greeting:
        return Greeting(text=request.text.upper())

    async def main():
        return await shout.bind(Greeting(text="fixed"))()

    assert kabudachi.run(main).text == "FIXED"


def test_a_group_runs_its_members_side_by_side_through_the_real_runtime():
    @declare
    def shout(request: Greeting) -> Greeting:
        return Greeting(text=request.text.upper())

    @declare
    def count(request: Greeting) -> Receipt:
        return Receipt(ok=len(request.text) > 0)

    async def main():
        return await kabudachi.group(shout, count)(Greeting(text="hi"))

    assert kabudachi.run(main) == [Greeting(text="HI"), Receipt(ok=True)]


def test_a_task_that_returns_a_flow_runs_it_as_its_continuation():
    @declare
    def shout(request: Greeting) -> Greeting:
        return Greeting(text=request.text.upper())

    @declare
    def wrap(request: Greeting) -> Greeting:
        return Greeting(text=f"[{request.text}]")

    @declare
    def plan(request: Greeting) -> kabudachi.Flow:
        return kabudachi.flow(shout.bind(request), wrap)

    async def main():
        return await plan(Greeting(text="hi"))

    assert kabudachi.run(main) == [Greeting(text="HI"), Greeting(text="[HI]")]


def test_map_runs_a_task_on_every_item_and_keeps_the_order():
    @declare
    def shout(request: Greeting) -> Greeting:
        return Greeting(text=request.text.upper())

    async def main():
        return await shout.map([Greeting(text=word) for word in ("a", "b", "c")])

    assert [g.text for g in kabudachi.run(main)] == ["A", "B", "C"]


def test_a_submission_past_the_hard_memory_limit_raises_backpressure_error():
    from kabudachi.errors import BackpressureError

    @declare
    def hold(request: Greeting) -> Greeting:
        return request

    kabudachi.configure(memory_soft_limit=100, memory_hard_limit=200, concurrency=1)

    async def main():
        with pytest.raises(BackpressureError):
            for _ in range(50):
                hold(Greeting(text="x" * 40))
        return "refused"

    assert kabudachi.run(main) == "refused"
