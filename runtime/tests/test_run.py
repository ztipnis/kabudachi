"""`kabudachi.run(main)` end to end: a real native runtime, a real worker
loop, tasks called from `main` and their results awaited."""

import asyncio
import functools
import logging
import threading
import time
from datetime import timedelta

import pytest

import kabudachi
from kabudachi import config as config_module
from kabudachi import lifecycle as lifecycle_module
from kabudachi import registry as registry_module
from kabudachi import runner as runner_module
from kabudachi.config import Configuration
from kabudachi.errors import RuntimeNotStartedError, StartupError, TaskBodyError, TaskDefinitionError
from kabudachi.lifecycle import HookRegistry
from kabudachi.registry import TaskRegistry
from proto_messages import Greeting, Receipt


@pytest.fixture(autouse=True)
def fresh_process_state(monkeypatch):
    """Each test declares its own tasks, hooks and settings, and starts with
    none left over."""
    monkeypatch.setattr(registry_module, "_default_registry", TaskRegistry())
    monkeypatch.setattr(lifecycle_module, "_default_hooks", HookRegistry())
    # Bodies here share state with the test through closures, which only a
    # body running in this process can see.
    configuration = Configuration()
    configuration.configure(processes=0)
    monkeypatch.setattr(config_module, "_process_configuration", configuration)


def declare(function, **options):
    return kabudachi.task(name=f"tests.{function.__name__}", **options)(function)


def test_a_synchronous_task_is_run_and_its_result_awaited():
    @declare
    def greet(request: Greeting) -> Greeting:
        return Greeting(text=f"hello {request.text}", times=request.times + 1)

    async def main():
        return await greet(Greeting(text="world", times=1))

    assert kabudachi.run(main) == Greeting(text="hello world", times=2)


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
        handles = [square(Greeting(times=n)) for n in range(50)]
        return [g.times for g in await asyncio.gather(*handles)]

    assert kabudachi.run(main) == [n * n for n in range(50)]


def test_calling_a_task_returns_a_handle_before_it_has_run():
    @declare
    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(0.05)
        return request

    async def main():
        handle = slow(Greeting(text="x"))
        before = handle.done()
        await handle
        return handle, before, handle.done(), handle.task_id, handle.shard_id

    handle, before, after, task_id, shard = kabudachi.run(main)

    assert isinstance(handle, kabudachi.TaskHandle)
    assert (before, after) == (False, True)
    assert shard.startswith("local/") and len(shard) > len("local/")
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


@pytest.mark.parametrize("is_async", [True, False], ids=["async", "sync"])
def test_no_more_tasks_run_at_once_than_configured(is_async):
    lock = threading.Lock()
    active = 0
    peak = 0

    def enter():
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)

    def leave():
        nonlocal active
        with lock:
            active -= 1

    if is_async:

        @declare
        async def slow(request: Greeting) -> Greeting:
            enter()
            await asyncio.sleep(0.03)
            leave()
            return request

    else:

        @declare
        def slow(request: Greeting) -> Greeting:
            enter()
            time.sleep(0.05)
            leave()
            return request

    kabudachi.configure(concurrency=2)

    async def main():
        return await asyncio.gather(*(slow(Greeting(text=str(n))) for n in range(8)))

    results = kabudachi.run(main)

    assert [result.text for result in results] == [str(n) for n in range(8)]
    assert peak == 2


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
        await asyncio.sleep(0.05)
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
        await asyncio.sleep(0.1)
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


def test_a_task_cannot_be_called_after_run_has_finished():
    @declare
    def greet(request: Greeting) -> Greeting:
        return request

    async def main():
        return None

    kabudachi.run(main)

    with pytest.raises(RuntimeNotStartedError):
        greet(Greeting())


def test_run_can_be_used_again_after_it_finishes_or_its_main_failed():
    @declare
    def greet(request: Greeting) -> Greeting:
        return request

    async def main():
        return (await greet(Greeting(text="again"))).text

    async def broken():
        raise RuntimeError("main fails")

    assert kabudachi.run(main) == "again"
    with pytest.raises(RuntimeError, match="main fails"):
        kabudachi.run(broken)
    assert kabudachi.run(main) == "again"


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


def test_a_task_that_fails_is_retried_through_the_real_runtime_and_finally_succeeds():
    calls = []

    @functools.partial(declare, retries=2)
    def flaky(request: Greeting) -> Greeting:
        calls.append(request.text)
        if len(calls) < 3:
            raise ValueError("not yet")
        return Greeting(text=f"done after {len(calls)}")

    async def main():
        return await flaky(Greeting(text="x"))

    assert kabudachi.run(main).text == "done after 3"
    assert calls == ["x", "x", "x"], "every attempt saw the same input"


def test_a_delayed_task_does_not_start_before_its_delay_has_passed():
    started_at = []

    @declare
    def stamp(request: Greeting) -> Greeting:
        started_at.append(time.monotonic())
        return request

    async def main():
        submitted = time.monotonic()
        await stamp.options(delay=timedelta(milliseconds=100))(Greeting())
        return started_at[0] - submitted

    assert kabudachi.run(main) >= 0.1


def test_a_task_that_times_out_is_retried_and_can_then_succeed():
    calls = []

    @functools.partial(declare, timeout=timedelta(milliseconds=80), retries=1)
    async def slow_once(request: Greeting) -> Greeting:
        calls.append(True)
        if len(calls) == 1:
            await asyncio.sleep(30)
        return Greeting(text="second try")

    async def main():
        return await slow_once(Greeting())

    assert kabudachi.run(main).text == "second try"
    assert len(calls) == 2


def declare_coalescing(function, **options):
    return kabudachi.coalescing_task(name=f"tests.{function.__name__}", **options)(function)


def test_a_running_generation_is_never_cancelled_and_the_next_one_waits_for_it():
    order = []

    @declare_coalescing
    async def refresh(request: Greeting) -> Greeting:
        order.append(f"start {request.text}")
        await asyncio.sleep(0.1)
        order.append(f"end {request.text}")
        return request

    async def main():
        running = refresh(Greeting(text="one"))
        while "start one" not in order:
            await asyncio.sleep(0.001)
        newer = refresh(Greeting(text="two"))
        return await running, await newer

    one, two = kabudachi.run(main)

    assert (one.text, two.text) == ("one", "two")
    assert order == ["start one", "end one", "start two", "end two"]


def test_different_keys_run_side_by_side():
    active = 0
    peak = 0

    @declare_coalescing
    async def refresh(request: Greeting) -> Greeting:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.1)
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


def test_a_flow_runs_its_stages_one_after_another_and_a_group_runs_side_by_side_through_the_real_runtime():
    @declare
    def shout(request: Greeting) -> Greeting:
        return Greeting(text=request.text.upper(), times=request.times)

    @declare
    def count(request: Greeting) -> Receipt:
        return Receipt(ok=request.times > 0)

    pipeline = kabudachi.flow(shout, shout.bind(times=5), kabudachi.group(shout, count))

    async def main():
        flow_handle = pipeline(Greeting(text="hi"))
        group_handle = kabudachi.group(shout, count)(Greeting(text="hi", times=5))
        return flow_handle, group_handle, await flow_handle, await group_handle

    flow_handle, group_handle, flowed, grouped = kabudachi.run(main)

    assert isinstance(flow_handle, kabudachi.FlowHandle)
    assert isinstance(group_handle, kabudachi.GroupHandle)
    assert flowed == [
        Greeting(text="HI"),
        Greeting(text="HI", times=5),
        [Greeting(text="HI", times=5), Receipt(ok=True)],
    ]
    assert grouped == [Greeting(text="HI", times=5), Receipt(ok=True)]


def test_a_bound_task_is_called_like_a_task():
    @declare
    def shout(request: Greeting) -> Greeting:
        return Greeting(text=request.text.upper())

    async def main():
        return await shout.bind(Greeting(text="fixed"))()

    assert kabudachi.run(main).text == "FIXED"


def test_a_submission_past_the_hard_memory_limit_raises_backpressure_error():
    from kabudachi.errors import BackpressureError, KabudachiError

    @declare
    def hold(request: Greeting) -> Greeting:
        return request

    kabudachi.configure(memory_soft_limit=100, memory_hard_limit=200, concurrency=1)

    async def main():
        with pytest.raises(BackpressureError) as refused:
            for _ in range(50):
                hold(Greeting(text="x" * 40))
        return refused.value

    refused = kabudachi.run(main)

    # A kabudachi error and a RuntimeError, as the pure-Python class was.
    assert isinstance(refused, KabudachiError)
    assert isinstance(refused, RuntimeError)


def test_a_long_chain_is_compacted_while_its_key_is_busy_and_folds_in_order():
    """A key's running generation holds it while newer ones pile up past the
    soft limit; a compaction folds the waiting payloads on a free worker
    place before the newest generation starts, and the newest still sees
    every payload folded in submission order."""
    kabudachi.configure(memory_soft_limit=400, memory_hard_limit=100_000, concurrency=2)
    holder_running = threading.Event()
    compacted_while_held = threading.Event()

    def fold_left(older: Greeting, newer: Greeting) -> Greeting:
        if holder_running.is_set():
            compacted_while_held.set()
        return Greeting(text=f"({older.text}>{newer.text})")

    @functools.partial(declare_coalescing, merge=fold_left)
    async def refresh(request: Greeting) -> Greeting:
        if request.text == "holder":
            holder_running.set()
            # Holds the key until a compaction has folded while it ran.
            for _ in range(500):
                if compacted_while_held.is_set():
                    break
                await asyncio.sleep(0.01)
            holder_running.clear()
        return request

    texts = [letter * 60 for letter in "abcdefgh"]

    async def main():
        held = refresh(Greeting(text="holder"))
        while not holder_running.is_set():
            await asyncio.sleep(0.01)
        handles = [refresh(Greeting(text=text)) for text in texts]
        await held
        return await handles[-1]

    newest = kabudachi.run(main)

    expected = functools.reduce(fold_left, [Greeting(text=text) for text in texts])
    assert newest.text == expected.text
    assert compacted_while_held.is_set(), "a compaction folded payloads while the key was busy"


def test_a_merge_that_fails_ends_its_compaction_and_fails_the_newest_generation_with_its_error():
    """The compaction run is internal, so the failure of its merge reaches no
    handle: it is reported to the leader, and the newest generation, which
    folds the whole chain itself, fails with the same error."""
    kabudachi.configure(memory_soft_limit=400, memory_hard_limit=100_000, concurrency=2)
    holder_running = threading.Event()
    merge_failed_while_held = threading.Event()

    def broken_merge(older: Greeting, newer: Greeting) -> Greeting:
        if holder_running.is_set():
            merge_failed_while_held.set()
        raise ValueError("these cannot be merged")

    @functools.partial(declare_coalescing, merge=broken_merge)
    async def refresh(request: Greeting) -> Greeting:
        if request.text == "holder":
            holder_running.set()
            for _ in range(500):
                if merge_failed_while_held.is_set():
                    break
                await asyncio.sleep(0.01)
            holder_running.clear()
        return request

    async def main():
        held = refresh(Greeting(text="holder"))
        while not holder_running.is_set():
            await asyncio.sleep(0.01)
        handles = [refresh(Greeting(text=letter * 60)) for letter in "abcdefgh"]
        await held
        with pytest.raises(ValueError, match="cannot be merged"):
            await handles[-1]

    kabudachi.run(main)

    assert merge_failed_while_held.is_set(), "the compaction ran the merge while the key was busy"


def test_lifecycle_hooks_run_in_this_process_around_the_runs_of_their_queues(caplog):
    seen = []
    per_thread = threading.local()

    @kabudachi.process_init
    async def opened():
        seen.append("init")

    @kabudachi.before_run(queues=["hooked"])
    async def started(context):
        seen.append(f"before {context.task_name} {context.attempt}")

    @kabudachi.before_run(queues=["hooked"])
    def checked_out(context):
        per_thread.context = context

    @kabudachi.after_run(queues=["hooked"])
    def checked_in(context, outcome):
        seen.append(f"after {context.task_name} {type(outcome).__name__}")
        raise ValueError("could not return the connection")

    @kabudachi.after_run(queues=["hooked"])
    async def closed(context, outcome):
        seen.append(f"closed {context.task_name}")

    def hooked(request: Greeting) -> Greeting:
        context = per_thread.context  # left by the before_run hook, on this body's thread
        return Greeting(text=context.run_id, times=context.attempt)

    def plain(request: Greeting) -> Greeting:
        return request

    hooked = declare(hooked, queue="hooked")
    plain = declare(plain)

    async def main():
        return await hooked(Greeting()), await plain(Greeting(text="untouched"))

    with caplog.at_level(logging.WARNING, logger="kabudachi"):
        hooked_result, plain_result = kabudachi.run(main)

    assert seen == [
        "init",
        "before tests.hooked 1",
        "after tests.hooked Greeting",
        "closed tests.hooked",
    ], "the after_run hooks all ran, the second after the first raised"
    assert hooked_result.times == 1 and hooked_result.text, "the body saw its run's context"
    assert plain_result.text == "untouched", "hooks of another queue left it alone"
    assert "ValueError" in caplog.text, "the failed after_run hook was logged; the result stood"


def test_a_body_or_hook_that_raises_system_exit_or_keyboard_interrupt_or_cancels_itself_fails_only_its_run_here():
    leaving = {"SystemExit": SystemExit, "KeyboardInterrupt": KeyboardInterrupt}

    @kabudachi.after_run(queues=["leaving"])
    def interrupts_cleanup(context, outcome):
        raise leaving[outcome.text]("leaving")

    def leaves(request: Greeting) -> Greeting:
        raise leaving[request.text]("leaving")

    async def leaves_async(request: Greeting) -> Greeting:
        raise SystemExit("leaving")

    async def cancels_itself(request: Greeting) -> Greeting:
        raise asyncio.CancelledError

    def cleaned_up(request: Greeting) -> Greeting:
        return request

    async def cleaned_up_async(request: Greeting) -> Greeting:
        return request

    async def steady(request: Greeting) -> Greeting:
        await asyncio.sleep(0.2)
        return Greeting(text="finished")

    leaves, leaves_async, cancels_itself, steady = map(
        declare, (leaves, leaves_async, cancels_itself, steady)
    )
    cleaned_up = declare(cleaned_up, queue="leaving")
    cleaned_up_async = declare(cleaned_up_async, queue="leaving")

    async def main():
        async with asyncio.timeout(10):
            neighbour = steady(Greeting())
            outcomes = await asyncio.gather(
                leaves(Greeting(text="SystemExit")),
                leaves(Greeting(text="KeyboardInterrupt")),
                leaves_async(Greeting()),
                cancels_itself(Greeting()),
                cleaned_up(Greeting(text="KeyboardInterrupt")),
                cleaned_up_async(Greeting(text="SystemExit")),
                return_exceptions=True,
            )
            return outcomes, (await neighbour).text

    outcomes, neighbour = kabudachi.run(main)

    assert [(type(outcome), getattr(outcome, "kind", None)) for outcome in outcomes] == [
        (TaskBodyError, "SystemExit"),
        (TaskBodyError, "KeyboardInterrupt"),
        (TaskBodyError, "SystemExit"),
        (TaskBodyError, "CancelledError"),
        (TaskBodyError, "KeyboardInterrupt"),
        (TaskBodyError, "SystemExit"),
    ], outcomes
    assert neighbour == "finished", "a body beside them ran to its end"


def test_a_process_init_hook_that_raises_system_exit_stops_the_start_with_its_name():
    @kabudachi.process_init
    def gives_up():
        raise SystemExit("no resources")

    async def main():
        pass

    with pytest.raises(StartupError, match=r"gives_up raised SystemExit: no resources"):
        kabudachi.run(main)

