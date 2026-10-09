"""Task bodies in task processes, through `kabudachi.run()`: where they run,
what crosses the pipe, nested calls, a process that dies, a body past its
hard limit, a cancel, and compaction. Each test starts one or two task
processes, which import their tasks from `pool_tasks`."""

import asyncio
import functools
import multiprocessing
import os
import struct
import time
from datetime import timedelta

import pytest

import kabudachi
import pool_tasks
from kabudachi import _native, ipc
from kabudachi import config as config_module
from kabudachi import lifecycle as lifecycle_module
from kabudachi import registry as registry_module
from kabudachi.config import Configuration
from kabudachi.errors import (
    StartupError,
    TaskBodyError,
    TaskCancelledError,
    TaskDefinitionError,
    TaskLostError,
    TaskTimeoutError,
)
from kabudachi.lifecycle import HookRegistry
from kabudachi.pool import ProcessPool, task_modules
from kabudachi.registry import TaskRegistry
from kabudachi.serializers import process_serializers
from kabudachi.session import Session
from faulting_runtime import FaultingRuntime
from proto_messages import Greeting


@pytest.fixture(autouse=True)
def markers(monkeypatch, tmp_path):
    """Fresh settings, the pool tasks and hooks alone in their registries,
    and a directory the test shares with its task processes."""
    registry = TaskRegistry()
    for definition in registry_module.default_registry().definitions():
        registry.register(definition)
    monkeypatch.setattr(registry_module, "_default_registry", registry)
    hooks = HookRegistry()
    for hook in lifecycle_module.default_hooks().all():
        hooks.register(hook)
    monkeypatch.setattr(lifecycle_module, "_default_hooks", hooks)
    monkeypatch.setattr(config_module, "_process_configuration", Configuration())
    monkeypatch.setenv(pool_tasks.MARKERS, str(tmp_path))
    return tmp_path


async def nothing():
    return None


def test_a_frame_cut_short_by_a_closed_pipe_ends_reading_after_the_whole_frames():
    reader, writer = multiprocessing.Pipe(duplex=False)
    writer.send(ipc.Exited("whole"))
    # A length prefix promising far more than follows, as when a task
    # process dies while writing a large frame.
    os.write(writer.fileno(), struct.pack("!i", 1_000_000) + b"cut short")
    writer.close()
    delivered = []

    ipc.read_frames(reader, delivered.append)

    assert delivered == [ipc.Exited("whole")]


def test_bodies_run_in_task_processes_spread_over_the_pool_and_send_back_results_and_errors():
    kabudachi.configure(processes=2, concurrency=1)

    async def main():
        first, second = await asyncio.gather(
            pool_tasks.where_async(Greeting(text="0.5")),
            pool_tasks.where_sync(Greeting(text="0.5")),
        )
        # Far larger than a pipe's buffer, so it crosses in many writes.
        large = await pool_tasks.sized(Greeting(times=4 * 1024 * 1024))
        with pytest.raises(ValueError, match="refused this"):
            await pool_tasks.refuses(Greeting(text="this"))
        return first.times, second.times, len(large.text)

    first, second, size = kabudachi.run(main)

    assert os.getpid() not in (first, second)
    assert first != second, "with one place per process, the second body went to the other one"
    assert size == 4 * 1024 * 1024


def declare_in_script_a_task():
    def scripted(request: Greeting) -> Greeting:
        return request

    scripted.__module__ = "__main__"
    kabudachi.task(name="tests.scripted")(scripted)


def declare_in_script_a_hook():
    def scripted_hook(context):
        pass

    scripted_hook.__module__ = "__main__"
    kabudachi.before_run(scripted_hook)


@pytest.mark.parametrize(
    "declare, named",
    [(declare_in_script_a_task, "tests.scripted"), (declare_in_script_a_hook, "scripted_hook")],
    ids=["task", "hook"],
)
def test_a_task_or_hook_declared_in_the_script_being_run_is_refused_before_anything_starts(
    declare, named
):
    declare()
    kabudachi.configure(processes=1)

    with pytest.raises(TaskDefinitionError, match=named):
        kabudachi.run(nothing)


def declare_only_here():
    @kabudachi.task(name="tests.only_here")
    def only_here(request: Greeting) -> Greeting:
        return request


def declare_hook_only_here():
    kabudachi.configure(imports=["pool_tasks"])

    @kabudachi.before_run
    def noted(context):
        pass


@pytest.mark.parametrize(
    "arrange, reason",
    [
        (
            functools.partial(kabudachi.configure, imports=["no_such_module_for_kabudachi_tests"]),
            "no_such_module_for_kabudachi_tests: ModuleNotFoundError",
        ),
        (declare_only_here, "tests.only_here is missing"),
        (
            functools.partial(
                kabudachi.configure,
                imports=["pool_slow_import"],
                process_start_timeout=timedelta(seconds=3),
            ),
            r"kabudachi-task-0 was not ready within 3 s while importing pool_slow_import",
        ),
        (declare_hook_only_here, "before_run hook test_pool.declare_hook_only_here.<locals>.noted is missing"),
        (
            lambda: pool_tasks.marker("process-init-fails").touch(),
            "process_init hook pool_tasks.open_resources raised RuntimeError: resources unavailable",
        ),
    ],
    ids=[
        "module_fails_to_import",
        "task_only_in_the_worker",
        "import_never_finishes",
        "hook_only_in_the_worker",
        "process_init_raises",
    ],
)
def test_task_processes_that_cannot_find_every_task_stop_the_start_with_the_reason(arrange, reason):
    kabudachi.configure(processes=1)
    arrange()

    with pytest.raises(StartupError, match=reason):
        kabudachi.run(nothing)


def test_a_long_chain_is_compacted_in_a_task_process(markers):
    kabudachi.configure(processes=1, concurrency=2, memory_soft_limit=400, memory_hard_limit=100_000)
    texts = [letter * 60 for letter in "abcdefgh"]

    async def main():
        held = pool_tasks.refresh(Greeting(text="holder"))
        while not (markers / "holding").exists():
            await asyncio.sleep(0.01)
        handles = [pool_tasks.refresh(Greeting(text=text)) for text in texts]
        return (await held).text, (await handles[-1]).text

    held, newest = kabudachi.run(main)

    assert held == "held until folded", "a compaction folded while the key was busy"
    assert int((markers / "folded-in").read_text()) != os.getpid(), "the merge ran in a task process"
    assert newest == functools.reduce(lambda older, newer: f"({older}>{newer})", texts)


def test_a_body_in_a_task_process_calls_tasks_flows_and_groups_and_gives_its_place_back_while_it_waits(
    markers,
):
    # One place in all: the called tasks can only run while the caller waits.
    kabudachi.configure(processes=1, concurrency=1)

    async def main():
        return await pool_tasks.calls_others(Greeting(times=0))

    result = kabudachi.run(main)

    assert result.times == 1 + 2 + 11 + 21
    assert result.text == "True cancelled ValueError UnknownTaskError"
    assert (markers / "called-back").read_text() == "101", "the task process ran the callback out"


def test_a_body_whose_process_dies_is_replayed_in_a_replacement_unless_its_task_is_ephemeral():
    kabudachi.configure(processes=1)

    async def main():
        async with asyncio.timeout(30):
            replayed = await pool_tasks.dies_once(Greeting(text="a"))
            with pytest.raises(TaskLostError):
                await pool_tasks.ephemeral_dies(Greeting())
            after = await pool_tasks.where_async(Greeting())
            return replayed.times, after.times

    replayed, after = kabudachi.run(main)

    assert os.getpid() not in (replayed, after)
    assert replayed != after, "the ephemeral body's death replaced the process again"


def test_a_body_past_its_hard_limit_settles_at_once_and_its_process_is_replaced_once_its_neighbour_finishes():
    kabudachi.configure(processes=1, concurrency=3)

    async def main():
        async with asyncio.timeout(30):
            loop = asyncio.get_running_loop()
            neighbour = pool_tasks.where_async(Greeting(text="1.5"))
            retried = pool_tasks.stubborn_then_quick(Greeting(text="r"))
            started = loop.time()
            with pytest.raises(TaskTimeoutError):
                await pool_tasks.stubborn(Greeting())
            failed_after = loop.time() - started
            return failed_after, await neighbour, await retried

    failed_after, neighbour, retried = kabudachi.run(main)

    # Settled at its hard limit (0.3 s + 0.2 s), not when the process died.
    assert failed_after < 1.2
    # No collateral kill: the neighbour shared the condemned process and still
    # returned, from a different process than the retry, which ran in the
    # replacement.
    assert retried.times != neighbour.times, "the retry ran in the replacement"
    assert float(retried.text) >= float(neighbour.text), "the retry waited for the old body's exit"


def test_a_cancelled_body_stops_when_asked_and_one_that_will_not_costs_its_process(markers):
    kabudachi.configure(processes=1, concurrency=1)

    async def main():
        async with asyncio.timeout(30):
            handle = pool_tasks.cancellable(Greeting(text="c"))
            while not (markers / "started-c").exists():
                await asyncio.sleep(0.01)
            assert handle.cancel()
            with pytest.raises(TaskCancelledError):
                await handle
            kept = (await pool_tasks.where_async(Greeting())).times
            racing = []
            for _ in range(20):
                racing.append(pool_tasks.where_async(Greeting()))
                await asyncio.sleep(0.002)
                racing[-1].cancel()
            outcomes = await asyncio.gather(*racing, return_exceptions=True)
            stuck = pool_tasks.ignores_cancel(Greeting())
            while not (markers / "ignoring").exists():
                await asyncio.sleep(0.01)
            stuck.cancel()
            # Its process's one place stays taken until the process is gone,
            # so the next body can only run in the replacement.
            replaced = (await pool_tasks.where_async(Greeting())).times
            return kept, outcomes, replaced

    kept, outcomes, replaced = kabudachi.run(main)

    assert (markers / "cancelled-c").exists(), "the body was cancelled in its process"
    assert kept == int((markers / "started-c").read_text()), "a body that stopped cost nothing"
    # A cancel racing a result in flight: each handle ends once, either way.
    assert all(isinstance(outcome, (Greeting, TaskCancelledError)) for outcome in outcomes), outcomes
    assert replaced != kept, "a body that ignored the cancel past its grace cost its process"


def test_a_run_handed_over_by_a_shard_still_running_at_its_abort_deadline_is_killed_with_its_process_and_lost(
    markers,
):
    kabudachi.configure(processes=1, concurrency=1)
    configuration = config_module.process_configuration()
    settings = configuration.settings()
    registry = registry_module.default_registry()
    hooks = lifecycle_module.default_hooks()
    seconds_left = 0.5

    async def main():
        pool = ProcessPool(settings, task_modules(registry, hooks, settings.imports), registry, hooks)
        native = _native.NativeRuntime("worker", "incarnation")
        runtime = FaultingRuntime(native)
        # Runs a shard's leader hands this worker, which settle no handle here.
        runtime.delivers_results = False
        serving = None
        await pool.start()
        try:
            async with asyncio.timeout(30):
                await native.wait_until_leader()
                session = Session(runtime, registry, process_serializers(), configuration, executor=pool)
                serving = asyncio.ensure_future(session.serve())
                session.submit(pool_tasks.outlives_its_deadline.definition, Greeting())
                while not (markers / "outliving").exists():
                    await asyncio.sleep(0.01)
                [run_id] = [event[1] for event in runtime.events if event[0] == "started"]
                # As the leader would once this worker lost contact with it.
                runtime.inject_abort(run_id, seconds_left)
                aborted = time.monotonic()
                while ("lost", run_id) not in runtime.events:
                    await asyncio.sleep(0.01)
                return time.monotonic() - aborted, list(runtime.events)
        finally:
            if serving is not None:
                serving.cancel()
                await asyncio.gather(serving, return_exceptions=True)
            await pool.stop(kill=True)
            native.shutdown()

    lost, events = asyncio.run(main())

    # Killed at the deadline, not when its 3 s cancel grace ran out.
    assert seconds_left <= lost < seconds_left + 1.5, lost
    assert [event[0] for event in events] == ["started", "lost"], events
    assert (markers / "asked to stop").exists(), "the body was asked to stop first"
    with pytest.raises(ProcessLookupError):
        os.kill(int((markers / "outliving").read_text()), 0)


def test_a_task_process_is_replaced_after_its_run_limit_and_after_a_recycling_task_once_its_run_finishes():
    kabudachi.configure(processes=1, concurrency=1, max_runs_per_process=3)
    most_alive = 0

    async def count_task_processes():
        nonlocal most_alive
        while True:
            most_alive = max(most_alive, len(multiprocessing.active_children()))
            await asyncio.sleep(0.005)

    async def main():
        async with asyncio.timeout(30):
            counting = asyncio.create_task(count_task_processes())
            pids = [(await pool_tasks.where_async(Greeting())).times for _ in range(4)]
            pids.append((await pool_tasks.recycles(Greeting())).times)
            pids.append((await pool_tasks.where_async(Greeting())).times)
            counting.cancel()
            return pids

    pids = kabudachi.run(main)

    # The recycling run returning the second process's id shows the drain was
    # graceful: a killed process would have lost it, and its replay would
    # have reported the third.
    first, second, third = pids[0], pids[3], pids[5]
    assert pids == [first] * 3 + [second] * 2 + [third], pids
    assert len({first, second, third, os.getpid()}) == 4
    # A draining process still counts: its replacement starts only once it
    # has exited, so there was never more than one task process.
    assert most_alive == 1


def test_lifecycle_hooks_prepare_each_run_in_its_task_process_and_a_failed_cleanup_replaces_it(markers):
    kabudachi.configure(processes=1, concurrency=1)

    async def main():
        async with asyncio.timeout(30):
            sync = await pool_tasks.hooked(Greeting())
            in_loop = await pool_tasks.hooked_async(Greeting())
            retried = await pool_tasks.refused_once(Greeting())
            with pytest.raises(LookupError, match="pool.refused is not ready"):
                await pool_tasks.refused(Greeting())
            await pool_tasks.where_async(Greeting())  # not on the hooks' queue
            (markers / "process-init-fails").touch()
            spoiled = await pool_tasks.hooked(Greeting(text="spoil"))
            while not (markers / "process-init-failures").exists():
                await asyncio.sleep(0.05)
            (markers / "process-init-fails").unlink()
            after = await pool_tasks.hooked(Greeting())
            return sync, in_loop, retried, spoiled, after

    sync, in_loop, retried, spoiled, after = kabudachi.run(main)

    assert sync.times != os.getpid()
    assert (sync.text, in_loop.text) == ("pool.hooked 1 True", "pool.hooked_async 1 True")
    assert retried.text == "2", "the failing before_run hook failed the first attempt, which was retried"
    assert (spoiled.text, spoiled.times) == ("spoil", sync.times), "the result stood"
    assert after.times not in (sync.times, os.getpid()), "the failed cleanup replaced the process"
    assert after.text == "pool.hooked 1 True", "a later start whose process_init failed was tried again"
    assert (markers / "hooks.log").read_text().splitlines() == [
        "before pool.hooked 1",
        "after pool.hooked 1 Greeting",
        "before pool.hooked_async 1",
        "after pool.hooked_async 1 Greeting",
        "before pool.refused_once 1",
        "after pool.refused_once 1 LookupError",
        "before pool.refused_once 2",
        "after pool.refused_once 2 Greeting",
        "before pool.refused 1",
        "after pool.refused 1 LookupError",
        "before pool.hooked 1",
        "after pool.hooked 1 Greeting",
        "before pool.hooked 1",
        "after pool.hooked 1 Greeting",
    ]


def test_a_body_raising_system_exit_or_keyboard_interrupt_fails_its_run_and_costs_its_process_after_its_neighbour(
    markers,
):
    kabudachi.configure(processes=1, concurrency=4)

    async def main():
        async with asyncio.timeout(30):
            neighbour = pool_tasks.steady(Greeting(text="n"))
            while not (markers / "steady-n").exists():
                await asyncio.sleep(0.01)
            outcomes = await asyncio.gather(
                pool_tasks.leaves(Greeting(text="SystemExit")),
                # On the loop of a process that ignores SIGINT: no Ctrl-C.
                pool_tasks.leaves_async(Greeting(text="KeyboardInterrupt")),
                return_exceptions=True,
            )
            finished = (await neighbour).times
            after = (await pool_tasks.where_async(Greeting())).times
            return outcomes, finished, after

    outcomes, neighbour, after = kabudachi.run(main)

    assert [(type(outcome), getattr(outcome, "kind", None)) for outcome in outcomes] == [
        (TaskBodyError, "SystemExit"),
        (TaskBodyError, "KeyboardInterrupt"),
    ], outcomes
    assert neighbour != os.getpid(), "the neighbour finished where it started"
    assert after not in (neighbour, os.getpid()), "the process was condemned and replaced"
