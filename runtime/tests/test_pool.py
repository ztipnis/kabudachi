"""Task bodies in task processes, through `kabudachi.run()`: where they run,
what crosses the pipe, nested calls, a process that dies, a body past its
hard limit, a cancel, and compaction. Each test starts one or two task
processes, which import their tasks from `pool_tasks`."""

import asyncio
import functools
import multiprocessing
import os
import struct
from datetime import timedelta

import pytest

import kabudachi
import pool_tasks
from kabudachi import config as config_module
from kabudachi import ipc
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
from kabudachi.registry import TaskRegistry
from proto_messages import Greeting


@pytest.fixture(autouse=True)
def markers(monkeypatch, tmp_path):
    """Fresh settings, the pool tasks alone in the registry, and a directory
    the test shares with its task processes."""
    registry = TaskRegistry()
    for definition in registry_module.default_registry().definitions():
        registry.register(definition)
    monkeypatch.setattr(registry_module, "_default_registry", registry)
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
        # Not a cancel anyone asked for, so the run fails like any other error.
        with pytest.raises(TaskBodyError, match="CancelledError") as raised:
            await pool_tasks.cancels_itself(Greeting())
        return first.times, second.times, len(large.text), raised.value.kind

    first, second, size, kind = kabudachi.run(main)

    assert os.getpid() not in (first, second)
    assert first != second, "with one place per process, the second body went to the other one"
    assert size == 4 * 1024 * 1024
    assert kind == "CancelledError"


def test_a_task_declared_in_the_script_being_run_is_refused_before_anything_starts():
    def scripted(request: Greeting) -> Greeting:
        return request

    scripted.__module__ = "__main__"
    kabudachi.task(name="tests.scripted")(scripted)
    kabudachi.configure(processes=1)

    with pytest.raises(TaskDefinitionError, match="tests.scripted"):
        kabudachi.run(nothing)


def declare_only_here():
    @kabudachi.task(name="tests.only_here")
    def only_here(request: Greeting) -> Greeting:
        return request


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
    ],
    ids=["module_fails_to_import", "task_only_in_the_worker", "import_never_finishes"],
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
