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
from kabudachi.errors import StartupError, TaskBodyError, TaskDefinitionError
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
                imports=["pool_tasks", "pool_slow_import"],
                process_start_timeout=timedelta(seconds=1),
            ),
            r"kabudachi-task-0 was not ready within 1 s while importing pool_slow_import",
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
