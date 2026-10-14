"""One acceptance scenario covering the Phase 6 shipping gate on real workers."""

import asyncio
import inspect
import multiprocessing
import os
import shutil
import subprocess
import sys
import time
import venv
from datetime import timedelta
from importlib import metadata
from pathlib import Path

import pytest

import kabudachi
from kabudachi import config as config_module
from kabudachi._native import RunState
from kabudachi.config import Configuration
from kabudachi.errors import (
    ReduceEmptyError,
    RuntimeNotStartedError,
    SerializationError,
    TaskCancelledError,
    TaskBodyError,
    TaskFailure,
    TaskSupersededError,
    TaskExpiredError,
)
from kabudachi.session import active_session
from cluster import Cluster
from functional_tasks import (
    FileBackend, Number, Order, concatenate, double, gated_concatenate, items, migrated, nothing,
    ordered, receipt, retry, returned_bound, returned_group, subtract, timed_out,
    unsubscribed,
)
from networked_tasks import echo, gather, hold, join, load, pipeline, plan, refuse, shout
from proto_messages import Greeting


def installed_scenario(root: Path) -> None:
    programs = root / "programs"
    programs.mkdir()
    for name in (
        "test_functionality.py", "functional_tasks.py", "cluster.py",
        "networked_tasks.py", "networked_worker.py", "proto_messages.py",
    ):
        shutil.copyfile(Path(__file__).with_name(name), programs / name)
    venv.EnvBuilder(symlinks=True, with_pip=False).create(root / "venv")
    python = root / "venv" / "bin" / "python"
    environment = {
        **os.environ,
        "PYTHONPATH": str(programs),
        "KABUDACHI_TEST_INSTALLED": "1",
    }

    def run(*arguments: str, timeout: int = 480) -> str:
        completed = subprocess.run(
            [str(python), *arguments], cwd=programs, env=environment,
            capture_output=True, text=True, timeout=timeout,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
        return completed.stdout

    run("-m", "ensurepip", "--default-pip")
    site = Path(run("-c", "import sysconfig; print(sysconfig.get_path('purelib'))").strip())
    dependencies = ("asgiref", "wrapt", "protobuf", "pytest", "pluggy", "iniconfig", "packaging", "pygments")
    (site / "test_dependencies.pth").write_text("".join(
        f"{metadata.distribution(name).locate_file('')}\n" for name in dependencies
    ))
    wheel = Path(os.environ["TEST_SRCDIR"], os.environ["WHEEL_FILE"])
    run("-m", "pip", "--disable-pip-version-check", "install", "--no-index", "--no-deps", str(wheel))
    run("-m", "pip", "--disable-pip-version-check", "check")
    scenario = root / "scenario"
    scenario.mkdir()
    program = '''
from pathlib import Path
import sys
import pytest
import kabudachi
from kabudachi import _native
import test_functionality
site = Path(sys.argv[2]).resolve()
assert Path(kabudachi.__file__).resolve().is_relative_to(site), kabudachi.__file__
assert Path(_native.__file__).resolve().is_relative_to(site), _native.__file__
with pytest.MonkeyPatch.context() as patch:
    test_functionality.test_the_phase6_library_works_for_an_independent_caller_across_workers_and_losses(Path(sys.argv[1]), patch)
print("Installed-wheel Phase 6 acceptance scenario passed", flush=True)
'''
    print(run("-c", program, str(scenario), str(site)), end="")


async def wait_for(description, probe, timeout=60):
    deadline = time.monotonic() + timeout
    while not (value := probe()):
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for {description}")
        await asyncio.sleep(0.02)
    return value


async def finish(handle, timeout, cluster):
    try:
        return await asyncio.wait_for(handle, timeout)
    except TimeoutError:
        print("timed out:", handle.task_id, await handle.status(), flush=True)
        for task in asyncio.all_tasks():
            if "_send_members" in str(task.get_coro()):
                task.print_stack()
        for worker in cluster.live():
            print("worker", worker.pid, worker.error_lines()[-20:], flush=True)
        raise


def new_stage(markers, before, cluster, exclude=()):
    for marker in set(markers.glob("reduce-start.*")) - before:
        holder = next(worker for worker in cluster.workers if worker.pid == int(marker.read_text()))
        if holder not in exclude:
            return holder
    return None


async def certified_records(markers, before, runtime):
    task_ids = {
        marker.read_text()
        for marker in set(markers.glob("claimed.*")) - before
    }
    records = []
    for task_id in task_ids:
        record = await runtime.task_record(task_id)
        assert record is not None and record.finished, task_id
        assert sum(run.state == RunState.SUCCEEDED for run in record.runs) == 1, task_id
        records.append(record)
    return records


def test_the_phase6_library_works_for_an_independent_caller_across_workers_and_losses(tmp_path, monkeypatch):
    if os.environ.get("KABUDACHI_TEST_INSTALLED") != "1":
        installed_scenario(tmp_path)
        return
    backend = tmp_path / "results"
    backend.mkdir()
    configuration = Configuration()
    configuration.configure(
        processes=0,
        heartbeat_interval=timedelta(milliseconds=200),
        heartbeat_timeout=timedelta(seconds=3),
        reconnect_timeout=timedelta(seconds=6),
        memory_soft_limit=4_000_000,
        memory_hard_limit=16_000_000,
        result_backend=FileBackend(backend),
    )
    monkeypatch.setattr(config_module, "_process_configuration", configuration)
    assert not inspect.signature(kabudachi.run).parameters
    with pytest.raises(TypeError):
        kabudachi.run(lambda: None)
    with pytest.raises(RuntimeNotStartedError):
        echo(Greeting(text="no connection"))
    assert callable(join.reduce)
    with pytest.raises(TypeError):
        kabudachi.flow(echo, receipt)

    # Five voters leave a quorum after losing a follower and then the leader.
    # These programs call the production entry point without any argument.
    cluster = Cluster.start(tmp_path, size=5, overrides={
        index: {
            "KABUDACHI_TEST_TASKS": "functional_tasks",
            # Five workers share CI's two CPUs. Keep the failure detector
            # past ordinary scheduling delays while this test floods records.
            "KABUDACHI_HEARTBEAT_INTERVAL": "0.2",
            "KABUDACHI_HEARTBEAT_TIMEOUT": "3",
            "KABUDACHI_RECONNECT_TIMEOUT": "6",
            "KABUDACHI_MEMORY_SOFT_LIMIT": "4000000",
            "KABUDACHI_TEST_STAGE_SECONDS": "0.4",
            "KABUDACHI_TEST_BACKEND": str(backend),
            "KABUDACHI_TEST_SCHEMA": "v3",
            "KABUDACHI_QUEUES": "default,priority",
        }
        for index in range(5)
    })

    async def scenario():
        children = {process.pid for process in multiprocessing.active_children()}
        async with kabudachi.client(seeds=[worker.address for worker in cluster.workers]):
            session = active_session()
            runtime = session.runtime
            assert {process.pid for process in multiprocessing.active_children()} == children
            assert cluster.leader().voters() == 5
            assert runtime.worker_id() not in {worker.id for worker in cluster.workers}
            assert runtime.voters() == 0
            assert len(list(tmp_path.glob("initialized.*"))) == 5

            called = echo(Greeting(text="remote"))
            callback = []
            called.callback(callback.append)
            await called.durable(kabudachi.Durability.REPLICATED)
            assert await called == Greeting(text="remote")
            await wait_for("the certified callback", lambda: callback)
            assert callback == [Greeting(text="remote")]
            status = await called.status()
            assert status.state == "succeeded" and status.delivered
            assert await kabudachi.resume(called.task_id) == Greeting(text="remote")
            assert await nothing(Greeting()) is None
            assert await double(Number(21)) == Number(42)
            assert await migrated(Order(quantity=21, note="dropped")) == Number(42)
            with pytest.raises(TaskExpiredError):
                await unsubscribed.options(expires=timedelta(milliseconds=200))(Greeting())
            with pytest.raises(SerializationError):
                echo(Number(1))
            with pytest.raises(TaskBodyError, match="refused remote") as failed:
                await refuse(Greeting(text="remote"))
            assert failed.value.kind == "ValueError"
            assert await retry(Greeting(text="retry-e2e")) == Greeting(text="retry-e2e")
            try:
                await timed_out(Greeting())
            except Exception as error:
                assert getattr(error, "kind", type(error).__name__) == "TaskTimeoutError"
            else:
                raise AssertionError("a timed-out task succeeded")

            large = Greeting(text="blob" * 300_000)
            assert await echo(large) == large
            bound = echo.bind(text="bound")
            assert await bound(Greeting(times=7)) == Greeting(text="bound", times=7)
            flowed = await kabudachi.flow(load, shout)(Greeting(text="a"))
            assert [value.text for value in flowed] == ["loaded a", "LOADED A"]
            grouped = await kabudachi.group(
                ordered.bind(Greeting(text="first", times=200)),
                ordered.bind(Greeting(text="second", times=10)),
            )()
            assert [value.text for value in grouped] == ["first", "second"]
            inputs = [Greeting(text=f"mapped-{index}", times=(8-index)*20) for index in range(8)]
            mapped = ordered.map(inputs)
            await wait_for("map members on different workers", lambda: len({
                marker.read_text() for marker in tmp_path.glob("mapped-*.started")
            }) >= 2)
            (tmp_path / "mapped.go").touch()
            values = await mapped
            assert [value.text for value in values] == [f"mapped-{index}" for index in range(8)]
            assert len({value.times for value in values}) >= 2
            assert (await mapped.status()).state == "succeeded"
            pages = echo.map([Greeting(text=f"{index},") for index in range(1100)])
            paged = await finish(pages, 180, cluster)
            assert [value.text for value in paged] == [f"{index}," for index in range(1100)]
            collected = await kabudachi.group(echo, refuse, on_error="collect_all")(Greeting(text="collect"))
            assert collected[0] == Greeting(text="collect")
            assert isinstance(collected[1], TaskFailure) and collected[1].kind == "ValueError"
            with pytest.raises(TaskBodyError):
                await kabudachi.group(echo, refuse)(Greeting(text="fail fast"))
            assert [value.text for value in await plan(Greeting(text="a"))] == ["loaded a", "LOADED A"]
            assert [value.text for value in await returned_group(Greeting(text="a"))] == ["a", "loaded a"]
            assert await returned_bound(Greeting(text="bound continuation")) == [Greeting(text="bound continuation")]
            assert [value.text for value in await pipeline(Greeting(text="a"))] == [
                "LOADED A", "loaded a", "A", "loaded a", "LOADED A", "abcde", "loaded aA", "ValueError: refused a",
            ]

            letters = [Greeting(text=letter) for letter in "abcd"]
            assert (await subtract.reduce(letters, initial=Greeting(text="s"))).text == "((((s-a)-b)-c)-d)"
            assert (await join.reduce(letters, chunk_size=2)).text == "abcd"
            assert await subtract.reduce([], initial=Greeting(text="seed")) == Greeting(text="seed")
            assert await join.reduce([Greeting(text="one")]) == Greeting(text="one")
            assert (await concatenate.reduce(paged, chunk_size=64)).text == "".join(value.text for value in paged)
            with pytest.raises(ReduceEmptyError):
                join.reduce([])
            staged = await kabudachi.flow(items, echo.map, join.reduce)(Greeting(text="abcd"))
            assert staged[-1].text == "abcd"

            old = gather.options(key="acceptance", delay=timedelta(seconds=1))(Greeting(text="old"))
            new = gather.options(key="acceptance", delay=timedelta(seconds=1))(Greeting(text="new"))
            with pytest.raises(TaskSupersededError):
                await old
            assert (await new).text == "old+new"
            cancelled = hold(Greeting(text="cancel-e2e", times=-1))
            await wait_for("a cancellable body to start", lambda: list(tmp_path.glob("cancel-e2e.[0-9]*")))
            assert cancelled.cancel()
            with pytest.raises(TaskCancelledError):
                await cancelled

            before = set(tmp_path.glob("reduce-start.*"))
            claims = set(tmp_path.glob("claimed.*"))
            leader = cluster.leader()
            reducing = gated_concatenate.reduce(paged, chunk_size=64)
            victim = await wait_for("a reducer on a follower", lambda: new_stage(tmp_path, before, cluster, (leader,)))
            victim.kill()
            (tmp_path / "reduce.go").touch()
            assert (await finish(reducing, 120, cluster)).text == "".join(value.text for value in paged)
            records = await certified_records(tmp_path, claims, runtime)
            assert records and any(any(run.state == RunState.LOST for run in record.runs) for record in records)

            before = set(tmp_path.glob("shout.*"))
            claims = set(tmp_path.glob("claimed.*"))
            continuing = plan(Greeting(text="recovered"))
            await wait_for("a returned continuation to start", lambda: set(tmp_path.glob("shout.*")) - before)
            leader.kill()
            recovered = await finish(continuing, 60, cluster)
            assert [value.text for value in recovered] == ["loaded recovered", "LOADED RECOVERED"]
            await certified_records(tmp_path, claims, runtime)
            assert await echo(Greeting(text="after election")) == Greeting(text="after election")
            assert await kabudachi.resume(continuing.task_id) == recovered
            assert await kabudachi.resume(pages.task_id) == paged
            assert session.decode_resolved(await runtime.composite_results(pages.task_id)) == paged
            assert runtime.worker_id() not in {worker.id for worker in cluster.live()}
            assert len(cluster.live()) == 3
        assert active_session() is None
        assert {process.pid for process in multiprocessing.active_children()} == children
        async with kabudachi.client(seeds=[worker.address for worker in cluster.live()]):
            assert active_session().runtime.worker_id() != runtime.worker_id()
            assert await kabudachi.resume(called.task_id) == Greeting(text="remote")
            assert await kabudachi.resume(pages.task_id) == paged
            assert await echo(Greeting(text="new caller")) == Greeting(text="new caller")

    try:
        asyncio.run(asyncio.wait_for(scenario(), 420))
        # A caller disconnect changes no worker membership and leaves workers
        # available for a later caller. Workers drain through SIGTERM.
        for worker in cluster.live():
            assert worker.stop() == 0, worker.error_lines()
    finally:
        cluster.close()
