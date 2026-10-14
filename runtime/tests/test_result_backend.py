"""Result backends: a Python object with async `store` and `load` keeps the
contract every backend keeps, and a call it cannot answer is an error, not a
hang."""

import asyncio
import contextlib
import logging
import threading
import time

import _kabudachi_contract
import pytest

import kabudachi
from kabudachi import _native
from kabudachi import config as config_module
from kabudachi import registry as registry_module
from kabudachi.config import Configuration
from kabudachi.errors import ResultUnavailableError, TaskBodyError
from kabudachi.flow import Flow, flow
from kabudachi.native_protocol import NetworkedArguments, OneNodeArguments, SubmitArguments
from kabudachi.options import SubmissionOptions, SubmissionSettings
from kabudachi.registry import TaskRegistry
from proto_messages import Greeting


class DictBackend:
    """The plainest backend: a dict, ignoring expiry, which the wrapper enforces."""

    def __init__(self):
        self.entries = {}

    async def store(self, key, entry, expires_at):
        self.entries[key] = entry

    async def load(self, key):
        return self.entries.get(key)


class HangingBackend:
    async def store(self, key, entry, expires_at):
        await asyncio.sleep(60)

    async def load(self, key):
        await asyncio.sleep(60)


class TimingOutBackend:
    """A backend whose own client timed out."""

    async def store(self, key, entry, expires_at):
        raise TimeoutError("the server did not reply")

    async def load(self, key):
        raise TimeoutError("the server did not reply")


@contextlib.contextmanager
def running_loop():
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join()
        loop.close()


def test_a_python_object_with_async_store_and_load_keeps_the_result_backend_contract():
    with running_loop() as loop:
        _kabudachi_contract.check_result_backend(DictBackend, loop)


def test_a_call_the_backend_does_not_answer_is_unavailable_and_one_it_raises_is_refused():
    with running_loop() as loop:
        hung = _kabudachi_contract.load_error(HangingBackend, loop, 200)
        raised = _kabudachi_contract.load_error(TimingOutBackend, loop, 5000)

        async def on_the_loop():
            return _kabudachi_contract.load_error(DictBackend, loop, 3000)

        # The adapter would wait for the very loop it blocks, so it refuses at once.
        on_loop = asyncio.run_coroutine_threadsafe(on_the_loop(), loop).result(timeout=2)

    assert "did not answer" in hung
    assert "refused" in raised and "the server did not reply" in raised
    assert "event loop" in on_loop


class RefusingBackend:
    async def store(self, key, entry, expires_at):
        raise ConnectionError("the backend is down")

    async def load(self, key):
        return None


async def run_one_task(native, result, *, result_ttl_ms=None):
    """Submits a task, runs it to `result` and returns its id."""
    task_id = native.submit(
        SubmitArguments(
            "billing.charge",
            1,
            b"input",
            "task",
            None,
            SubmissionOptions(),
            SubmissionSettings("default", None),
            result_ttl_ms=result_ttl_ms,
        )
    )
    [claimed] = await asyncio.wait_for(native.claim_pending(1), 5)
    native.report_started(claimed.task_run_id)
    native.complete(claimed.task_run_id, _native.result_digest(result), result=result)
    return task_id


async def persistence_once_settled(native, task_id):
    """The task's record and its newest run's persistence, once that says how
    mirroring went."""
    deadline = time.monotonic() + 5
    while True:
        record = await native.task_record(task_id)
        persistence = record.runs[-1].persistence if record is not None and record.runs else None
        if persistence in ("disabled", "stored", "failed") or time.monotonic() > deadline:
            return record, persistence
        await asyncio.sleep(0.02)


def test_a_certified_result_is_mirrored_recorded_stored_and_resumed_from_the_backend():
    backend = DictBackend()
    with running_loop() as loop:
        native = _native.NativeRuntime(
            "worker-1", "incarnation-1", OneNodeArguments(), _native.PythonResultBackend(backend, loop)
        )

        async def main():
            await native.wait_until_leader()
            task_id = await run_one_task(native, b"the-result")
            _, persistence = await persistence_once_settled(native, task_id)
            return task_id, persistence, await native.resume_task(task_id)

        try:
            task_id, persistence, resumed = asyncio.run(main())
        finally:
            native.shutdown()

    assert persistence == "stored"
    assert f"task/{task_id}" in backend.entries
    assert (resumed.state, resumed.payload, resumed.definition_id) == ("succeeded", b"the-result", "billing.charge")
    assert resumed.persistence == "stored"


def test_a_task_that_keeps_no_result_is_never_mirrored():
    backend = DictBackend()
    with running_loop() as loop:
        native = _native.NativeRuntime(
            "worker-1", "incarnation-1", OneNodeArguments(), _native.PythonResultBackend(backend, loop)
        )

        async def main():
            await native.wait_until_leader()
            task_id = await run_one_task(native, b"the-result", result_ttl_ms=0)
            _, persistence = await persistence_once_settled(native, task_id)
            return persistence, await native.resume_task(task_id)

        try:
            persistence, resumed = asyncio.run(main())
        finally:
            native.shutdown()

    assert persistence == "disabled"
    assert backend.entries == {}
    assert resumed.state == "succeeded" and resumed.payload is None
    assert "no entry" in resumed.unavailable


def test_a_backend_that_fails_is_logged_and_leaves_the_run_succeeded():
    records = []
    handler = logging.Handler()
    handler.emit = records.append
    logger = logging.getLogger("kabudachi.result_backend")
    logger.addHandler(handler)
    with running_loop() as loop:
        native = _native.NativeRuntime(
            "worker-1", "incarnation-1", OneNodeArguments(), _native.PythonResultBackend(RefusingBackend(), loop)
        )

        async def main():
            await native.wait_until_leader()
            task_id = await run_one_task(native, b"the-result")
            return await persistence_once_settled(native, task_id)

        try:
            record, persistence = asyncio.run(main())
        finally:
            native.shutdown()
            logger.removeHandler(handler)

    assert persistence == "failed"
    assert record.runs[-1].state == _native.RunState.SUCCEEDED
    assert any("the backend is down" in record.getMessage() for record in records)


def test_a_networked_worker_mirrors_what_its_leader_certified_and_the_shard_says_stored():
    backend = DictBackend()
    with running_loop() as loop:
        native = _native.NetworkedRuntime(
            NetworkedArguments(
                "results",
                "/ip4/127.0.0.1/tcp/0",
                heartbeat_interval_ms=100,
                heartbeat_timeout_ms=1000,
                reconnect_timeout_ms=2000,
            ),
            _native.PythonResultBackend(backend, loop),
        )

        async def main():
            await asyncio.wait_for(native.wait_until_ready(), 5)
            task_id = await run_one_task(native, b"the-result")
            _, persistence = await persistence_once_settled(native, task_id)
            return task_id, persistence, await native.resume_task(task_id)

        try:
            task_id, persistence, resumed = asyncio.run(main())
        finally:
            native.shutdown()

    assert persistence == "stored"
    assert f"task/{task_id}" in backend.entries
    assert (resumed.state, resumed.payload) == ("succeeded", b"the-result")


class ForgetfulBackend(DictBackend):
    """A backend that keeps nothing while `remembering` is false."""

    remembering = False

    async def store(self, key, entry, expires_at):
        if self.remembering:
            await super().store(key, entry, expires_at)


def test_a_result_the_backend_lost_is_resumed_from_the_shards_blobs_and_mirrored_again():
    # More than fits one message, so the worker stores it in the shard as blobs.
    result = bytes(index % 251 for index in range(1_100_000))
    backend = ForgetfulBackend()
    with running_loop() as loop:
        native = _native.NetworkedRuntime(
            NetworkedArguments(
                "results",
                "/ip4/127.0.0.1/tcp/0",
                heartbeat_interval_ms=100,
                heartbeat_timeout_ms=1000,
                reconnect_timeout_ms=2000,
            ),
            _native.PythonResultBackend(backend, loop),
        )

        async def main():
            await asyncio.wait_for(native.wait_until_ready(), 5)
            task_id = await run_one_task(native, result)
            await persistence_once_settled(native, task_id)
            lost = dict(backend.entries)
            backend.remembering = True
            resumed = await native.resume_task(task_id)
            deadline = time.monotonic() + 5
            while f"task/{task_id}" not in backend.entries and time.monotonic() < deadline:
                await asyncio.sleep(0.02)
            return task_id, lost, resumed

        try:
            task_id, lost, resumed = asyncio.run(main())
        finally:
            native.shutdown()

    assert lost == {}
    assert (resumed.state, resumed.payload, resumed.definition_id) == ("succeeded", result, "billing.charge")
    assert f"task/{task_id}" in backend.entries


class FailingBackend(DictBackend):
    async def store(self, key, entry, expires_at):
        raise ConnectionError("the store is down")


async def eventually_status(handle, persistence):
    for _ in range(200):
        status = await handle.status()
        if status.persistence == persistence:
            return status
        await asyncio.sleep(0.01)
    raise AssertionError(f"{handle.task_id} never reached persistence {persistence!r}: {status}")


def test_a_run_mirrors_each_result_once_certified_and_resumes_it_from_the_backend(monkeypatch, caplog):
    monkeypatch.setattr(registry_module, "_default_registry", TaskRegistry())
    backend = DictBackend()
    configuration = Configuration()
    configuration.configure(processes=0, result_backend=backend)
    monkeypatch.setattr(config_module, "_process_configuration", configuration)

    @kabudachi.task(name="tests.echo")
    def echo(request: Greeting) -> Greeting:
        return Greeting(text=request.text.upper())

    @kabudachi.task(name="tests.unkept", result_ttl=None)
    def unkept(request: Greeting) -> Greeting:
        return request

    @kabudachi.task(name="tests.broken")
    def broken(request: Greeting) -> Greeting:
        raise ValueError("no")

    @kabudachi.task(name="tests.relay")
    def relay(request: Greeting) -> Flow:
        return flow(echo.bind(request), echo)

    async def main():
        handle = echo(Greeting(text="kept"))
        assert await handle == Greeting(text="KEPT")
        await eventually_status(handle, "stored")
        assert await kabudachi.resume(handle.task_id) == Greeting(text="KEPT")

        # An entry that is not the certified run's value is never returned.
        key = f"task/{handle.task_id}"
        original = backend.entries[key]
        backend.entries[key] = original.replace(b"KEPT", b"KEPS")
        with pytest.raises(ResultUnavailableError):
            await kabudachi.resume(handle.task_id)
        backend.entries[key] = original

        quiet = unkept(Greeting(text="gone"))
        await quiet
        assert (await quiet.status()).persistence == "disabled"
        with pytest.raises(ResultUnavailableError):
            await kabudachi.resume(quiet.task_id)

        failed = broken(Greeting())
        with pytest.raises(ValueError):
            await failed
        # Only the failure's kind is in the record, as for a task another process ran.
        with pytest.raises(TaskBodyError) as raised:
            await kabudachi.resume(failed.task_id)
        assert raised.value.kind == "ValueError"

        relayed = relay(Greeting(text="twice"))
        # A task that returned a flow resolves to its stages' results, and so does resuming it.
        assert await relayed == [Greeting(text="TWICE"), Greeting(text="TWICE")]
        assert await asyncio.wait_for(kabudachi.resume(relayed.task_id), 5) == await relayed

        with pytest.raises(ResultUnavailableError):
            await kabudachi.resume("no-such-task")

    kabudachi.run(main)

    # A backend that fails changes no outcome; the failure is logged.
    configuration.configure(result_backend=FailingBackend())

    async def failing():
        handle = echo(Greeting(text="still"))
        assert await handle == Greeting(text="STILL")
        await eventually_status(handle, "failed")

    with caplog.at_level(logging.WARNING, logger="kabudachi.result_backend"):
        kabudachi.run(failing)
    assert any("the store is down" in record.getMessage() for record in caplog.records)


def test_a_maps_results_are_resumed_from_the_backend_by_a_later_run_whose_shard_never_held_them(monkeypatch):
    """A one-node shard lives and dies with its run: the second run's shard
    has no record of the first run's map, its pages or its blobs, so only the
    backend can give its results back. Large enough for several pages and
    a value in chunks, with the default bounds."""
    monkeypatch.setattr(registry_module, "_default_registry", TaskRegistry())
    backend = DictBackend()
    configuration = Configuration()
    configuration.configure(processes=0, result_backend=backend)
    monkeypatch.setattr(config_module, "_process_configuration", configuration)

    @kabudachi.task(name="tests.label")
    def label(request: Greeting) -> Greeting:
        if request.times == 7:
            raise ValueError("seven")
        # About 1 KiB each, so 300 fill two default pages; one member past a
        # default chunk, so its bytes go as chunks.
        text = "x" * 300_000 if request.times == 0 else f"{request.times:04}" * 256
        return Greeting(text=text, times=request.times)

    items = [Greeting(times=n) for n in range(300)]

    async def first():
        handle = label.map(items, on_error="collect_all")
        results = await handle
        return (await handle.status()).composite_id, results

    composite_id, expected = kabudachi.run(first)
    assert any(key.startswith(f"composite/{composite_id}/page/1") for key in backend.entries), "several pages"
    assert any("/chunk/" in key for key in backend.entries), "a value in chunks"

    async def later():
        return await asyncio.wait_for(kabudachi.resume(composite_id), 10)

    resumed = kabudachi.run(later)
    # The first run's handle gave back the error its own process raised; a
    # later run has only the failure's kind and message.
    assert isinstance(expected[7], ValueError)
    assert isinstance(resumed[7], kabudachi.TaskFailure) and resumed[7].kind == "ValueError"
    assert resumed[:7] + resumed[8:] == expected[:7] + expected[8:]
