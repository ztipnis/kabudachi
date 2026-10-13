"""Result backends: a Python object with async `store` and `load` keeps the
contract every backend keeps, and a call it cannot answer is an error, not a
hang."""

import asyncio
import contextlib
import logging
import threading
import time

import _kabudachi_contract

from kabudachi import _native
from kabudachi.native_protocol import NetworkedArguments, OneNodeArguments, SubmitArguments
from kabudachi.options import SubmissionOptions, SubmissionSettings


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
