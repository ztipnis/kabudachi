"""Result backends: a Python object with async `store` and `load` keeps the
contract every backend keeps, and a call it cannot answer is an error, not a
hang."""

import asyncio
import contextlib
import threading

import _kabudachi_contract


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
