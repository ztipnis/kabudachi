"""Result backends: a Python object with async `store` and `load` keeps the
contract every backend keeps."""

import asyncio
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


def test_a_python_object_with_async_store_and_load_keeps_the_result_backend_contract():
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        _kabudachi_contract.check_result_backend(DictBackend, loop)
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join()
        loop.close()
