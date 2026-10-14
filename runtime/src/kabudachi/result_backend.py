"""Result backends: where certified results are kept for later lookup.

A backend is a Redis or Valkey server, given as a URL, or any object with
the two coroutine methods of `ResultBackend`. Entries are opaque bytes;
kabudachi encodes, checks and expires them itself, so a backend is a plain
key-value store.
"""

import inspect
from datetime import datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class ResultBackend(Protocol):
    """What a custom result backend implements.

    `store` keeps `entry` under `key` until `expires_at` (UTC), replacing
    what was there; it may drop it after. `load` returns what `key` holds,
    or `None`. Both are coroutines run on the run's event loop; one that
    raises, or takes longer than 10 seconds, counts as a failure, which is
    logged and never changes a task's outcome.
    """

    async def store(self, key: str, entry: bytes, expires_at: datetime) -> None: ...

    async def load(self, key: str) -> bytes | None: ...


def is_backend(value: object) -> bool:
    """Whether `value` has coroutine methods `store` and `load`."""
    return all(
        inspect.iscoroutinefunction(getattr(value, name, None)) for name in ("store", "load")
    )
