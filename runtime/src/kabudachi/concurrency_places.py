"""The places task bodies run in."""

import asyncio
import threading
from typing import Any

from kabudachi.handle import WaitObserver


class _BodyWaits:
    """Tracks when one running task body is waiting for another task, so the
    places can count it as not occupying one while it does."""

    def __init__(self, places: "ConcurrencyPlaces") -> None:
        self._places = places
        self._waits = 0

    def waiting_started(self) -> None:
        """The body has started waiting for a task; its place is given up while it does."""
        self._waits += 1
        if self._waits == 1:
            self._places.blocked()

    def waiting_finished(self) -> None:
        """The body has stopped waiting for a task; it takes its place back."""
        self._waits -= 1
        if self._waits == 0:
            self._places.unblocked()


class ConcurrencyPlaces:
    """The places task bodies run in: at most `concurrency` bodies hold one
    at a time, and a body gives its place back while it waits for another
    task, from whichever thread it waits on."""

    def __init__(self, concurrency: int) -> None:
        self._concurrency = concurrency
        self._lock = threading.Lock()  # guards _blocked, which any thread changes
        self._blocked = 0
        self._running: set[asyncio.Future[Any]] = set()  # the loop's thread only
        self._freed = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None

    def free(self) -> int:
        with self._lock:
            blocked = self._blocked
        return self._concurrency - (len(self._running) - blocked)

    def occupy(self, running: "asyncio.Future[Any]") -> None:
        """`running` holds a place until it is done. Call on the run's loop."""
        self._loop = asyncio.get_running_loop()
        self._running.add(running)
        running.add_done_callback(self._left)

    def _left(self, running: "asyncio.Future[Any]") -> None:
        self._running.discard(running)
        # Whatever ended it already reached its handle; retrieving it keeps
        # asyncio from logging it as never retrieved.
        if not running.cancelled():
            running.exception()
        self._freed.set()

    def blocked(self) -> None:
        """A body gave its place back. Safe from any thread; wakes the worker
        loop on the loop's own thread."""
        with self._lock:
            self._blocked += 1
        self.wake()

    def unblocked(self) -> None:
        """A body took its place back. Safe from any thread."""
        with self._lock:
            self._blocked -= 1

    def watch_body(self) -> WaitObserver:
        """What one running body reports its waits to: one place is given back
        however many tasks it waits for at once."""
        return _BodyWaits(self)

    def wake(self) -> None:
        """Makes a pending `wait_for_free` return, so its caller looks again.
        Safe from any thread."""
        loop = self._loop
        if loop is None:
            self._freed.set()
            return
        try:
            loop.call_soon_threadsafe(self._freed.set)
        except RuntimeError:
            pass  # the loop is closed, so there is no worker loop to wake

    async def wait_for_free(self) -> None:
        """Returns once a place may be free, or when `wake` is called; at once
        if one is free now."""
        self._loop = asyncio.get_running_loop()
        if self.free() > 0:
            return
        # No await since free() was read, so a place freed from now on is not
        # missed by clearing here: every set() runs on this loop.
        self._freed.clear()
        await self._freed.wait()

    async def wait_until_running_finish(self) -> None:
        """Waits for every task that is running now to finish."""
        while self._running:
            await asyncio.wait(set(self._running))
