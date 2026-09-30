"""Work the runtime runs on the run's event loop for other threads."""

import asyncio
import contextvars
import threading
from collections.abc import Callable, Coroutine
from typing import Any


class LoopHostedWork:
    """Coroutines the runtime runs on the run's event loop on behalf of other
    threads (flow and group orchestrations, task callbacks). Each counts as
    outstanding from the moment it is asked for until it ends, however it
    ends, so finishing cannot miss one."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._outstanding = 0
        # Held here, so work whose handle was dropped still runs to its end.
        self._tasks: set[asyncio.Task[Any]] = set()

    def attach(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def has_loop(self) -> bool:
        """Whether there is an open loop to host on: the attached one or,
        failing that, one running on the calling thread, which is attached."""
        return self._open_loop() is not None

    def _open_loop(self) -> asyncio.AbstractEventLoop | None:
        loop = self._loop
        if loop is None:
            try:
                loop = self._loop = asyncio.get_running_loop()
            except RuntimeError:
                return None
        return None if loop.is_closed() else loop

    def spawn(
        self,
        coroutine: Coroutine[Any, Any, Any],
        *,
        context: contextvars.Context | None = None,
        when_done: Callable[[], None] | None = None,
    ) -> bool:
        """Runs `coroutine` on the run's loop, in `context` if given. Once it
        ends, even if it was cancelled before it started, calls `when_done`
        and then stops counting it. Safe to call from any thread. Returns
        `False`, with the coroutine closed and nothing counted, if there is no
        open loop to run it on."""
        loop = self._open_loop()
        if loop is None:
            coroutine.close()
            return False
        with self._lock:
            self._outstanding += 1
        try:
            loop.call_soon_threadsafe(self._start, coroutine, context, when_done)
        except RuntimeError:  # the loop closed between looking and asking
            coroutine.close()
            self._ended(None)
            return False
        return True

    @property
    def outstanding(self) -> int:
        with self._lock:
            return self._outstanding

    def _start(
        self,
        coroutine: Coroutine[Any, Any, Any],
        context: contextvars.Context | None,
        when_done: Callable[[], None] | None,
    ) -> None:
        task = asyncio.get_running_loop().create_task(coroutine, context=context)
        self._tasks.add(task)
        task.add_done_callback(lambda ended: self._finished(ended, when_done))

    def _finished(
        self, task: "asyncio.Task[Any]", when_done: Callable[[], None] | None
    ) -> None:
        self._tasks.discard(task)
        self._ended(when_done)

    def _ended(self, when_done: Callable[[], None] | None) -> None:
        try:
            if when_done is not None:
                when_done()
        finally:
            with self._lock:
                self._outstanding -= 1
