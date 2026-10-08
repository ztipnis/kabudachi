"""A task process: imports the modules that declare tasks, tells its worker
which tasks it found, then runs the bodies the worker sends it until the
worker closes the pipe or tells it to stop.

Bodies run as they would in the worker: an async body on this process's
event loop, a synchronous one in a thread of a pool with one thread per
place. The worker decides how many run at once; this process runs what it is
sent.
"""

import asyncio
import importlib
import os
import pickle
import signal
import sys
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from kabudachi import ipc
from kabudachi.body import fold_compaction, run_serialized
from kabudachi.config import Settings
from kabudachi.registry import default_registry
from kabudachi.serializers import process_serializers


def main(connection: Any, settings: Settings, modules: tuple[str, ...]) -> None:
    """Runs this task process to its end. It always ends with `os._exit`: a
    synchronous body still in its thread must not keep the process alive."""
    # Ctrl-C in a terminal reaches every process of the group; only the
    # worker decides what it means.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    code = 1
    try:
        process = _TaskProcess(connection, settings)
        # Reading starts before the imports, which can take long: a worker
        # that dies meanwhile must not leave this process behind.
        process.start_reading()
        ready = _import(process, modules)
        if ready.error is None:
            asyncio.run(process.serve(ready))
        else:
            process.send(ready)
        code = 0
    finally:
        _exit(code)


def _exit(code: int) -> None:
    """Ends the process at once, after what the bodies printed is written."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass
    os._exit(code)


def _import(process: "_TaskProcess", modules: tuple[str, ...]) -> ipc.Ready:
    """Imports `modules` and lists the tasks and serializers they registered."""
    for module in modules:
        process.send(ipc.Importing(module))
        try:
            importlib.import_module(module)
        except BaseException as error:
            return ipc.Ready({}, (), error=f"could not import {module}: {type(error).__name__}: {error}")
    return ipc.Ready(
        {definition.name: definition.version for definition in default_registry().definitions()},
        tuple(process_serializers().names()),
    )


class _TaskProcess:
    """This process's side of the pipe, and the bodies it runs."""

    def __init__(self, connection: Any, settings: Settings) -> None:
        self._connection = connection
        self._send_lock = threading.Lock()
        self._registry = default_registry()
        self.serializers = process_serializers()
        self._threads = ThreadPoolExecutor(
            max_workers=settings.concurrency, thread_name_prefix="kabudachi-task"
        )
        self._outcomes: dict[str, asyncio.Future[Any]] = {}
        self._running: set[str] = set()
        self._draining = False
        self._finished = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._buffer_lock = threading.Lock()
        self._early: list[Any] = []
        # Runs the worker asked to cancel; a body that raises CancelledError
        # of its own accord is a failure unless it is one of these.
        self._cancel_asked: set[str] = set()

    def send(self, frame: Any) -> None:
        """Sends `frame` to the worker, from any thread. A worker that is gone
        ends this process from the reading thread instead."""
        try:
            with self._send_lock:
                self._connection.send(frame)
        except OSError:
            pass

    def start_reading(self) -> None:
        threading.Thread(target=self._read, name="kabudachi-pipe", daemon=True).start()

    async def serve(self, ready: ipc.Ready) -> None:
        loop = asyncio.get_running_loop()
        with self._buffer_lock:
            self._loop = loop
            # Frames that arrived during the imports are handled now, in order.
            for frame in self._early:
                loop.call_soon(self._received, frame)
            self._early.clear()
        self.send(ready)
        await self._finished.wait()

    def _read(self) -> None:
        ipc.read_frames(self._connection, self._received_on_reader)
        # The worker has gone or closed its end: nothing done here could
        # reach it any more.
        _exit(0)

    def _received_on_reader(self, frame: Any) -> None:
        with self._buffer_lock:
            if self._loop is None:
                self._early.append(frame)
                return
        self._loop.call_soon_threadsafe(self._received, frame)

    def _received(self, frame: Any) -> None:
        match frame:
            case ipc.Run():
                self._start_run(frame)
            case ipc.Compact():
                outcome = asyncio.get_running_loop().create_task(self._fold(frame))
                self._track(frame.run_id, outcome, outcome, self._report_compaction)
            case ipc.Cancel():
                outcome = self._outcomes.get(frame.run_id)
                if outcome is not None:
                    self._cancel_asked.add(frame.run_id)
                    outcome.cancel()
            case ipc.Drain():
                self._draining = True
                self._finish_if_idle()

    def _start_run(self, frame: ipc.Run) -> None:
        body = run_serialized(
            self._registry,
            self.serializers,
            self._threads,
            frame.definition_id,
            frame.source_version,
            frame.chain,
            frame.serialized_input,
        )
        self._track(frame.run_id, body.outcome, body.exited, self._report_result)

    async def _fold(self, frame: ipc.Compact) -> bytes:
        return fold_compaction(self._registry, self.serializers, frame.definition_id, frame.payloads)

    def _track(
        self,
        run_id: str,
        outcome: "asyncio.Future[Any]",
        exited: "asyncio.Future[Any]",
        report: Callable[[str, "asyncio.Future[Any]"], None],
    ) -> None:
        self._running.add(run_id)
        self._outcomes[run_id] = outcome
        # Registered before the exit is, so a result always reaches the
        # worker before the exit that frees its place.
        outcome.add_done_callback(lambda done: report(run_id, done))
        exited.add_done_callback(lambda _: _after(outcome, lambda: self._exited(run_id)))

    def _report_result(self, run_id: str, outcome: "asyncio.Future[Any]") -> None:
        self._outcomes.pop(run_id, None)
        if outcome.cancelled():
            self._report_cancelled(run_id)
            return
        error = outcome.exception()
        if error is not None:
            self.send(ipc.failed(run_id, error))
            return
        value = outcome.result()
        if isinstance(value, bytes):
            self.send(ipc.Result(run_id, value))
            return
        try:
            step = pickle.dumps(value)
        except Exception as unsendable:
            self.send(ipc.failed(run_id, unsendable))
            return
        self.send(ipc.Result(run_id, None, step))

    def _report_compaction(self, run_id: str, outcome: "asyncio.Future[Any]") -> None:
        self._outcomes.pop(run_id, None)
        if outcome.cancelled():
            self._report_cancelled(run_id)
            return
        error = outcome.exception()
        if error is not None:
            self.send(ipc.failed(run_id, error))
        else:
            self.send(ipc.Compacted(run_id, outcome.result()))

    def _report_cancelled(self, run_id: str) -> None:
        """A cancelled run is silent when the worker asked for it, and has
        stopped waiting for it; a body that raised CancelledError itself failed."""
        if run_id in self._cancel_asked:
            self._cancel_asked.discard(run_id)
        else:
            self.send(ipc.failed(run_id, asyncio.CancelledError()))

    def _exited(self, run_id: str) -> None:
        self._running.discard(run_id)
        self.send(ipc.Exited(run_id))
        self._finish_if_idle()

    def _finish_if_idle(self) -> None:
        if self._draining and not self._running:
            self._finished.set()


def _after(outcome: "asyncio.Future[Any]", then: Callable[[], None]) -> None:
    """Calls `then` once `outcome` is done, at once if it is."""
    if outcome.done():
        then()
    else:
        outcome.add_done_callback(lambda _: then())
