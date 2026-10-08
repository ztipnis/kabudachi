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
    ready = _import(connection, modules)
    if ready.error is None:
        asyncio.run(_TaskProcess(connection, settings).serve(ready))
    else:
        connection.send(ready)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


def _import(connection: Any, modules: tuple[str, ...]) -> ipc.Ready:
    """Imports `modules` and lists the tasks and serializers they registered."""
    for module in modules:
        connection.send(ipc.Importing(module))
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
        self._loop: asyncio.AbstractEventLoop

    def send(self, frame: Any) -> None:
        """Sends `frame` to the worker, from any thread. A worker that is gone
        ends this process from the reading thread instead."""
        try:
            with self._send_lock:
                self._connection.send(frame)
        except OSError:
            pass

    async def serve(self, ready: ipc.Ready) -> None:
        self._loop = asyncio.get_running_loop()
        threading.Thread(target=self._read, name="kabudachi-pipe", daemon=True).start()
        self.send(ready)
        await self._finished.wait()

    def _read(self) -> None:
        ipc.read_frames(self._connection, self._received_on_reader)
        # The worker has gone or closed its end: nothing done here could
        # reach it any more.
        os._exit(0)

    def _received_on_reader(self, frame: Any) -> None:
        self._loop.call_soon_threadsafe(self._received, frame)

    def _received(self, frame: Any) -> None:
        match frame:
            case ipc.Run():
                self._start_run(frame)
            case ipc.Compact():
                outcome = self._loop.create_task(self._fold(frame))
                self._track(frame.run_id, outcome, outcome, self._report_compaction)
            case ipc.Cancel():
                outcome = self._outcomes.get(frame.run_id)
                if outcome is not None:
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
            return  # the worker asked for it and has stopped waiting for this run
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
            return
        error = outcome.exception()
        if error is not None:
            self.send(ipc.failed(run_id, error))
        else:
            self.send(ipc.Compacted(run_id, outcome.result()))

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
