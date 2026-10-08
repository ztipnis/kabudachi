"""A task process: imports the modules that declare tasks, tells its worker
which tasks it found, then runs the bodies the worker sends it until the
worker closes the pipe or tells it to stop.

Bodies run as they would in the worker: an async body on this process's
event loop, a synchronous one in a thread of a pool with one thread per
place. The worker decides how many run at once; this process runs what it is
sent. A task, flow or group a body calls is submitted by the worker, which
answers at once and later sends its outcome; while a body waits for one, the
worker counts its place free.

It runs its `process_init` hooks before it says it is ready, and every body
between its run hooks.
"""

import asyncio
import concurrent.futures
import contextvars
import dataclasses
import functools
import importlib
import itertools
import os
import pickle
import signal
import sys
import threading
import traceback
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from kabudachi import ipc
from kabudachi.body import fold_compaction, run_serialized
from kabudachi.config import Settings
from kabudachi.errors import StartupError
from kabudachi.handle import TaskHandle, current_body, run_callback_inline
from kabudachi.hosted_work import LoopHostedWork
from kabudachi.lifecycle import RunContext, default_hooks, hooks_for_run, initialize_process
from kabudachi.options import SubmissionOptions
from kabudachi.registry import default_registry
from kabudachi.serializers import process_serializers
from kabudachi.session import activate, invoke_callback

# How long a task process asked to stop (SIGTERM) waits for its async bodies
# to end before it exits anyway. Its worker kills it after the same grace.
STOP_GRACE_SECONDS = 1.0


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
        hooks=tuple(hook.described for hook in default_hooks().all()),
    )


class _TaskProcess:
    """This process's side of the pipe, and the bodies it runs."""

    def __init__(self, connection: Any, settings: Settings) -> None:
        self._connection = connection
        self._send_lock = threading.Lock()
        self._registry = default_registry()
        self._hooks = default_hooks()
        self.serializers = process_serializers()
        self._threads = ThreadPoolExecutor(
            max_workers=settings.concurrency, thread_name_prefix="kabudachi-task"
        )
        self._outcomes: dict[str, asyncio.Future[Any]] = {}
        self._running: set[str] = set()
        self._draining = False
        # Set by SIGTERM: what the bodies do from then on is not reported,
        # so the worker counts their runs lost with this process.
        self._stopping = False
        self._finished = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._buffer_lock = threading.Lock()
        self._early: list[Any] = []
        # Runs the worker asked to cancel; a body that raises CancelledError
        # of its own accord is a failure unless it is one of these.
        self._cancel_asked: set[str] = set()
        self._requests = itertools.count()
        # Requests waiting for the worker's answer: (answer, whether it is a submission).
        self._asked: dict[int, tuple[concurrent.futures.Future[Any], bool]] = {}
        # Handles of tasks bodies here submitted, until the worker sends their outcome.
        self._handles: dict[str, TaskHandle] = {}
        self._hosted = LoopHostedWork()

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
        loop.add_signal_handler(signal.SIGTERM, self._stop_now)
        self._hosted.attach(loop)
        # A stand-in with only what a body reaches through `kabudachi`.
        activate(_ChildSession(self))  # type: ignore[arg-type]
        with self._buffer_lock:
            self._loop = loop
            # Frames that arrived during the imports are handled now, in order.
            for frame in self._early:
                loop.call_soon(self._received, frame)
            self._early.clear()
        try:
            await initialize_process(self._hooks)
        except StartupError as error:
            # Nothing here may run until the process is prepared; the worker
            # gives up on, or replaces, a process that cannot be.
            self.send(dataclasses.replace(ready, error=f"could not initialize: {error}"))
            return
        self.send(ready)
        await self._finished.wait()

    def _read(self) -> None:
        try:
            ipc.read_frames(self._connection, self._received_on_reader)
        except BaseException:
            # The reader broke: say so, rather than exit as if the worker left.
            traceback.print_exc()
            _exit(1)
        # The worker has gone or closed its end: nothing done here could
        # reach it any more.
        _exit(0)

    def _received_on_reader(self, frame: Any) -> None:
        # Answers are taken here, not on the loop: a body waiting on its own
        # thread for one must not need the loop to get it.
        match frame:
            case ipc.Reply():
                self._answered(frame)
                return
            case ipc.WaitDone():
                handle = self._handles.pop(frame.task_id, None)
                if handle is not None and frame.error is not None:
                    handle._fail(frame.error)
                elif handle is not None:
                    handle._resolve(frame.value)
                return
        with self._buffer_lock:
            if self._loop is None:
                self._early.append(frame)
                return
        self._loop.call_soon_threadsafe(self._received, frame)

    def ask(self, make: Callable[[int], Any], *, submission: bool) -> Any:
        """Sends the request `make(request)` builds and waits for the worker's
        answer on this thread (the loop's, for an async body: the worker
        answers at once). A submission is answered with a handle."""
        request = next(self._requests)
        answer: concurrent.futures.Future[Any] = concurrent.futures.Future()
        self._asked[request] = (answer, submission)
        self.send(make(request))
        return answer.result()

    def _answered(self, reply: ipc.Reply) -> None:
        answer, submission = self._asked.pop(reply.request)
        if reply.error is not None:
            answer.set_exception(reply.error)
        elif submission:
            task_id, shard_id = reply.value
            handle = TaskHandle(task_id, self._cancel, self._run_callback, shard_id=shard_id)
            # Kept before the asker gets it: the outcome can follow at once.
            self._handles[task_id] = handle
            answer.set_result(handle)
        else:
            answer.set_result(reply.value)

    def _cancel(self, task_id: str) -> bool:
        return self.ask(lambda request: ipc.CancelTask(request, task_id), submission=False)

    def _run_callback(self, function: Any, value: Any) -> None:
        # A drain waits for it too; checked again once it no longer counts.
        def ended() -> None:
            asyncio.get_running_loop().call_soon(self._finish_if_idle)

        if not self._hosted.spawn(invoke_callback(function, value), when_done=ended):
            run_callback_inline(function, value)

    def _received(self, frame: Any) -> None:
        if self._stopping and isinstance(frame, (ipc.Run, ipc.Compact)):
            # Lost with this process, which is ending; the worker runs it again.
            return
        match frame:
            case ipc.Run():
                self._start_run(frame)
            case ipc.Compact():
                outcome = asyncio.get_running_loop().create_task(self._fold(frame))
                self._track(frame.run_id, outcome, outcome, self._report_compaction)
            case ipc.Cancel():
                outcome = self._outcomes.get(frame.run_id)
                if outcome is not None and outcome.cancel():
                    self._cancel_asked.add(frame.run_id)
            case ipc.Drain():
                self._draining = True
                self._finish_if_idle()

    def _stop_now(self) -> None:
        """SIGTERM: cancels every body and ends the process once the async
        ones have stopped, without reporting their runs, which the worker
        counts lost when this process exits. A synchronous body cannot be
        stopped; the exit ends it. An async body that ignores the cancel is
        ended by the exit after a grace, whoever sent the SIGTERM."""
        if self._stopping:
            return
        self._stopping = True
        outcomes = list(self._outcomes.values())
        for outcome in outcomes:
            outcome.cancel()
        if not outcomes:
            _exit(0)
        loop = asyncio.get_running_loop()
        loop.call_later(STOP_GRACE_SECONDS, _exit, 0)
        stopped = loop.create_task(asyncio.wait(outcomes))
        stopped.add_done_callback(lambda _: _exit(0))

    def _start_run(self, frame: ipc.Run) -> None:
        # A body that waits for a task it called gives its place back meanwhile.
        context = contextvars.copy_context()
        context.run(current_body.set, _BodyWaits(self, frame.run_id))
        hooks = hooks_for_run(
            self._hooks,
            RunContext(frame.definition_id, frame.run_id, frame.attempt),
            frame.queue,
            recycle=functools.partial(self.send, ipc.Recycle()),
        )
        body = context.run(
            run_serialized,
            self._registry,
            self.serializers,
            self._threads,
            frame.definition_id,
            frame.source_version,
            frame.chain,
            frame.serialized_input,
            hooks,
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
        if self._stopping:
            return
        asked = run_id in self._cancel_asked
        self._cancel_asked.discard(run_id)
        if outcome.cancelled():
            self._report_cancelled(run_id, asked)
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
        if self._stopping:
            return
        asked = run_id in self._cancel_asked
        self._cancel_asked.discard(run_id)
        if outcome.cancelled():
            self._report_cancelled(run_id, asked)
            return
        error = outcome.exception()
        if error is not None:
            self.send(ipc.failed(run_id, error))
        else:
            self.send(ipc.Compacted(run_id, outcome.result()))

    def _report_cancelled(self, run_id: str, asked: bool) -> None:
        """A cancelled run is silent when the worker asked for it, and has
        stopped waiting for it; a body that raised CancelledError itself failed."""
        if not asked:
            self.send(ipc.failed(run_id, asyncio.CancelledError()))

    def _exited(self, run_id: str) -> None:
        self._running.discard(run_id)
        if self._stopping:
            return
        self.send(ipc.Exited(run_id))
        self._finish_if_idle()

    def _finish_if_idle(self) -> None:
        if self._draining and not self._running and self._hosted.outstanding == 0:
            self._finished.set()


class _BodyWaits:
    """Tells the worker when one run's body starts and stops waiting for
    tasks it called, so the worker counts its place free meanwhile."""

    def __init__(self, process: _TaskProcess, run_id: str) -> None:
        self._process = process
        self._run_id = run_id
        self._lock = threading.Lock()
        self._waits = 0

    def waiting_started(self) -> None:
        with self._lock:
            self._waits += 1
            if self._waits == 1:
                self._process.send(ipc.Waiting(self._run_id))

    def waiting_finished(self) -> None:
        with self._lock:
            self._waits -= 1
            if self._waits == 0:
                self._process.send(ipc.Resumed(self._run_id))


class _ChildSession:
    """What a body here reaches through `kabudachi`: tasks, flows and groups
    are submitted to the worker, and handles settle when it says so."""

    def __init__(self, process: _TaskProcess) -> None:
        self._process = process
        self.composites = _ChildComposites(process)

    def submit(
        self, definition: Any, argument: Any, options: SubmissionOptions | None = None
    ) -> TaskHandle:
        serializer = self._process.serializers.get(definition.serializer)
        payload = serializer.encode(argument, definition.input_type)
        chosen = options or SubmissionOptions()
        return self._process.ask(
            lambda request: ipc.Submit(request, definition.name, payload, chosen), submission=True
        )


class _ChildComposites:
    """Flows and groups called here: started by the worker, which holds them."""

    def __init__(self, process: _TaskProcess) -> None:
        self._process = process

    def submit_flow(self, flow: Any, previous: Any) -> TaskHandle:
        return self._submit("flow", flow, previous)

    def submit_group(self, group: Any, previous: Any) -> TaskHandle:
        return self._submit("group", group, previous)

    def _submit(self, kind: str, step: Any, previous: Any) -> TaskHandle:
        sent, before = pickle.dumps(step), pickle.dumps(previous)
        return self._process.ask(
            lambda request: ipc.Submit(request, composite=kind, step=sent, previous=before),
            submission=True,
        )


def _after(outcome: "asyncio.Future[Any]", then: Callable[[], None]) -> None:
    """Calls `then` once `outcome` is done, at once if it is."""
    if outcome.done():
        then()
    else:
        outcome.add_done_callback(lambda _: then())
