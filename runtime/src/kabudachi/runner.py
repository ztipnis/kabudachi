"""`kabudachi.run`: start a worker in this process and run your program on it."""

import asyncio
import signal
import threading
import uuid
from collections.abc import Awaitable, Callable
from typing import TypeVar, overload

from kabudachi.config import process_configuration
from kabudachi.registry import default_registry
from kabudachi.serializers import process_serializers
from kabudachi.session import (
    Session,
    activate,
    active_session,
    deactivate,
    validate_definitions,
)

try:
    import kabudachi._native as _native
except ModuleNotFoundError as error:
    # Only the extension itself being absent is tolerated (so the pure-Python
    # parts of the package can be used without it); a broken import inside it
    # is not.
    if error.name != "kabudachi._native":
        raise
    _native = None

T = TypeVar("T")


@overload
def run(main: Callable[[], Awaitable[T]]) -> T: ...
@overload
def run(main: None = None) -> None: ...
def run(main: Callable[[], Awaitable[T]] | None = None) -> T | None:
    """Runs `main()` with a kabudachi worker running beside it, and returns
    what it returns.

    This owns the event loop: like `asyncio.run` it cannot be called from a
    running loop, and Ctrl-C cancels `main`. While `main` runs, calling a task
    queues it for the worker and gives back a handle to await. A task that
    waits for another task does not occupy one of the `concurrency` places
    while it waits, and takes its place back as soon as it resumes, even if
    that briefly puts more than `concurrency` bodies in progress. The
    `concurrency` setting is read when `run` starts.

    When `main` returns, `run` waits for every task that was called to finish
    before it stops the worker. If `main` raises, or is cancelled, tasks
    already running are let finish, tasks that have not started fail with
    `RunStoppedError`, and the error is raised. If the worker itself fails,
    `main` is cancelled and the worker's error is raised.

    Without `main`, `run` serves as a worker (on the main thread) until it
    receives SIGINT or SIGTERM, then drains like the end of `main`: it waits
    for every task that was called and returns `None`. A second signal stops
    the waiting: tasks not finished are abandoned, `run` raises
    `KeyboardInterrupt`, and a synchronous task still in its thread keeps the
    interpreter from exiting until it returns.

    Raises `TaskDefinitionError`, before `main` starts, if a registered task
    cannot work with the serializers of this process.
    """
    if main is None and threading.current_thread() is not threading.main_thread():
        raise RuntimeError(
            "kabudachi.run() without main handles signals, so it needs the main thread"
        )
    if _native is None:
        raise RuntimeError("the kabudachi native extension is not available")
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError("kabudachi.run() cannot be called from a running event loop")
    if active_session() is not None:
        raise RuntimeError("kabudachi.run() is already running in this process")
    validate_definitions(default_registry(), process_serializers())

    with asyncio.Runner() as runner:
        if main is None:
            with _StopSignals(runner.get_loop()) as signals:

                async def serve() -> None:
                    await signals.stop.wait()

                return runner.run(_run_with_worker(serve))
        return runner.run(_run_with_worker(main))


async def _cancel(task: "asyncio.Future[object] | None") -> None:
    """Cancels `task` if it is still running and waits for it to end."""
    if task is not None and not task.done():
        task.cancel()
    if task is not None:
        await asyncio.gather(task, return_exceptions=True)


class _StopSignals:
    """SIGINT and SIGTERM while serving: the first asks for a graceful stop
    and the second interrupts at once, as a second Ctrl-C does under
    `asyncio.run`. Plain signal handlers, not the event loop's, so the second
    still works when a task body blocks the loop. The handlers in place before
    are restored on exit."""

    _SIGNALS = (signal.SIGINT, signal.SIGTERM)

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._received = 0
        self._previous: dict[signal.Signals, object] = {}
        self.stop = asyncio.Event()

    def __enter__(self) -> "_StopSignals":
        for number in self._SIGNALS:
            self._previous[number] = signal.signal(number, self._handle)
        return self

    def __exit__(self, *exc_info: object) -> None:
        for number, previous in self._previous.items():
            signal.signal(number, previous)

    def _handle(self, number: int, frame: object) -> None:
        self._received += 1
        if self._received > 1:
            raise KeyboardInterrupt
        self._loop.call_soon_threadsafe(self.stop.set)


def _worker_ended(worker: "asyncio.Task[None]") -> BaseException:
    """The error that ended the worker loop, or a stand-in if it ended without one."""
    if worker.cancelled():
        return RuntimeError("the kabudachi worker loop was cancelled")
    return worker.exception() or RuntimeError("the kabudachi worker loop stopped unexpectedly")


async def _until_done_or_worker_stops(work: Awaitable[T], worker: "asyncio.Task[None]") -> T:
    """The result of `work`, unless the worker loop ends first: then `work`
    is cancelled and the worker's error raised, so a dead worker is never
    waited on."""
    task = asyncio.ensure_future(work)
    try:
        await asyncio.wait({task, worker}, return_when=asyncio.FIRST_COMPLETED)
        # A worker that ended wins even if `work` finished at the same moment,
        # so its failure is never swallowed.
        if worker.done():
            raise _worker_ended(worker)
        return task.result()
    finally:
        await _cancel(task)


async def _run_with_worker(main: Callable[[], Awaitable[T]]) -> T:
    configuration = process_configuration()
    native = _native.NativeRuntime(
        uuid.uuid4().hex,
        uuid.uuid4().hex,
        result_ttl_ms=configuration.resolve("result_ttl") * 1000,
        memory_soft_limit=configuration.resolve("memory_soft_limit"),
        memory_hard_limit=configuration.resolve("memory_hard_limit"),
    )
    session = None
    worker = None
    try:
        await native.wait_until_leader()
        session = Session(
            native, default_registry(), process_serializers(), configuration
        )
        activate(session)
        worker = asyncio.create_task(session.serve())
        result = await _until_done_or_worker_stops(main(), worker)
        await _until_done_or_worker_stops(session.wait_until_idle(), worker)
        return result
    except BaseException:
        if session is not None:
            session.stop_claiming()
            await _cancel(worker)
            await session.wait_until_running_finish()
        raise
    finally:
        if session is not None:
            # Also on the normal path: between the run going idle and being
            # deactivated a thread can still submit, and would never be served.
            session.stop_claiming()
            deactivate(session)
            session.close()
        await _cancel(worker)
        native.shutdown()
