"""`kabudachi.run`: start a worker in this process and run your program on it."""

import _thread
import asyncio
import inspect
import os
import signal
import socket
import sys
import threading
import traceback
import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, TypeVar, overload

from kabudachi import _native
from kabudachi.config import Configuration, Settings, authority_parts, process_configuration
from kabudachi.execution import Executor, InProcessExecutor
from kabudachi.lifecycle import default_hooks
from kabudachi.pool import ProcessPool, task_modules
from kabudachi.registry import default_registry
from kabudachi.serializers import process_serializers
from kabudachi.session import (
    Session,
    activate,
    active_session,
    deactivate,
    validate_definitions,
)

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
    queues it for the worker and gives back a handle to await. Task bodies
    run in `processes` task processes (one per CPU by default), each with
    `concurrency` places, or, with `processes=0`, in this process. A task that
    waits for another task does not occupy one of the `concurrency` places
    while it waits, and takes its place back as soon as it resumes, even if
    that briefly puts more than `concurrency` bodies in progress. The
    `processes`, `concurrency` and `imports` settings are read when `run`
    starts.

    When `main` returns, `run` waits for every task that was called to finish
    before it stops the worker. If `main` raises, or is cancelled, tasks
    already running are let finish, tasks that have not started fail with
    `RunStoppedError`, and the error is raised. If the worker itself fails,
    `main` is cancelled and the worker's error is raised.

    Without `main`, `run` serves as a worker (on the main thread) until it
    receives SIGINT or SIGTERM, then drains like the end of `main`: it waits
    for every task that was called and returns `None`. A second signal stops
    the waiting: task processes are stopped at once, tasks not finished are
    abandoned, `run` raises
    `KeyboardInterrupt`, and a synchronous task still in its thread keeps the
    interpreter from exiting until it returns.

    With `listen` set, the process is a worker of the networked shard
    `shard`. It bootstraps from `seeds` or `authority`, `run` waits until it
    has joined and knows its leader, and it runs the tasks the shard's leader
    hands it, whoever submitted them. Awaiting a task's result there raises
    `RemoteResultUnavailableError`, and cancelling one asks the leader. When
    `main` returns, `run` waits until the leader has stored every task
    called; then, as at the first signal, the worker also lets the runs it
    holds finish and waits until its leader has taken their reports.

    Raises `TaskDefinitionError`, before `main` starts, if a registered task
    cannot work with the serializers of this process, and if `processes` is
    above 0 and a task is declared in the script being run. Raises
    `StartupError` if the task processes cannot start.
    """
    if main is None and threading.current_thread() is not threading.main_thread():
        raise RuntimeError(
            "kabudachi.run() without main handles signals, so it needs the main thread"
        )
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError("kabudachi.run() cannot be called from a running event loop")
    if active_session() is not None:
        raise RuntimeError("kabudachi.run() is already running in this process")
    validate_definitions(default_registry(), process_serializers())
    executor = _executor_for(process_configuration())

    with asyncio.Runner() as runner:
        if main is None:
            with _StopSignals(runner.get_loop()) as signals:

                async def serve() -> None:
                    await signals.stop.wait()

                return runner.run(_run_with_worker(serve, executor, serving=True))
        return runner.run(_run_with_worker(main, executor))


def _executor_for(configuration: Configuration) -> Executor:
    """Task processes as configured, or this process with `processes=0`.
    Raises `TaskDefinitionError` for a task or hook task processes could not import."""
    settings = configuration.settings()
    registry = default_registry()
    hooks = default_hooks()
    if settings.processes == 0:
        return InProcessExecutor(registry, process_serializers(), settings.concurrency, hooks)
    return ProcessPool(settings, task_modules(registry, hooks, settings.imports), registry, hooks)


def _millis(duration: timedelta) -> int:
    return round(duration.total_seconds() * 1000)


def _native_runtime(settings: Settings) -> Any:
    """A one-node shard of this process's own, or, with `listen` set, a
    worker of the networked shard `settings.shard`."""
    limits = {
        "result_ttl_ms": settings.result_ttl * 1000,
        "memory_soft_limit": settings.memory_soft_limit,
        "memory_hard_limit": settings.memory_hard_limit,
    }
    if settings.listen is None:
        return _native.NativeRuntime(uuid.uuid4().hex, uuid.uuid4().hex, **limits)
    authority = {}
    if settings.authority is not None:
        url, prefix, database, ttl = authority_parts(settings.authority)
        authority = {
            "authority_url": url,
            "authority_key_prefix": prefix,
            "authority_database": database,
        }
        if ttl is not None:
            authority["authority_ttl_ms"] = _millis(timedelta(seconds=ttl))
    return _native.NetworkedRuntime(
        settings.shard,
        settings.listen,
        seeds=list(settings.seeds),
        external_address=settings.external_address,
        heartbeat_interval_ms=_millis(settings.heartbeat_interval),
        heartbeat_timeout_ms=_millis(settings.heartbeat_timeout),
        reconnect_timeout_ms=_millis(settings.reconnect_timeout),
        **authority,
        **limits,
    )


async def _cancel(task: "asyncio.Future[object] | None") -> None:
    """Cancels `task` if it is still running and waits for it to end."""
    if task is not None and not task.done():
        task.cancel()
    if task is not None:
        await asyncio.gather(task, return_exceptions=True)


_NOT_RAISED = -2
_RAISED = -1


class _StopSignals:
    """SIGINT and SIGTERM while serving: the first asks for a graceful stop
    and the second interrupts at once, as a second Ctrl-C does under
    `asyncio.run`. Plain signal handlers, not the event loop's, so the second
    still works when a task body blocks the loop. The handlers in place before
    are restored on exit.

    Signals are counted from the wakeup fd, not from handler calls: CPython
    runs a handler once however many times its signal arrived before the main
    thread got to run it (for example while a task body is in a blocking call
    that the first signal did not interrupt), but writes a byte to the wakeup
    fd for every arrival. While serving, `run` owns the process wakeup fd, and
    a watcher thread reads it, so the count never depends on the main thread
    running.

    Limit: a task that replaces the wakeup fd or the SIGINT or SIGTERM
    handlers (for example with `loop.add_signal_handler`) is unsupported while
    serving. Signals are then no longer counted and a second one may not stop
    the run.

    Once two signals have arrived the main thread must be interrupted. The
    handler cannot raise while the interrupted code is inside asyncio, where
    an exception can lose a task's wakeup, so it queues a raise between
    callbacks instead; a task body that blocks the loop never runs that
    callback. The watcher therefore signals the main thread again (about every
    50 ms) until an interrupt has been raised, and that signal arrives outside
    asyncio. At most one such re-send is outstanding: the next SIGINT byte on
    the wakeup fd is attributed to it and not counted, so re-sends never
    inflate the count and never merge with each other. A re-send is not
    sent again until its byte has been seen.

    After an interrupt has been raised the watcher stops re-sending, takes in
    every byte already written, and remembers the real arrivals so far. Any
    real arrival beyond that re-arms the interrupt, so a task body that
    swallowed it does not make later Ctrl-Cs do nothing. A raise can also land
    where CPython discards exceptions (a `__del__`, a weakref callback, a
    garbage-collection finalizer: printed as "Exception ignored in") and never
    propagate. While serving, a `sys.unraisablehook` wrapper sees that
    discard and re-arms, so the interrupt is delivered again; a slow unwind is
    never mistaken for a discard, so the interrupt is never raised twice into
    one unwind.

    The interrupt is never raised from `run` or from this class's own methods,
    which are the frames where the run is already stopping or already handling
    a signal and a raise would skip putting the handlers back. CPython has no
    reentrancy guard for Python signal handlers: a handler can run again at
    the first eval-breaker check inside the handler itself, in frames that are
    none of those. So the handler returns at once while it is already running;
    the watcher re-sends anyway. On exit, re-sent signals still pending are
    discarded with SIGINT blocked, so none is left to hit the restored
    handler.

    Remaining limits, by design. The kernel merges equal signals that arrive
    while one is pending, so a real SIGINT that arrives while a re-send is
    pending can be merged into it: that press is lost, and the next one works.
    CPython sets a signal's tripped flag before the C handler writes its wakeup
    byte, so a handler can run before its byte is readable; the second signal
    is then only seen once the byte arrives, and the watcher's re-send raises
    it. C paths that silently clear an error (for example `PyObject_HasAttr`
    on 3.11 and 3.12, or `PyDict_GetItem` with a Python `__hash__` or `__eq__`)
    can drop a KeyboardInterrupt without calling the hook; the next real press
    re-arms. A real signal whose byte is written after the interrupt was
    raised but before the watcher's drain that follows is taken as part of
    that raise rather than as a re-arm. The reverse also holds for a re-arming
    press whose C handler runs on a thread other than the main one: if its
    byte is written after the drain, the next pass reads it as a new press and
    re-sends once into the same unwind, because the byte follows the tripped
    flag by one C statement. A garbage collection triggered during the drain
    runs finalizers on the watcher thread while it holds the lock; a finalizer
    that waits on something the main thread holds while its handler waits for
    that lock would deadlock."""

    _SIGNALS = (signal.SIGINT, signal.SIGTERM)
    _POLL_SECONDS = 0.05

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._arrived = 0
        # Whether the watcher ever sent the main thread a signal itself.
        self._resent = False
        # Whether a failure of the watcher has been printed already.
        self._reported = False
        # A re-send whose SIGINT byte has not been read yet; the next one is it.
        self._outstanding = False
        # One value for the whole raise state, so no reader sees it half
        # updated: _NOT_RAISED, _RAISED while an interrupt is raised and the
        # bytes written before it are not yet read, or the real arrivals at the
        # time of the raise once they are. While raised the watcher does not
        # re-send; a later arrival re-arms.
        #
        # Every write goes through `_lock`; reads take no lock. The lock is
        # reentrant because the handler runs on the main thread between any two
        # bytecodes, including inside a critical section the main thread holds,
        # and must not wait for itself. Critical sections are a compare and a
        # store, plus in one place a drain of the wakeup socket that never
        # blocks, so the handler never waits for more than the watcher
        # finishing that, and the watcher never waits for the main thread
        # while holding it.
        #
        # Why the watcher's clear cannot hit a newer raise: it clears only if
        # the state still equals the mark it read, compared and stored inside
        # one critical section. A discard and new raise since the read leave
        # _RAISED, which is not a mark, so the clear does nothing and the new
        # raise stays. The state can never return to the same mark meanwhile:
        # only this watcher thread stores marks, and it is busy with this clear.
        self._state = _NOT_RAISED
        self._lock = threading.RLock()
        # Set while the handler runs, so a handler CPython runs inside it returns.
        self._in_handler = False
        # At most one raise between callbacks is ever queued.
        self._deferred = False
        self._closing = False
        self._previous: dict[signal.Signals, object] = {}
        self._previous_wakeup = -1
        self._wakeup_installed = False
        self._watcher: threading.Thread | None = None
        self._previous_unraisable: Callable[[sys.UnraisableHookArgs], object] | None = None
        self._arrivals: socket.socket
        self._wakeup: socket.socket
        self.stop = asyncio.Event()

    def __enter__(self) -> "_StopSignals":
        for number in self._SIGNALS:
            if signal.getsignal(number) is None:
                # Not installed from Python, so it could not be put back.
                raise RuntimeError(
                    f"cannot serve: the {number.name} handler was not installed from Python "
                    "and could not be restored"
                )
        blocking = hasattr(signal, "pthread_sigmask")
        before: set[signal.Signals] = set()
        if blocking:
            before = signal.pthread_sigmask(signal.SIG_BLOCK, ())
        arrivals: socket.socket | None = None
        wakeup: socket.socket | None = None
        try:
            arrivals, wakeup = socket.socketpair()
            if blocking:
                # A signal handled by the previous handler between installing
                # the wakeup fd and recording it would skip resetting the fd on
                # restore; it is delivered to ours once everything is in place.
                # Blocking runs the handlers of signals already pending, so a
                # raise from one must put the mask back and free the sockets.
                signal.pthread_sigmask(signal.SIG_BLOCK, set(self._SIGNALS))
        except BaseException:
            if blocking:
                signal.pthread_sigmask(signal.SIG_SETMASK, before)
            for sock in (arrivals, wakeup):
                if sock is not None:
                    sock.close()
            raise
        self._arrivals, self._wakeup = arrivals, wakeup
        try:
            self._arrivals.settimeout(self._POLL_SECONDS)
            self._wakeup.setblocking(False)
            self._previous_wakeup = signal.set_wakeup_fd(
                self._wakeup.fileno(), warn_on_full_buffer=False
            )
            self._wakeup_installed = True
            for number in self._SIGNALS:
                self._previous[number] = signal.signal(number, self._handle)
            self._previous_unraisable = sys.unraisablehook
            sys.unraisablehook = self._unraisable
            watcher = threading.Thread(target=self._watch, daemon=True)
            watcher.start()
            self._watcher = watcher
        except BaseException:
            self._restore()
            raise
        finally:
            if blocking:
                signal.pthread_sigmask(signal.SIG_SETMASK, before)
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._restore()

    def _restore(self) -> None:
        # Stop handling first so a stray signal cannot reach the handler while
        # the handlers are being put back.
        self._closing = True
        blocking = hasattr(signal, "pthread_sigmask")
        if blocking:
            # The watcher's re-sent SIGINT must not be delivered between here
            # and the end of the drain.
            old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
        try:
            failures: list[BaseException] = []

            def step(action: Callable[..., object], *args: object) -> None:
                # Each step stands alone: one failing (for example a handler an
                # embedded interpreter refuses to set) must not skip the rest.
                try:
                    action(*args)
                except Exception as error:
                    failures.append(error)

            if self._watcher is not None:
                try:
                    # Wakes the watcher at once; the byte is not a signal number.
                    self._wakeup.send(b"\0")
                except OSError:
                    pass
                self._watcher.join()
            if self._previous_unraisable is not None:
                if sys.unraisablehook == self._unraisable:
                    sys.unraisablehook = self._previous_unraisable
                # Kept, not cleared: a hook left installed by someone who wrapped
                # and restored it must still chain to the original.
            if blocking and self._resent:
                # Ignoring a signal drops its pending instances. Never wait for
                # one: another thread may have taken it already.
                step(signal.signal, signal.SIGINT, signal.SIG_IGN)
            # SIGINT goes last: a stray SIGINT taken by another thread must not
            # raise from the restored default handler before SIGTERM and the
            # wakeup fd are put back.
            previous = self._previous
            if signal.SIGTERM in previous:
                step(signal.signal, signal.SIGTERM, previous[signal.SIGTERM])
            if self._wakeup_installed:
                step(signal.set_wakeup_fd, self._previous_wakeup)
                self._wakeup_installed = False
            if signal.SIGINT in previous:
                step(signal.signal, signal.SIGINT, previous[signal.SIGINT])
            previous.clear()
            if failures:
                raise failures[0]
        finally:
            # Closed before the mask is restored: unblocking can hand a pending
            # real SIGINT to the restored handler, which may raise.
            try:
                self._arrivals.close()
                self._wakeup.close()
            finally:
                if blocking:
                    signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)

    def _second(self) -> bool:
        return self._arrived >= 2

    def _is_raised(self) -> bool:
        return self._state != _NOT_RAISED

    def _raised(self) -> None:
        with self._lock:
            self._state = _RAISED

    def _discard(self) -> None:
        with self._lock:
            self._state = _NOT_RAISED

    def _kick(self) -> None:
        self._resent = True
        if hasattr(signal, "pthread_kill"):
            signal.pthread_kill(threading.main_thread().ident, signal.SIGINT)
        else:
            _thread.interrupt_main(signal.SIGINT)
        # Only once the send succeeded: a send that raised produces no byte, and
        # an outstanding mark nothing clears would stop the watcher re-sending.
        self._outstanding = True

    def _absorb(self, data: bytes) -> bool:
        """Counts the real arrivals in `data`; whether it held the re-send."""
        attributed = False
        for number in data:
            if number not in self._SIGNALS:
                continue
            if number == signal.SIGINT and self._outstanding:
                self._outstanding = False
                attributed = True
            else:
                self._arrived += 1
        return attributed

    def _drain(self) -> bool:
        """Reads what is in the socket now without waiting; whether it held the
        re-send. A byte is written after the signal's tripped flag is set, so a
        handler call that has begun may still have its byte unwritten."""
        attributed = False
        fd = self._arrivals.fileno()
        while True:
            try:
                data = os.read(fd, 4096)
            except OSError:
                return attributed
            if not data:
                return attributed
            attributed = self._absorb(data) or attributed

    def _watch(self) -> None:
        while not self._closing:
            try:
                data = self._arrivals.recv(4096)
            except TimeoutError:
                data = b""
            except OSError:
                return
            try:
                self._watch_pass(data)
            except Exception:
                # Reported and kept going: a dead watcher would leave a second
                # signal unable to interrupt a blocked loop, with no sign why.
                # Printed once, and guarded: a pass that keeps failing would
                # flood stderr every poll, and a broken stderr must not kill
                # the watcher either.
                if not self._reported:
                    self._reported = True
                    try:
                        traceback.print_exc()
                    except Exception:
                        pass

    def _watch_pass(self, data: bytes) -> None:
        attributed = self._absorb(data)
        # Read once: the main thread can change it at any time after.
        state = self._state
        if state == _RAISED:
            with self._lock:
                # Checked again under the lock, and before the drain: a discard
                # and a new raise since the read are both _RAISED, so a mark
                # taken from a drain that ran before this point could be older
                # than the new raise and later clear it for a press it
                # already counted. The drain never waits (the socket is read
                # without blocking) and only this thread touches what it
                # updates, so the handler waits here for at most one drain (finalizers run by a
                # collection during it excepted, see the class docstring).
                if self._state == _RAISED:
                    self._drain()
                    self._state = self._arrived
        elif state != _NOT_RAISED and self._arrived > state:
            with self._lock:
                # Only the raise whose mark was read is cleared; a newer one
                # must stay.
                if self._state == state:
                    self._state = _NOT_RAISED
        # A poll that just read the re-send waits out the next one before
        # re-sending, so a main thread inside asyncio is not hammered.
        if (
            self._second()
            and not self._is_raised()
            and not self._outstanding
            and not attributed
            and not self._closing
        ):
            self._kick()

    def _unraisable(self, unraisable: "sys.UnraisableHookArgs") -> None:
        # CPython reports an exception it discards here. A discarded interrupt
        # is the only proof that a raise did not propagate, so only then is the
        # interrupt delivered again. Always chains, and never re-arms before
        # the previous hook has returned or raised: the watcher would re-send
        # inside a foreign hook's frame, and if that hook raised, CPython would
        # not call this hook again, so nothing would re-arm.
        try:
            previous = self._previous_unraisable or sys.__unraisablehook__
            previous(unraisable)
        finally:
            try:
                exc_type = unraisable.exc_type
                # Signal-raised interrupts only land on the main thread, so a
                # discard elsewhere is not the outstanding raise.
                if (
                    threading.current_thread() is threading.main_thread()
                    and self._is_raised()
                    and not self._closing
                    and isinstance(exc_type, type)
                    and issubclass(exc_type, KeyboardInterrupt)
                ):
                    self._discard()
            except Exception:
                pass

    def _raise_between_callbacks(self) -> None:
        self._deferred = False
        if self._closing or self._is_raised():
            return
        self._raised()
        raise KeyboardInterrupt

    def _handle(self, number: int, frame: object) -> None:
        if self._in_handler:
            return
        self._in_handler = True
        try:
            self._handle_once(frame)
        finally:
            self._in_handler = False

    def _handle_once(self, frame: object) -> None:
        if self._closing:
            return
        if not self._second():
            self._loop.call_soon_threadsafe(self.stop.set)
            return
        if self._is_raised() or _is_stopping(frame):
            return
        if _in_event_loop_internals(frame):
            # Raising here could abort asyncio between resolving a future and
            # scheduling its waiters, losing a task's wakeup so that shutdown
            # waits on that task forever. Raise between callbacks instead; the
            # watcher re-sends the signal for a body that blocks the loop.
            if not self._deferred:
                self._deferred = True
                self._loop.call_soon_threadsafe(self._raise_between_callbacks)
            return
        self._raised()
        raise KeyboardInterrupt


_STOPPING_CODES = frozenset(
    {run.__code__}
    | {
        member.__code__
        for member in vars(_StopSignals).values()
        if inspect.isfunction(member)
    }
)


def _is_stopping(frame: object) -> bool:
    """Whether the interrupted code is `run` or a method of `_StopSignals`: the
    run is already stopping there, or a handler is already running, and a raise
    would skip restoring the handlers. The watcher sends again until an
    interrupt is raised."""
    return getattr(frame, "f_code", None) in _STOPPING_CODES


def _in_event_loop_internals(frame: object) -> bool:
    """Whether the interrupted code is inside asyncio or selectors."""
    module = getattr(frame, "f_globals", {}).get("__name__", "")
    return module == "selectors" or module == "asyncio" or module.startswith("asyncio.")


def _worker_ended(worker: "asyncio.Task[None]") -> BaseException:
    """The error that ended the worker loop, or a stand-in if it ended without one."""
    if worker.cancelled():
        return RuntimeError("the kabudachi worker loop was cancelled")
    return worker.exception() or RuntimeError("the kabudachi worker loop stopped unexpectedly")


async def _until_done_or_worker_stops(work: Awaitable[T], *workers: "asyncio.Task[None]") -> T:
    """The result of `work`, unless a worker loop ends first: then `work`
    is cancelled and that loop's error raised, so a dead worker is never
    waited on."""
    task = asyncio.ensure_future(work)
    try:
        await asyncio.wait({task, *workers}, return_when=asyncio.FIRST_COMPLETED)
        # A worker that ended wins even if `work` finished at the same moment,
        # so its failure is never swallowed.
        for worker in workers:
            if worker.done():
                raise _worker_ended(worker)
        return task.result()
    finally:
        await _cancel(task)


async def _drain_held_runs(
    session: Session, native: Any, claiming: "asyncio.Task[None]", events: "asyncio.Task[None]"
) -> None:
    """Lets the runs a networked worker holds finish, then waits until its
    leader has taken their reports, or its reconnect timeout has passed.
    Claiming and taking have stopped, so the claim loop ends on its own. The
    event loop that ran all along goes on meanwhile, so no event it has taken
    is lost: a run the leader cancels, or whose abort deadline nears, stops.
    A runtime that fails ends the wait. The claim loop is neither cancelled
    nor read here: how it ended is the caller's to read, and a claim loop
    that failed still leaves the held runs to finish and report."""
    await asyncio.wait({claiming, events}, return_when=asyncio.FIRST_COMPLETED)
    if events.done():
        raise _worker_ended(events)
    await _until_done_or_worker_stops(session.wait_until_running_finish(), events)
    await _until_done_or_worker_stops(native.wait_until_reported(), events)


async def _run_with_worker(
    main: Callable[[], Awaitable[T]], executor: Executor, *, serving: bool = False
) -> T:
    """With `serving`, `main` only waits for the first signal, so a cancel is
    the second one: task processes are then stopped at once, not after their
    running bodies, which a body that ignores the cancel would hold up for its
    cancel grace. In this process nothing can be killed, so running bodies are
    still waited for."""
    configuration = process_configuration()
    settings = configuration.settings()
    networked = settings.listen is not None
    # Set once every body has finished: the task processes are then let go
    # idle. Otherwise (a second signal, or a run cancelled while it waited
    # for its bodies) they are stopped at once.
    graceful = False
    await executor.start()
    try:
        native = _native_runtime(settings)
        session = None
        claiming = None
        events = None
        try:
            if networked:
                # Joined, with a leader it knows, which may be another process.
                await native.wait_until_ready()
            else:
                await native.wait_until_leader()
            session = Session(
                native, default_registry(), process_serializers(), configuration, executor=executor
            )
            activate(session)
            # Two loops, not `serve`: the event loop must outlive the claim loop,
            # through the drain, or events it has taken would be lost with it.
            claiming = asyncio.create_task(session.work())
            events = asyncio.create_task(session.watch_events())
            result = await _until_done_or_worker_stops(main(), claiming, events)
            await _until_done_or_worker_stops(session.wait_until_idle(), claiming, events)
            if networked:
                # It also holds runs no handle here waits for: they finish,
                # and the leader takes their reports, before the worker goes.
                session.stop_claiming()
                native.stop_taking()
                await _drain_held_runs(session, native, claiming, events)
                # A claim loop that failed fails the run, once its held runs
                # have reported.
                claiming.result()
            graceful = True
            return result
        except BaseException as error:
            if session is not None:
                session.stop_claiming()
                if networked:
                    # Ends a waiting claim, which must not be cancelled.
                    native.stop_taking()
                forced = serving and isinstance(error, (asyncio.CancelledError, KeyboardInterrupt))
                if networked and not forced and events is not None and not events.done():
                    try:
                        await _drain_held_runs(session, native, claiming, events)
                    except Exception:
                        # The error being raised says why the run ended; a
                        # runtime failing meanwhile only cuts the drain short.
                        pass
                await _cancel(claiming)
                await _cancel(events)
                if forced and executor.stops_bodies_at_once:
                    # Their runs settle as lost once their processes are gone.
                    await executor.stop(kill=True)
                await session.wait_until_running_finish()
                graceful = True
            raise
        finally:
            if session is not None:
                # Also on the normal path: between the run going idle and being
                # deactivated a thread can still submit, and would never be served.
                session.stop_claiming()
                deactivate(session)
            await _cancel(claiming)
            await _cancel(events)
            native.shutdown()
    finally:
        try:
            await executor.stop(kill=not graceful)
        except BaseException:
            await executor.stop(kill=True)  # a second cancel must not leave children
            raise
