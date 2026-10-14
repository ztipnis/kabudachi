"""Caller connections, independent of worker execution and membership."""

import asyncio
import concurrent.futures
import threading
import weakref
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any

from kabudachi.config import Configuration, Settings, process_configuration
from kabudachi.errors import ConfigurationError, RunStoppedError, RuntimeNotStartedError
from kabudachi.registry import default_registry
from kabudachi.runner import _cancel, _native_runtime, refresh_logging
from kabudachi.serializers import process_serializers
from kabudachi.session import Session, activate, active_session, deactivate, validate_definitions


def current_session() -> Session:
    if (session := active_session()) is not None:
        return session
    configuration = process_configuration()
    with configuration._caller_lock:
        if configuration._caller is not None and configuration._caller.events.done():
            configuration._caller = None
        if configuration._caller is None:
            settings = configuration.settings()
            if not settings.seeds and settings.authority is None:
                raise RuntimeNotStartedError("configure caller seeds or an authority before calling a task")
            configuration._caller = _Caller(configuration)
        return configuration._caller.session


def _settings(configuration: Configuration, **overrides: Any) -> Settings:
    configured = configuration.settings()
    settings = replace(configured, **{
        "listen": configured.listen or "/ip4/0.0.0.0/tcp/0",
        "processes": max(1, configured.processes),
        "health_listen": None,
        **overrides,
    })
    if not settings.seeds and settings.authority is None:
        raise ConfigurationError("a caller needs seeds or an authority; it cannot found a shard")
    return settings


async def _connect(configuration: Configuration, settings: Settings) -> tuple[Session, asyncio.Task[Any]]:
    validate_definitions(default_registry(), process_serializers())
    refresh_logging()
    native = _native_runtime(settings, client_only=True)
    try:
        await asyncio.wait_for(native.wait_until_ready(), settings.process_start_timeout.total_seconds())
        session = Session(native, default_registry(), process_serializers(), configuration)
        events = asyncio.create_task(session.watch_events())

        def stopped(ended: asyncio.Task[Any]) -> None:
            error = None if ended.cancelled() else ended.exception()
            session.close_caller(error or RunStoppedError("the caller connection stopped"))

        events.add_done_callback(stopped)
        return session, events
    except BaseException:
        native.shutdown()
        raise


def _serve_loop(ready: concurrent.futures.Future[asyncio.AbstractEventLoop]) -> None:
    try:
        with asyncio.Runner() as runner:
            ready.set_result(runner.get_loop())
            runner.get_loop().run_forever()
    except BaseException as error:
        if not ready.done():
            ready.set_exception(error)
        else:
            raise


def _release(loop: asyncio.AbstractEventLoop, thread: threading.Thread, session: Session) -> None:
    session.close_caller(RunStoppedError("the caller owner was destroyed"))
    session.runtime.shutdown()
    loop.call_soon_threadsafe(loop.stop)
    if threading.current_thread() is not thread:
        thread.join(timeout=5)


class _Caller:
    """Owns the implicit caller independently of an application's event loops.

    The configuration owns this object; the loop thread owns no reference
    back to it. Finalization stops transport and the loop on collection or
    interpreter exit. The session borrows configuration so it cannot keep
    its own owner alive after the application replaces that configuration.
    """

    def __init__(self, configuration: Configuration) -> None:
        ready: concurrent.futures.Future[asyncio.AbstractEventLoop] = concurrent.futures.Future()
        self.thread = threading.Thread(target=_serve_loop, args=(ready,), name="kabudachi-caller", daemon=True)
        self.thread.start()
        self.loop = ready.result()
        try:
            self.session, self.events = asyncio.run_coroutine_threadsafe(
                _connect(weakref.proxy(configuration), _settings(configuration)), self.loop,
            ).result()
        except BaseException:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(timeout=5)
            raise
        self._finalizer = weakref.finalize(self, _release, self.loop, self.thread, self.session)


@asynccontextmanager
async def client(
    *,
    seeds: Sequence[str] | None = None,
    authority: str | None = None,
    shard: str | None = None,
    listen: str | None = None,
) -> AsyncIterator[None]:
    """Connects a caller to existing workers on the application's event loop.

    Inside ``async with kabudachi.client(seeds=[worker_address])``, call and
    await tasks, flows, groups, maps, and reducers normally. This process
    never executes task bodies, starts task processes, votes, or registers
    as a worker. Task bodies may still submit and await children through
    the worker running them.

    Connection arguments override ``configure`` and environment settings.
    At least one seed or an authority is required. ``listen`` defaults to
    the configured address, or a free TCP port on all interfaces; workers
    must be able to reach it to deliver results. Configure
    ``external_address`` when a different advertised address is needed.
    Startup waits up to ``process_start_timeout`` for a reachable leader.

    Normal exit waits for submitted work and callbacks. An exceptional
    exit closes local handles; accepted durable tasks remain on the shard.
    Only one client or worker may be active in a process.
    """
    if active_session() is not None:
        raise RuntimeError("a kabudachi worker or client is already running in this process")
    configuration = process_configuration()
    overrides = {name: value for name, value in {
        "seeds": None if seeds is None else tuple(seeds),
        "authority": authority, "shard": shard, "listen": listen,
    }.items() if value is not None}
    settings = _settings(configuration, **overrides)
    session = None
    events = None
    try:
        session, events = await _connect(configuration, settings)
        activate(session)
        yield
        idle = asyncio.create_task(session.wait_until_idle())
        try:
            await asyncio.wait((idle, events), return_when=asyncio.FIRST_COMPLETED)
            if events.done():
                events.result()
            await idle
        finally:
            await _cancel(idle)
    finally:
        if session is not None:
            session.close_caller(RunStoppedError("the caller connection closed"))
            deactivate(session)
        await _cancel(events)
        if session is not None:
            session.runtime.shutdown()
