"""Caller connections, independent of worker execution and membership."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any

from kabudachi.config import process_configuration
from kabudachi.errors import ConfigurationError, RunStoppedError
from kabudachi.registry import default_registry
from kabudachi.runner import _cancel, _native_runtime, refresh_logging
from kabudachi.serializers import process_serializers
from kabudachi.session import Session, activate, active_session, deactivate, validate_definitions


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
    configured = configuration.settings()
    settings = replace(
        configured,
        listen=listen or configured.listen or "/ip4/0.0.0.0/tcp/0",
        seeds=tuple(configured.seeds if seeds is None else seeds),
        authority=configured.authority if authority is None else authority,
        shard=configured.shard if shard is None else shard,
        # The caller starts no executor. This preserves worker validation of
        # the shared connection settings without relaxing its kill guarantee.
        processes=max(1, configured.processes),
        health_listen=None,
    )
    if not settings.seeds and settings.authority is None:
        raise ConfigurationError("a caller needs seeds or an authority; it cannot found a shard")
    validate_definitions(default_registry(), process_serializers())
    refresh_logging()
    native = _native_runtime(settings, client_only=True)
    session = None
    events = None
    try:
        await asyncio.wait_for(native.wait_until_ready(), settings.process_start_timeout.total_seconds())
        session = Session(native, default_registry(), process_serializers(), configuration)
        activate(session)
        events = asyncio.create_task(session.watch_events())

        def stopped(ended: asyncio.Task[Any]) -> None:
            if not ended.cancelled() and ended.exception() is not None:
                session.close_caller(ended.exception())

        events.add_done_callback(stopped)
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
        native.shutdown()
