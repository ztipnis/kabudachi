"""Function-based worker orchestration, confined to test support."""

import asyncio
import threading
from collections.abc import Awaitable, Callable
from typing import TypeVar

from kabudachi.config import process_configuration
from kabudachi.registry import default_registry
from kabudachi import runner as worker_runner
from kabudachi.serializers import process_serializers
from kabudachi.session import active_session, validate_definitions

T = TypeVar("T")


def run_with_worker(main: Callable[[], Awaitable[T]] | None = None) -> T | None:
    """Runs a test program beside a worker, using the private lifecycle seam."""
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
    worker_runner.refresh_logging()
    executor = worker_runner._executor_for(process_configuration())

    with asyncio.Runner() as runner:
        if main is None:
            grace = process_configuration().settings().drain_grace
            with worker_runner._StopSignals(runner.get_loop(), grace=grace) as signals:

                async def serve() -> None:
                    await signals.stop.wait()

                return runner.run(worker_runner._run_with_worker(serve, executor, serving=True))
        return runner.run(worker_runner._run_with_worker(main, executor))
