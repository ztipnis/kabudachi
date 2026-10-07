"""One running task body, and holding it to its timeout.

A body can be asked to stop but not always made to: an async body may ignore
cancellation, and a synchronous body in a thread cannot be interrupted at all.
So a body that does not stop is abandoned, not killed: its run
fails, and it is left to finish on its own, its outcome discarded.
"""

import asyncio
from concurrent.futures import Executor
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from asgiref.sync import sync_to_async

from kabudachi.errors import TaskTimeoutError
from kabudachi.registry import TaskDefinition


@dataclass
class RunningBody:
    """A body that has started."""

    outcome: "asyncio.Task[Any]"
    """Ends with the body's value or error, or cancelled if it was asked to
    stop. It can end before the body has really stopped."""

    exited: "asyncio.Future[None]"
    """Done once the body has really stopped: for a synchronous body, once its
    thread has returned, which cancelling `outcome` does not wait for."""


def start_body(definition: TaskDefinition, argument: Any, threads: Executor) -> RunningBody:
    """Starts running `definition` on `argument`, as a task for an async
    body and in `threads` for a synchronous one."""
    loop = asyncio.get_running_loop()
    exited: asyncio.Future[None] = loop.create_future()

    def mark_exited() -> None:
        if not exited.done():
            exited.set_result(None)

    if definition.is_async:
        outcome = loop.create_task(definition.func(argument))
        outcome.add_done_callback(lambda _: mark_exited())
    else:
        func = definition.func

        def in_thread(value: Any) -> Any:
            try:
                return func(value)
            finally:
                try:
                    loop.call_soon_threadsafe(mark_exited)
                except RuntimeError:
                    # The loop is gone, so there is nobody to tell.
                    pass

        # thread_sensitive is off: the default runs every call on one shared
        # thread, so synchronous tasks would wait for each other.
        run_in_thread = sync_to_async(in_thread, thread_sensitive=False, executor=threads)
        outcome = loop.create_task(run_in_thread(argument))
    # An outcome nobody awaits any more (the body was abandoned) must not be
    # logged as never retrieved.
    outcome.add_done_callback(lambda done: done.cancelled() or done.exception())
    return RunningBody(outcome, exited)


async def run_within(
    body: RunningBody, timeout: timedelta | None, cancel_grace: timedelta
) -> Any:
    """The body's value, or its own error. If it takes longer than `timeout`
    it is asked to stop, and `TaskTimeoutError` is raised as soon as it has
    stopped or `cancel_grace` has passed, whichever is first. Afterwards
    `body.exited` says whether it is still running."""
    if timeout is None:
        return await body.outcome
    done, _ = await asyncio.wait({body.outcome}, timeout=timeout.total_seconds())
    if done:
        return body.outcome.result()
    body.outcome.cancel()
    await asyncio.wait({body.exited}, timeout=cancel_grace.total_seconds())
    raise TaskTimeoutError(f"the task ran longer than its timeout of {timeout}")
