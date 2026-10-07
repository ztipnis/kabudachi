"""What calling a task gives back."""

import asyncio
import concurrent.futures
import contextvars
import inspect
import logging
from collections.abc import Awaitable, Callable, Generator
from typing import Any, Protocol

LOCAL_SHARD_ID = "local"

_logger = logging.getLogger("kabudachi")


# Callback coroutines started on a running loop, held so they are not collected mid-run.
_inline_callbacks: set["asyncio.Task[None]"] = set()


async def _await_callback(outcome: Awaitable[Any]) -> None:
    """Awaits what an async callback returned; a failure is logged, by type only."""
    try:
        await outcome
    except Exception as error:
        _logger.warning("a task callback failed with %s", type(error).__name__)


def run_callback_inline(function: Callable[[Any], Any], value: Any) -> None:
    """Calls `function(value)` here and now, for a handle with no run to run it
    for it. An async `function` is scheduled on the running loop, or run to
    completion on a new one if there is none. A failure is logged, by type
    only, and goes no further."""
    try:
        outcome = function(value)
        if not inspect.isawaitable(outcome):
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(_await_callback(outcome))
        else:
            task = loop.create_task(_await_callback(outcome))
            _inline_callbacks.add(task)
            task.add_done_callback(_inline_callbacks.discard)
    except Exception as error:
        _logger.warning("a task callback failed with %s", type(error).__name__)


class WaitObserver(Protocol):
    """Told when the task body that is running starts and stops waiting for
    another task, so the worker can let another task use its place meanwhile."""

    def waiting_started(self) -> None:
        ...

    def waiting_finished(self) -> None:
        ...


# Set inside a running task body. Awaiting a handle from there hands the
# body's place back to the worker while it waits: otherwise a task waiting
# for a task it called could hold the place that task needs.
current_body: contextvars.ContextVar[WaitObserver | None] = contextvars.ContextVar(
    "kabudachi_current_body", default=None
)


class TaskHandle:
    """A submitted task. Await it for the task's result.

    The result is only ever delivered once the leader has certified it. If the
    task raised, awaiting the handle raises that error. It can be awaited more
    than once, from any event loop, and one awaiter giving up (a timeout, a
    cancellation) does not affect the task or any other awaiter.
    """

    def __init__(
        self,
        task_id: str,
        canceller: Callable[[str], bool] | None = None,
        callback_runner: Callable[[Callable[[Any], Any], Any], None] = run_callback_inline,
    ) -> None:
        self.task_id = task_id
        self.shard_id = LOCAL_SHARD_ID
        self._canceller = canceller
        self._callback_runner = callback_runner
        # Thread-safe, because the result can arrive from any thread.
        self._outcome: concurrent.futures.Future[Any] = concurrent.futures.Future()

    def done(self) -> bool:
        """Whether the task has finished, successfully or not."""
        return self._outcome.done()

    def callback(self, function: Callable[[Any], Any]) -> "TaskHandle":
        """Calls `function` with the task's result once the leader has
        certified it, and returns this handle. Callbacks are for reactions
        that may be lost, like metrics or a notification: one
        runs only for a task that succeeded, is held by the run even if this
        handle is dropped, and is lost if the run ends first. `function` may
        be async; a synchronous one runs off the event loop, so it may block.

        A callback that raises is logged, by the error's type only, and
        changes nothing: not the task's outcome, and not any other callback.
        Callbacks run in the order they were added. Raises `TypeError` if
        `function` cannot be called.
        """
        if not callable(function):
            raise TypeError(f"a callback must be callable, not {function!r}")

        def when_settled(outcome: "concurrent.futures.Future[Any]") -> None:
            if outcome.cancelled() or outcome.exception() is not None:
                return
            self._callback_runner(function, outcome.result())

        self._outcome.add_done_callback(when_settled)
        return self

    def cancel(self) -> bool:
        """Cancels the task, whatever it is doing, and says whether it did.

        A task that has not started never does; one that is running has its
        body asked to stop, and nothing it does afterwards counts. Awaiting the
        handle then raises `TaskCancelledError`. Returns `False`, and changes
        nothing, if the task has already finished. Safe to call from any
        thread; awaiting a handle and cancelling it are separate, so an
        awaiter giving up never cancels the task.
        """
        if self._outcome.done() or self._canceller is None:
            return False
        return self._canceller(self.task_id)

    def __await__(self) -> Generator[Any, None, Any]:
        return self._wait().__await__()

    async def _wait(self) -> Any:
        body = current_body.get()
        if body is None or self._outcome.done():
            return await self._outcome_future()
        body.waiting_started()
        try:
            return await self._outcome_future()
        finally:
            body.waiting_finished()

    def _outcome_future(self) -> "asyncio.Future[Any]":
        # Shielded: cancelling this await must not cancel the shared outcome.
        return asyncio.shield(asyncio.wrap_future(self._outcome))

    def _resolve(self, value: Any) -> None:
        """Settles the handle with `value`, unless it is already settled."""
        try:
            self._outcome.set_result(value)
        except concurrent.futures.InvalidStateError:
            pass

    def _fail(self, error: BaseException) -> None:
        """Settles the handle with `error`, unless it is already settled."""
        try:
            self._outcome.set_exception(error)
        except concurrent.futures.InvalidStateError:
            pass
