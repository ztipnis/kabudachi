"""Lifecycle hooks: application code that runs where task bodies run, to set
up each process and to prepare and clean up around each run.

A `process_init` hook runs once in each task process before it takes a run
(with `processes=0`, once in this process before the worker starts). A
`before_run` hook runs before each run of a task on one of its queues, and an
`after_run` hook after it, with what the body returned or raised.

A run hook runs on the thread the body runs on: a synchronous body's own
thread, so state a hook keeps per thread (a database connection, say) is the
body's, and the event loop for an async body, in the body's own asyncio task.
An async hook around a synchronous body runs on the loop while the body's
thread waits for it; a synchronous hook around an async body blocks the loop
while it runs.

Hooks are declared at package scope, like tasks, so a task process that
imports the task modules declares them too. They are kept for the life of
the process.
"""

import asyncio
import enum
import inspect
import logging
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from asgiref.sync import async_to_sync

from kabudachi.errors import StartupError

_logger = logging.getLogger("kabudachi")


@dataclass(frozen=True)
class RunContext:
    """The run a `before_run` or `after_run` hook is called for."""

    task_name: str
    run_id: str
    attempt: int
    """1 for a task's first run, then one more for each retry."""


class HookKind(enum.Enum):
    """When a lifecycle hook runs."""

    PROCESS_INIT = "process_init"
    BEFORE_RUN = "before_run"
    AFTER_RUN = "after_run"


# What each kind of hook is called with, in order.
_PARAMETERS: dict[HookKind, tuple[str, ...]] = {
    HookKind.PROCESS_INIT: (),
    HookKind.BEFORE_RUN: ("context",),
    HookKind.AFTER_RUN: ("context", "outcome"),
}


@dataclass(frozen=True)
class LifecycleHook:
    """One declared hook."""

    kind: HookKind
    func: Callable[..., Any]
    queues: frozenset[str] | None
    """The queues whose runs it is called for; `None` is every queue."""
    name: str
    """Its module and qualified name, the same in every process that imports it."""
    module: str
    is_async: bool

    @property
    def described(self) -> str:
        return f"{self.kind.value} hook {self.name}"


class HookRegistry:
    """Hooks, in the order they were declared."""

    def __init__(self) -> None:
        self._hooks: list[LifecycleHook] = []

    def register(self, hook: LifecycleHook) -> None:
        """Adds `hook`. Raises `ValueError` if a hook of the same kind and
        name is already declared, and then keeps the one that was there."""
        if any(existing.described == hook.described for existing in self._hooks):
            raise ValueError(
                f"{hook.described} is already declared; hooks are found by module and "
                "qualified name, so give each its own named function (lambdas in one "
                "scope share a name, as do any two partials)"
            )
        self._hooks.append(hook)

    def all(self) -> tuple[LifecycleHook, ...]:
        return tuple(self._hooks)

    def of_kind(self, kind: HookKind, queue: str | None = None) -> tuple[LifecycleHook, ...]:
        """The hooks of `kind`, in the order declared; given `queue`, only
        those called for that queue's runs."""
        return tuple(
            hook
            for hook in self._hooks
            if hook.kind is kind
            and (queue is None or hook.queues is None or queue in hook.queues)
        )


_default_hooks = HookRegistry()


def default_hooks() -> HookRegistry:
    """The registry the hook decorators add to."""
    return _default_hooks


def process_init(func: Any) -> Any:
    """Declares a hook that prepares a process for running task bodies:
    `@kabudachi.process_init`. It runs once in each task process, after the
    process imports the task modules and before it takes a run; with
    `processes=0`, once in this process before the worker starts. It takes
    no arguments and may be async. It belongs to the process, not to a
    queue: every task process runs it.

    If it raises when the task processes first start, `kabudachi.run()`
    raises `StartupError` naming it. A process started later to replace one,
    whose hook raises, is started again after a wait that grows each time.

    Returns the function itself. Raises `TypeError` if `func` cannot be
    called with no arguments, and `ValueError` if the hook is already
    declared.
    """
    return _register(HookKind.PROCESS_INIT, func, None)


def before_run(func: Any = None, /, *, queues: Iterable[str] | None = None) -> Any:
    """Declares a hook called before each run of a task on one of `queues`
    (by default every queue), with the run's `RunContext`. It may be async,
    and runs on the thread the body runs on, in the process that runs it.

    If it raises, the run fails with its error and the task's retries
    apply; the hooks declared after it and the body do not run, and the
    `after_run` hooks are called with that error.

    Returns the function itself. Raises `TypeError` if `func` cannot be
    called with one argument or `queues` is not a list of queue names, and
    `ValueError` if a queue name is blank or the hook is already declared.
    """
    return _declare(HookKind.BEFORE_RUN, func, queues)


def after_run(func: Any = None, /, *, queues: Iterable[str] | None = None) -> Any:
    """Declares a hook called after each run of a task on one of `queues`
    (by default every queue), with the run's `RunContext` and its outcome:
    what the body returned, or the exception the body or a `before_run`
    hook raised (a cancellation included). It may be async, runs where
    `before_run` hooks run, and finishes before the run's result is sent.

    A synchronous body cannot be stopped: one that ran past its time limit,
    or was cancelled, and returns later still gives this hook what it
    returned, though its run has already ended with `TaskTimeoutError` or
    `TaskCancelledError`.

    If it raises, the error is logged by its type, the hooks declared after
    it still run, and the run's result stands; the task process is then
    replaced by a fresh one once its runs finish, since what the hook did
    not clean up may be left in it. With `processes=0` the error is only
    logged: nothing is replaced. A hook that raises what is not an
    `Exception` (`KeyboardInterrupt`, or a cancellation) fails the same way,
    and is raised again once the other hooks have run, unless the run had
    already failed, whose error then stands.

    Returns the function itself. Raises `TypeError` if `func` cannot be
    called with two arguments or `queues` is not a list of queue names, and
    `ValueError` if a queue name is blank or the hook is already declared.
    """
    return _declare(HookKind.AFTER_RUN, func, queues)


async def initialize_process(hooks: HookRegistry) -> None:
    """Runs every `process_init` hook, in the order declared: a synchronous
    one on this thread, an async one on the running loop. Raises
    `StartupError` naming the first that raises, `SystemExit` and
    `KeyboardInterrupt` included; the rest do not run."""
    for hook in hooks.of_kind(HookKind.PROCESS_INIT):
        try:
            if hook.is_async:
                await hook.func()
            else:
                hook.func()
        except asyncio.CancelledError:
            raise  # the start itself was stopped
        except BaseException as error:
            raise StartupError(
                f"{hook.described} raised {type(error).__name__}: {error}"
            ) from error


@dataclass(frozen=True)
class RunHooks:
    """The hooks around one run, and who to tell when they leave the process
    unfit for more runs."""

    context: RunContext
    before: tuple[LifecycleHook, ...]
    after: tuple[LifecycleHook, ...]
    recycle: Callable[[], None]
    """Called, from any thread, once an `after_run` hook has raised."""
    condemn: Callable[[], None]
    """Called, from any thread, when the body or a hook raised `SystemExit`
    or `KeyboardInterrupt`: the process should take no more runs."""

    def around_sync(self, call: Callable[[], Any]) -> Any:
        """Calls the hooks and `call()` on this thread, which is not the event
        loop's thread; an async hook runs on the loop while this thread
        waits. Returns what `call` returned, or raises what it or a
        `before_run` hook raised."""
        try:
            for hook in self.before:
                _call_from_thread(hook, self.context)
            outcome = call()
        except BaseException as error:
            # The run's own error stands over any an after_run hook raised.
            self._after_from_thread(error)
            raise
        escaped = self._after_from_thread(outcome)
        if escaped is not None:
            raise escaped
        return outcome

    async def around_async(self, call: Callable[[], Awaitable[Any]]) -> Any:
        """Runs the hooks and awaits `call()` on the running loop. Returns
        what `call` returned, or raises what it or a `before_run` hook
        raised."""
        try:
            for hook in self.before:
                await _call_on_loop(hook, self.context)
            outcome = await call()
        except BaseException as error:
            # The run's own error stands over any an after_run hook raised.
            await self._after_on_loop(error)
            raise
        escaped = await self._after_on_loop(outcome)
        if escaped is not None:
            raise escaped
        return outcome

    def _after_from_thread(self, outcome: Any) -> BaseException | None:
        """Calls every `after_run` hook, whichever raise. Returns the first
        error a hook raised that is not an `Exception`, for the caller to
        raise again."""
        failed = False
        escaped: BaseException | None = None
        for hook in self.after:
            try:
                _call_from_thread(hook, self.context, outcome)
            except BaseException as error:
                self._cleanup_failed(hook, error)
                failed = True
                if escaped is None and not isinstance(error, Exception):
                    escaped = error
        if failed:
            self.recycle()
        return escaped

    async def _after_on_loop(self, outcome: Any) -> BaseException | None:
        """As `_after_from_thread`, on the running loop."""
        failed = False
        escaped: BaseException | None = None
        for hook in self.after:
            try:
                await _call_on_loop(hook, self.context, outcome)
            except BaseException as error:
                self._cleanup_failed(hook, error)
                failed = True
                if escaped is None and not isinstance(error, Exception):
                    escaped = error
        if failed:
            self.recycle()
        return escaped

    def _cleanup_failed(self, hook: LifecycleHook, error: BaseException) -> None:
        # The type only: an error's message can hold task input, which only
        # DEBUG logs. One that is not an `Exception` fails the run instead.
        _logger.warning(
            "%s raised %s after a run of %s%s",
            hook.described,
            type(error).__name__,
            self.context.task_name,
            "; the run's result stands" if isinstance(error, Exception) else "",
        )
        _logger.debug("%s raised", hook.described, exc_info=error)


def hooks_for_run(
    registry: HookRegistry,
    context: RunContext,
    queue: str,
    *,
    recycle: Callable[[], None],
    condemn: Callable[[], None],
) -> RunHooks:
    """The hooks of `registry` called around the run `context` of a task on `queue`."""
    return RunHooks(
        context,
        registry.of_kind(HookKind.BEFORE_RUN, queue),
        registry.of_kind(HookKind.AFTER_RUN, queue),
        recycle,
        condemn,
    )


def _call_from_thread(hook: LifecycleHook, *arguments: Any) -> None:
    if hook.is_async:
        # Runs on the loop that started this thread, which waits meanwhile.
        async_to_sync(hook.func)(*arguments)
    else:
        hook.func(*arguments)


async def _call_on_loop(hook: LifecycleHook, *arguments: Any) -> None:
    if hook.is_async:
        await hook.func(*arguments)
    else:
        hook.func(*arguments)


def _declare(kind: HookKind, func: Any, queues: Any) -> Any:
    chosen = _queue_names(queues)
    if func is None:
        return lambda target: _register(kind, target, chosen)
    return _register(kind, func, chosen)


def _register(kind: HookKind, func: Any, queues: frozenset[str] | None) -> Any:
    if not callable(func):
        raise TypeError(f"a {kind.value} hook must be callable, not {func!r}")
    module = getattr(func, "__module__", None) or type(func).__module__
    qualified = getattr(func, "__qualname__", None) or type(func).__qualname__
    name = f"{module}.{qualified}"
    parameters = _PARAMETERS[kind]
    try:
        inspect.signature(func).bind(*parameters)
    except TypeError:
        expected = f"({', '.join(parameters)})" if parameters else "no arguments"
        raise TypeError(f"{kind.value} hook {name} must take {expected}") from None
    except ValueError:
        pass  # no signature to inspect, as for some builtins: it is trusted
    is_async = inspect.iscoroutinefunction(func) or inspect.iscoroutinefunction(
        getattr(type(func), "__call__", None)
    )
    default_hooks().register(LifecycleHook(kind, func, queues, name, module, is_async))
    return func


def _queue_names(queues: Any) -> frozenset[str] | None:
    if queues is None:
        return None
    if isinstance(queues, str) or not isinstance(queues, Iterable):
        raise TypeError(f"queues must be a list of queue names, not {queues!r}")
    names = tuple(queues)
    if not names or not all(isinstance(name, str) and name.strip() for name in names):
        raise ValueError(f"queues must be a list of queue names, not {queues!r}")
    return frozenset(names)
