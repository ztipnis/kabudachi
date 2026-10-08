"""Tasks: a function made callable as distributed work, and the decorators
that declare one."""

import inspect
import typing
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, overload

import wrapt

from kabudachi.config import UNSET, Unset
from kabudachi.errors import TaskDefinitionError
from kabudachi.flow import BoundTask, MapStep, bind_task, is_step_type
from kabudachi.handle import TaskHandle
from kabudachi.options import SubmissionOptions, submission_options
from kabudachi.registry import TaskDefinition, TaskKind, TaskRegistry, default_registry
from kabudachi.serializers import (
    DEFAULT_SERIALIZER,
    SerializerRegistry,
    process_serializers,
)
from kabudachi.session import current_session

_ONE_ARGUMENT_KINDS = (
    inspect.Parameter.POSITIONAL_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
)


class Task(wrapt.ObjectProxy):
    """A function declared as a task. It looks and reads like the function it
    wraps, but calling it queues the work for a worker and returns a handle;
    `__wrapped__` is the function itself.

    Building one checks the function and every option, and registers the task
    by name, so a task that could never work is refused where it is written.
    Nothing is registered if anything is invalid.

    Args:
        func: The function, or an object with `__call__`.
        registry: Where the task is registered.
        name: Identifies the task to every worker. Defaults to the
            function's module and name.
        version: The version of the task's data format. Every worker must
            agree on it.
        queue: The queue tasks are sent to, instead of the process default.
        serializer: The name of the serializer that encodes the task's input
            and output.
        kind: The delivery semantics of the task.
        retries: How many more times a run that raises is replaced by a new
            attempt, at once. The handle only sees the last attempt's outcome.
        timeout: How long a run may take before its body is asked to stop
            (cancelled, for an async body). A run that hits it fails with
            `TaskTimeoutError` and is retried per `retries`. `None` is no
            limit. Ignored by `.local()`.
        cancel_grace: How long a run has to stop once asked, before it is
            failed anyway and its body abandoned. Defaults to
            the process's.
        merge: For a coalescing task, a pure reducer `(older, newer) -> merged`
            that combines the payloads of superseded generations, oldest
            first, on the worker that claims the newest one. Without it the
            newest payload wins.
        drop_oldest: For a coalescing task, whether a submission that would
            pass the hard memory limit drops this key's oldest retained
            payloads to fit, instead of being refused.
        recycle_process: Whether the task process that runs it is replaced
            by a fresh one after the run: it takes no new runs, and is
            replaced once its other runs have finished and it has exited.
            Ignored with `processes=0`.
        serializers: Where the serializer is looked up to check the task's
            types. Defaults to the registry of this process.

    Raises:
        TaskDefinitionError: If the function cannot be a task or an option is
            invalid.
        DuplicateTaskError: If the name is already taken.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        registry: TaskRegistry,
        name: str | Unset = UNSET,
        version: int = 0,
        queue: str | Unset = UNSET,
        serializer: str = DEFAULT_SERIALIZER,
        kind: TaskKind = TaskKind.TASK,
        retries: int = 0,
        timeout: timedelta | None = None,
        cancel_grace: timedelta | Unset = UNSET,
        merge: Callable[[Any, Any], Any] | None = None,
        drop_oldest: bool = False,
        recycle_process: bool = False,
        serializers: SerializerRegistry | None = None,
    ) -> None:
        if not callable(func):
            raise TaskDefinitionError(f"a task must be a callable, not {func!r}")
        if isinstance(version, bool) or not isinstance(version, int) or version < 0:
            raise TaskDefinitionError(
                f"the task version must be a non-negative integer, not {version!r}"
            )
        if queue is not UNSET:
            self._require_string("queue", queue)
        self._require_string("serializer", serializer)
        if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
            raise TaskDefinitionError(
                f"the task retries must be a non-negative integer, not {retries!r}"
            )

        self._check_timing(timeout, cancel_grace)
        self._check_merge(merge, kind)
        self._check_drop_oldest(drop_oldest, kind)
        if not isinstance(recycle_process, bool):
            raise TaskDefinitionError(
                f"the task recycle_process must be True or False, not {recycle_process!r}"
            )

        task_name = self._task_name(func, name)
        input_type, output_type = self._read_types(task_name, func)
        definition = TaskDefinition(
            name=task_name,
            func=func,
            kind=kind,
            serializer=serializer,
            version=version,
            queue=queue,
            input_type=input_type,
            output_type=output_type,
            is_async=inspect.iscoroutinefunction(self._annotated_callable(func)),
            module=getattr(func, "__module__", None) or "",
            retries=retries,
            timeout=timeout,
            cancel_grace=cancel_grace,
            merge=merge,
            drop_oldest=drop_oldest,
            recycle_process=recycle_process,
            continues=is_step_type(output_type),
        )
        self._check_serializer_supports(
            definition, process_serializers() if serializers is None else serializers
        )
        super().__init__(func)
        registry.register(definition)
        self._self_definition = definition
        self._self_serializers = serializers

    @property
    def definition(self) -> TaskDefinition:
        """What this task is: its name, types, serializer and options."""
        return self._self_definition

    def __call__(self, argument: Any) -> TaskHandle:
        """Queues the task on `argument` and returns its handle.

        Only works while `kabudachi.run()` is running.
        """
        return current_session().submit(self._self_definition, argument)

    def options(
        self,
        *,
        delay: timedelta | None = None,
        eta: datetime | None = None,
        expires: timedelta | datetime | None = None,
        key: str | None = None,
    ) -> "_OptionedTask":
        """This task with timing for one submission, called like the task:
        `task.options(delay=timedelta(minutes=5))(argument)`.

        `delay` (a duration) or `eta` (an aware time) says when the task may
        first start; neither promises a start time, only that it is not
        earlier. `expires` (a duration or an aware time) says that if the task
        has not started by then it fails with `TaskExpiredError` instead of
        running late. `key` is a coalescing task's flat key for this
        submission; a `@task` has none. Raises `ValueError` or
        `TypeError` at once if the options are wrong or contradict each other.
        """
        if key is not None and self._self_definition.kind is not TaskKind.COALESCING:
            raise ValueError("only a coalescing task takes a key")
        chosen = submission_options(delay=delay, eta=eta, expires=expires, key=key)
        return _OptionedTask(self._self_definition, chosen)

    @property
    def map(self) -> MapStep:
        """This task run on every item of a list: `await task.map(items)` gives
        the results in the order of the items, and `task.map` is also a `flow`
        stage that takes the list the stage before it returns. Each item is an
        independent task; `on_error` is the policy of a `group`."""
        return MapStep(self._self_definition, self._self_serializers or process_serializers())

    def bind(self, *value: Any, **fields: Any) -> BoundTask:
        """This task with its input fixed, for use as a `flow` stage or called
        on its own. `bind(value)` fixes the whole input, so the bound task is
        called with no argument; `bind(field=value, ...)` sets fields on the
        message the bound task is called with, and only works when the task's
        input is a protobuf message. Raises `TypeError` or `ValueError` at
        once for a binding that cannot work.
        """
        serializers = self._self_serializers or process_serializers()
        return bind_task(self._self_definition, serializers, value, fields)

    def local(self, argument: Any) -> Any:
        """Runs the function here and now, with no runtime, scheduling or
        certification, and returns what it returns: the result itself for a
        synchronous function, an awaitable of it for an async one.

        The argument and the result pass through the task's serializer, as
        they would on a worker, so the body sees a copy of its input and a
        value of the wrong type is refused here rather than later. Raises
        `SerializationError` if either is not what the task declares; an
        error raised by the body is raised as it is.
        """
        definition = self._self_definition
        serializers = self._self_serializers or process_serializers()
        serializer = serializers.get(definition.serializer)
        copied = serializer.decode(
            serializer.encode(argument, definition.input_type), definition.input_type
        )
        outcome = self.__wrapped__(copied)
        if definition.continues:
            return outcome  # a step to run, not a result to check

        def checked(value: Any) -> Any:
            return serializer.decode(
                serializer.encode(value, definition.output_type), definition.output_type
            )

        if definition.is_async:

            async def finish() -> Any:
                return checked(await outcome)

            return finish()
        return checked(outcome)

    @classmethod
    def declare(
        cls,
        func: Any,
        kind: TaskKind,
        options: dict[str, Any],
    ) -> "Task | Callable[[Callable[..., Any]], Task]":
        """What a task decorator returns: the task, if it was given a
        function, or a decorator for one if it was given only options.

        Tasks declared this way are registered in the registry of this
        process. Raises `TypeError` if given something else.
        """
        if func is None:
            return lambda target: cls(target, registry=default_registry(), kind=kind, **options)
        if not callable(func):
            raise TypeError(
                "a task decorator takes a function, or its options as keyword arguments"
            )
        return cls(func, registry=default_registry(), kind=kind, **options)

    @staticmethod
    def _check_merge(merge: Any, kind: TaskKind) -> None:
        if merge is None:
            return
        if kind is not TaskKind.COALESCING:
            raise TypeError("only a coalescing task takes a merge reducer")
        try:
            inspect.signature(merge).bind(None, None)
        except (TypeError, ValueError):
            raise TaskDefinitionError(
                f"the task merge must be a callable taking (older, newer), not {merge!r}"
            ) from None

    @staticmethod
    def _check_drop_oldest(drop_oldest: Any, kind: TaskKind) -> None:
        if not isinstance(drop_oldest, bool):
            raise TaskDefinitionError(
                f"the task drop_oldest must be True or False, not {drop_oldest!r}"
            )
        if drop_oldest and kind is not TaskKind.COALESCING:
            raise TypeError("only a coalescing task takes drop_oldest")

    @staticmethod
    def _check_timing(timeout: Any, cancel_grace: Any) -> None:
        if timeout is not None and (
            not isinstance(timeout, timedelta) or timeout <= timedelta(0)
        ):
            raise TaskDefinitionError(
                f"the task timeout must be a positive timedelta or None, not {timeout!r}"
            )
        if cancel_grace is not UNSET and (
            not isinstance(cancel_grace, timedelta) or cancel_grace < timedelta(0)
        ):
            raise TaskDefinitionError(
                f"the task cancel_grace must be a non-negative timedelta, not {cancel_grace!r}"
            )

    @staticmethod
    def _require_string(what: str, value: Any) -> None:
        if not isinstance(value, str) or not value.strip():
            raise TaskDefinitionError(f"the task {what} must be a non-empty string, not {value!r}")

    @classmethod
    def _task_name(cls, func: Callable[..., Any], chosen: str | Unset) -> str:
        if chosen is not UNSET:
            cls._require_string("name", chosen)
            return chosen
        qualified = getattr(func, "__qualname__", None)
        module = getattr(func, "__module__", None)
        if qualified is None or module is None:
            raise TaskDefinitionError(f"{func!r} has no name of its own; give the task a name=")
        if "<locals>" in qualified or "<lambda>" in qualified:
            raise TaskDefinitionError(
                f"{qualified} is not defined at package scope, so workers cannot find it; "
                "define it at the top level of a module, or give the task a name="
            )
        return f"{module}.{qualified}"

    @staticmethod
    def _annotated_callable(func: Callable[..., Any]) -> Callable[..., Any]:
        """Where a callable's annotations live: on a function or method
        itself, on `__call__` for an object that can be called."""
        if inspect.isfunction(func) or inspect.ismethod(func):
            return func
        return type(func).__call__

    @classmethod
    def _read_types(cls, name: str, func: Callable[..., Any]) -> tuple[Any, Any]:
        """The input and return types of `func`, which must take exactly one argument."""
        if inspect.isclass(func):
            raise TaskDefinitionError(
                f"task {name!r} is a class; a task is a function, or an object with __call__"
            )
        try:
            parameters = list(inspect.signature(func).parameters.values())
        except (TypeError, ValueError) as error:
            raise TaskDefinitionError(
                f"task {name!r} has no signature that can be inspected: {error}"
            ) from error
        if len(parameters) != 1 or parameters[0].kind not in _ONE_ARGUMENT_KINDS:
            raise TaskDefinitionError(f"task {name!r} must take exactly one argument, its input")
        try:
            hints = typing.get_type_hints(cls._annotated_callable(func))
        except Exception as error:
            # An annotation is code, so resolving one can fail in any way.
            raise TaskDefinitionError(
                f"task {name!r} has an annotation that cannot be resolved: {error!r}"
            ) from error
        if parameters[0].name not in hints:
            raise TaskDefinitionError(f"task {name!r} has no input type annotation")
        if "return" not in hints:
            raise TaskDefinitionError(f"task {name!r} has no return type annotation")
        return hints[parameters[0].name], hints["return"]

    @staticmethod
    def _check_serializer_supports(
        definition: TaskDefinition, serializers: SerializerRegistry
    ) -> None:
        """Refuses a task whose types its serializer cannot handle. A
        serializer that is not registered here, or cannot run here, is not
        checked: it may be registered later, and it is checked again before
        the task first runs."""
        backend = serializers.find(definition.serializer)
        if backend is None or not backend.available():
            return
        for role, value_type in definition.types_unsupported_by(backend):
            raise TaskDefinitionError(
                f"task {definition.name!r}: the {definition.serializer!r} serializer "
                f"does not support the {role} type {value_type!r}"
            )


class _OptionedTask:
    """A task and the timing of one submission of it. Calling it submits."""

    def __init__(self, definition: TaskDefinition, options: SubmissionOptions) -> None:
        self._definition = definition
        self._options = options

    def __call__(self, argument: Any) -> TaskHandle:
        return current_session().submit(self._definition, argument, self._options)


@overload
def task(func: Callable[..., Any], /) -> Task: ...
@overload
def task(
    *,
    name: str | Unset = ...,
    version: int = ...,
    queue: str | Unset = ...,
    serializer: str = ...,
    retries: int = ...,
    timeout: timedelta | None = ...,
    cancel_grace: timedelta | Unset = ...,
    recycle_process: bool = ...,
) -> Callable[[Callable[..., Any]], Task]: ...
def task(
    func: Any = None,
    /,
    *,
    name: str | Unset = UNSET,
    version: int = 0,
    queue: str | Unset = UNSET,
    serializer: str = DEFAULT_SERIALIZER,
    retries: int = 0,
    timeout: timedelta | None = None,
    cancel_grace: timedelta | Unset = UNSET,
    recycle_process: bool = False,
) -> Any:
    """Declares a durable task: work that survives the loss of the worker
    running it. Use it bare (`@task`) or with options (`@task(queue="gpu")`).

    The function takes exactly one argument, its input, and both that and the
    return value must be annotated with types its serializer supports. Every
    worker of a shard must import the same tasks under the same names,
    because a task is sent to a worker by name.

    The options are those of `Task`; `retries` is how many more times a run
    that raises, or times out, is tried again. `recycle_process=True` replaces the
    task process after each run of this task, once it has drained: its other
    runs finish and it exits, and only then is the fresh one started. `name`
    defaults to the function's module and name, which must then be defined at package scope. `serializer` must
    be registered under the same name on every worker; it is checked here if
    it is already registered, and otherwise before the task first runs.

    Raises `TaskDefinitionError` or `DuplicateTaskError` as `Task` does, and
    `TypeError` if given something other than a function or keyword options.
    """
    options = {
        "name": name,
        "version": version,
        "queue": queue,
        "serializer": serializer,
        "retries": retries,
        "timeout": timeout,
        "cancel_grace": cancel_grace,
        "recycle_process": recycle_process,
    }
    return Task.declare(func, TaskKind.TASK, options)


@overload
def ephemeral_task(func: Callable[..., Any], /) -> Task: ...
@overload
def ephemeral_task(
    *,
    name: str | Unset = ...,
    version: int = ...,
    queue: str | Unset = ...,
    serializer: str = ...,
    timeout: timedelta | None = ...,
    cancel_grace: timedelta | Unset = ...,
    recycle_process: bool = ...,
) -> Callable[[Callable[..., Any]], Task]: ...
def ephemeral_task(
    func: Any = None,
    /,
    *,
    name: str | Unset = UNSET,
    version: int = 0,
    queue: str | Unset = UNSET,
    serializer: str = DEFAULT_SERIALIZER,
    timeout: timedelta | None = None,
    cancel_grace: timedelta | Unset = UNSET,
    recycle_process: bool = False,
) -> Any:
    """Declares a best-effort task: it is never replayed after the loss of the
    worker running it, and losing every worker may lose it.

    Takes the same arguments, and raises the same errors, as `task`, whose
    documentation also covers what every worker must agree on.
    """
    options = {
        "name": name,
        "version": version,
        "queue": queue,
        "serializer": serializer,
        "timeout": timeout,
        "cancel_grace": cancel_grace,
        "recycle_process": recycle_process,
    }
    return Task.declare(func, TaskKind.EPHEMERAL, options)


@overload
def coalescing_task(func: Callable[..., Any], /) -> Task: ...
@overload
def coalescing_task(
    *,
    name: str | Unset = ...,
    version: int = ...,
    queue: str | Unset = ...,
    serializer: str = ...,
    retries: int = ...,
    timeout: timedelta | None = ...,
    cancel_grace: timedelta | Unset = ...,
    merge: Callable[[Any, Any], Any] | None = ...,
    drop_oldest: bool = ...,
    recycle_process: bool = ...,
) -> Callable[[Callable[..., Any]], Task]: ...
def coalescing_task(
    func: Any = None,
    /,
    *,
    name: str | Unset = UNSET,
    version: int = 0,
    queue: str | Unset = UNSET,
    serializer: str = DEFAULT_SERIALIZER,
    retries: int = 0,
    timeout: timedelta | None = None,
    cancel_grace: timedelta | Unset = UNSET,
    merge: Callable[[Any, Any], Any] | None = None,
    drop_oldest: bool = False,
    recycle_process: bool = False,
) -> Any:
    """Declares continuously replaced work: a newer pending submission with
    the same key supersedes an older one, but never a running one, and only
    one generation of a key runs at a time.

    A superseded submission never runs, and its handle raises
    `TaskSupersededError`. Its payload is folded, oldest first, into the
    newest generation's input with `merge=` (a pure `(older, newer)`
    reducer; by default the newest payload wins) on the worker that claims
    it. The default key is shared by every submission; `.options(key=...)`
    gives a submission its own. A timeout fails the generation and it is not
    requeued.

    `drop_oldest=True` opts in to losing this key's oldest retained payloads,
    oldest first, when a submission would pass the hard memory limit, instead
    of refusing it with `BackpressureError`. It is never the
    default: a dropped payload is never folded.

    Takes the other arguments, and raises the errors, of `task`.
    """
    options = {
        "name": name,
        "version": version,
        "queue": queue,
        "serializer": serializer,
        "retries": retries,
        "timeout": timeout,
        "cancel_grace": cancel_grace,
        "merge": merge,
        "drop_oldest": drop_oldest,
        "recycle_process": recycle_process,
    }
    return Task.declare(func, TaskKind.COALESCING, options)
