"""Tasks: a function made callable as distributed work, and the decorators
that declare one."""

import inspect
import typing
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, overload

import wrapt

from kabudachi._checks import is_count, is_non_negative_timedelta, is_positive_timedelta
from kabudachi.config import UNSET, Unset
from kabudachi.errors import TaskDefinitionError
from kabudachi.flow import BoundTask, MapStep, ReduceStep, bind_task, is_step_type
from kabudachi.handle import TaskHandle
from kabudachi.options import SubmissionOptions, submission_options
from kabudachi.registry import (
    ReducerShape,
    TaskDefinition,
    TaskKind,
    TaskRegistry,
    default_registry,
)
from kabudachi.serializers import (
    DEFAULT_SERIALIZER,
    SerializerRegistry,
    process_serializers,
)
from kabudachi.session import current_session

RESERVED_NAME_PREFIX = "kabudachi."

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
        queue: The queue tasks are sent to, instead of the process default.
        serializer: The name of the serializer that encodes the task's input
            and output.
        kind: The delivery semantics of the task.
        retries: How many more times a run that raises is replaced by a new
            attempt, at once. The handle only sees the last attempt's outcome.
            Defaults to the process's `retries`; ignored, and 0, for a task
            with `retriable=False`. An ephemeral task is never retried.
        retriable: `False` for a plain task that is not safe to run twice: a
            run that was running when its worker or task process was lost is
            orphaned instead of run again. Such a task takes no `retries`.
        timeout: How long a run may take before its body is asked to stop
            (cancelled, for an async body). A run that hits it fails with
            `TaskTimeoutError` and is retried per `retries`. `None` is no
            limit. Defaults to the process's `timeout`. Ignored by `.local()`.
        result_ttl: How long the task's record and mirrored result are kept
            once it finishes. `None` keeps no result: nothing is mirrored, and
            the record is kept only the minute the shard needs. Defaults to
            the queue's `result_ttl`, else the process's.
        cancel_grace: How long a run has to stop once asked, before it is
            failed anyway and its body abandoned. Defaults to
            the process's.
        reconnect_timeout: How long a run may go without its worker being
            heard, past the time it takes to suspect a silent worker, before
            the leader replays it on another; and so how long a worker cut
            off from its leader has to stop it. Defaults to the queue's
            `QueueOptions.reconnect_timeout`, else the shard's.
        raise_on_failed_migration: Whether a task whose input this worker
            cannot migrate to its schema version (a migration raised, or a
            bump is declared incompatible) is left for another worker
            instead of dropped. Left, it fails with `MigrationError` after
            `version_block_ttl`; dropped, it fails with `MigrationError` at
            once and the worker logs a warning. A task whose input is newer
            than the worker's version is always left.
        merge: For a coalescing task, a pure reducer `(older, newer) -> merged`
            that combines the payloads of superseded generations, oldest
            first, on the worker that claims the newest one. Without it the
            newest payload wins. For a reducer, a synchronous function
            defined at module level (or a lambda) `(left, right) -> combined`
            that a tree reduce uses to combine the results of two adjacent
            branches; every worker that imports the reducer then has the
            same one.
        associative: For a reducer, whether it may be evaluated as a tree:
            it must then take and return one type, and combining two results
            is the reducer itself.
        tree: For a reducer, whether a reduce folds chunks of the list side
            by side and combines adjacent results, instead of one item after
            another. Defaults to `associative`. A tree needs exactly one way
            to combine two results: `associative=True` or a `merge`.
        reduces: Whether the function must be a reducer, a task of two
            parameters `(item, previous)`. A task of two parameters is a
            reducer either way.
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
        queue: str | Unset = UNSET,
        serializer: str = DEFAULT_SERIALIZER,
        kind: TaskKind = TaskKind.TASK,
        retries: int | Unset = UNSET,
        retriable: bool = True,
        timeout: timedelta | None | Unset = UNSET,
        result_ttl: timedelta | None | Unset = UNSET,
        cancel_grace: timedelta | Unset = UNSET,
        merge: Callable[[Any, Any], Any] | None = None,
        drop_oldest: bool = False,
        recycle_process: bool = False,
        reconnect_timeout: timedelta | None = None,
        associative: bool = False,
        tree: bool | None = None,
        reduces: bool = False,
        raise_on_failed_migration: bool = False,
        serializers: SerializerRegistry | None = None,
    ) -> None:
        self._check_identity(func, queue, serializer)
        if retries is not UNSET:
            self._require_non_negative_count("retries", retries)
        self._check_retriable(retriable, retries, kind)

        self._check_timing(timeout, cancel_grace, reconnect_timeout, result_ttl)
        self._check_drop_oldest(drop_oldest, kind)
        self._require_bool("recycle_process", recycle_process)
        self._require_bool("raise_on_failed_migration", raise_on_failed_migration)

        task_name = self._task_name(func, name)
        types = self._read_types(task_name, func)
        input_type, _, output_type = types
        reducing = self._reducer_shape(
            task_name, kind, types, reduces, associative, tree, merge
        )
        self._check_merge(merge, kind, reducing)
        definition = TaskDefinition(
            name=task_name,
            func=func,
            kind=kind,
            serializer=serializer,
            queue=queue,
            input_type=input_type,
            output_type=output_type,
            is_async=inspect.iscoroutinefunction(self._annotated_callable(func)),
            module=getattr(func, "__module__", None) or "",
            # An ephemeral task is never run again, so the process's `retries`
            # does not apply to it.
            retries=0 if kind is TaskKind.EPHEMERAL and retries is UNSET else retries,
            retriable=retriable,
            timeout=timeout,
            result_ttl=result_ttl,
            cancel_grace=cancel_grace,
            merge=None if reducing else merge,
            drop_oldest=drop_oldest,
            recycle_process=recycle_process,
            reconnect_timeout=reconnect_timeout,
            raise_on_failed_migration=raise_on_failed_migration,
            continues=is_step_type(output_type),
            reducer=reducing,
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

        Only works while `kabudachi.run()` is running. A reducer is not
        called: it is run with `.reduce(items)`, or here with `.local`.
        """
        self._refuse_reducer("run it with .reduce(items) or .local(item, previous)")
        return current_session().submit(self._self_definition, argument)

    def _refuse_reducer(self, instead: str) -> None:
        if self._self_definition.reducer is not None:
            raise TypeError(f"{self._self_definition.name} is a reducer: {instead}")

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
        self._refuse_reducer("run it with .reduce(items)")
        if key is not None and self._self_definition.kind is not TaskKind.COALESCING:
            raise ValueError("only a coalescing task takes a key")
        chosen = submission_options(delay=delay, eta=eta, expires=expires, key=key)
        return _OptionedTask(self._self_definition, chosen)

    @property
    def map(self) -> MapStep:
        """This task run on every item of a list: `await task.map(items)` gives
        the results in the order of the items, and `task.map` is also a `flow`
        stage that takes the list the stage before it returns. Each item is an
        independent task; `on_error` is the policy of a `group`. Raises
        `TypeError` for a reducer."""
        self._refuse_reducer("run it with .reduce(items)")
        return MapStep(self._self_definition, self._self_serializers or process_serializers())

    @property
    def reduce(self) -> ReduceStep:
        """A reducer folded over a list of items: `await task.reduce(items,
        initial=..., chunk_size=...)` gives the folded value, and `task.reduce`
        is also a `flow` stage that folds the list the stage before it gives.
        Raises `TypeError` for a task that is not a reducer."""
        if self._self_definition.reducer is None:
            raise TypeError(f"{self._self_definition.name} takes one argument, so it is not a reducer")
        return ReduceStep(self._self_definition, self._self_serializers or process_serializers())

    def bind(self, *value: Any, **fields: Any) -> BoundTask:
        """This task with its input fixed, for use as a `flow` stage or called
        on its own. `bind(value)` fixes the whole input, so the bound task is
        called with no argument; `bind(field=value, ...)` sets fields on the
        message the bound task is called with, and only works when the task's
        input is a protobuf message. Raises `TypeError` or `ValueError` at
        once for a binding that cannot work, and for a reducer.
        """
        self._refuse_reducer("run it with .reduce(items)")
        serializers = self._self_serializers or process_serializers()
        return bind_task(self._self_definition, serializers, value, fields)

    def local(self, *arguments: Any) -> Any:
        """Runs the function here and now, with no runtime, scheduling or
        certification, and returns what it returns: the result itself for a
        synchronous function, an awaitable of it for an async one. A task
        takes its argument; a reducer takes `(item, previous)`.

        The arguments and the result pass through the task's serializer, as
        they would on a worker, so the body sees a copy of its input and a
        value of the wrong type is refused here rather than later. Raises
        `SerializationError` if either is not what the task declares, and
        `TypeError` for the wrong number of arguments; an error raised by the
        body is raised as it is.
        """
        definition = self._self_definition
        reducing = definition.reducer
        parameter_types = [definition.input_type]
        if reducing is not None:
            parameter_types.append(reducing.previous_type)
        if len(arguments) != len(parameter_types):
            wanted = "(item, previous)" if reducing is not None else "(argument)"
            raise TypeError(f"{definition.name}.local takes {wanted}, not {len(arguments)} arguments")
        serializers = self._self_serializers or process_serializers()
        serializer = serializers.get(definition.serializer)
        copied = [
            serializer.decode(serializer.encode(argument, value_type), value_type)
            for argument, value_type in zip(arguments, parameter_types, strict=True)
        ]
        outcome = self.__wrapped__(*copied)
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

    @staticmethod
    def _check_identity(func: Any, queue: Any, serializer: Any) -> None:
        """Checks what names the task: its function, queue and serializer."""
        if not callable(func):
            raise TaskDefinitionError(f"a task must be a callable, not {func!r}")
        if queue is not UNSET:
            Task._require_string("queue", queue)
        Task._require_string("serializer", serializer)

    @staticmethod
    def _check_merge(merge: Any, kind: TaskKind, reducing: ReducerShape | None) -> None:
        if merge is None:
            return
        if kind is not TaskKind.COALESCING and reducing is None:
            raise TypeError("only a coalescing task or a reducer takes a merge function")
        try:
            inspect.signature(merge).bind(None, None)
        except (TypeError, ValueError):
            raise TaskDefinitionError(
                f"the task merge must be a callable taking two values, not {merge!r}"
            ) from None
        if reducing is None:
            return
        if inspect.iscoroutinefunction(merge):
            raise TaskDefinitionError("a reducer's merge must be synchronous")
        if "<locals>" in getattr(merge, "__qualname__", ""):
            raise TaskDefinitionError(
                "a reducer's merge must be a module-level function or lambda, "
                "so every worker that imports the reducer has the same one"
            )

    @staticmethod
    def _check_drop_oldest(drop_oldest: Any, kind: TaskKind) -> None:
        Task._require_bool("drop_oldest", drop_oldest)
        if drop_oldest and kind is not TaskKind.COALESCING:
            raise TypeError("only a coalescing task takes drop_oldest")

    @staticmethod
    def _check_retriable(retriable: Any, retries: int | Unset, kind: TaskKind) -> None:
        Task._require_bool("retriable", retriable)
        if retriable:
            return
        if retries is not UNSET and retries > 0:
            raise TaskDefinitionError(
                "a task that is not retriable takes no retries: running it again is what it must not do"
            )
        if kind is not TaskKind.TASK:
            raise TaskDefinitionError(
                "only a plain task can be declared not retriable: an ephemeral task is never run again "
                "anyway, and a coalescing task's generations replace one another"
            )

    @staticmethod
    def _check_timing(
        timeout: Any, cancel_grace: Any, reconnect_timeout: Any, result_ttl: Any
    ) -> None:
        if timeout is not UNSET and timeout is not None and not is_positive_timedelta(timeout):
            raise TaskDefinitionError(
                f"the task timeout must be a positive timedelta or None, not {timeout!r}"
            )
        if result_ttl is not UNSET and result_ttl is not None and not is_positive_timedelta(result_ttl):
            raise TaskDefinitionError(
                f"the task result_ttl must be a positive timedelta or None, not {result_ttl!r}"
            )
        if cancel_grace is not UNSET and not is_non_negative_timedelta(cancel_grace):
            raise TaskDefinitionError(
                f"the task cancel_grace must be a non-negative timedelta, not {cancel_grace!r}"
            )
        if reconnect_timeout is not None and not is_positive_timedelta(reconnect_timeout):
            raise TaskDefinitionError(
                "the task reconnect_timeout must be a positive timedelta or None, "
                f"not {reconnect_timeout!r}"
            )

    @staticmethod
    def _require_non_negative_count(what: str, value: Any) -> None:
        if not is_count(value) or value < 0:
            raise TaskDefinitionError(
                f"the task {what} must be a non-negative integer, not {value!r}"
            )

    @staticmethod
    def _require_bool(what: str, value: Any) -> None:
        if not isinstance(value, bool):
            raise TaskDefinitionError(f"the task {what} must be True or False, not {value!r}")

    @staticmethod
    def _require_string(what: str, value: Any) -> None:
        if not isinstance(value, str) or not value.strip():
            raise TaskDefinitionError(f"the task {what} must be a non-empty string, not {value!r}")

    @classmethod
    def _task_name(cls, func: Callable[..., Any], chosen: str | Unset) -> str:
        name = cls._chosen_or_default_name(func, chosen)
        if name.startswith(RESERVED_NAME_PREFIX):
            raise TaskDefinitionError(
                f"the task name {name!r} cannot start with {RESERVED_NAME_PREFIX!r}, "
                "which is kept for kabudachi's own runs; give the task a name="
            )
        return name

    @classmethod
    def _chosen_or_default_name(cls, func: Callable[..., Any], chosen: str | Unset) -> str:
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
    def _read_types(cls, name: str, func: Callable[..., Any]) -> tuple[Any, Any, Any]:
        """The input, previous and return types of `func`, which must take one
        argument, its input, or two as a reducer: an item and the result so
        far. A task of one argument has no previous type, `UNSET`."""
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
        if len(parameters) not in (1, 2) or any(
            parameter.kind not in _ONE_ARGUMENT_KINDS for parameter in parameters
        ):
            raise TaskDefinitionError(
                f"task {name!r} must take exactly one argument, its input, "
                "or two as a reducer: an item and the result so far"
            )
        try:
            hints = typing.get_type_hints(cls._annotated_callable(func))
        except Exception as error:
            # An annotation is code, so resolving one can fail in any way.
            raise TaskDefinitionError(
                f"task {name!r} has an annotation that cannot be resolved: {error!r}"
            ) from error
        if parameters[0].name not in hints:
            raise TaskDefinitionError(f"task {name!r} has no input type annotation")
        if len(parameters) == 2 and parameters[1].name not in hints:
            raise TaskDefinitionError(f"task {name!r} has no previous type annotation")
        if "return" not in hints:
            raise TaskDefinitionError(f"task {name!r} has no return type annotation")
        previous_type = hints[parameters[1].name] if len(parameters) == 2 else UNSET
        return hints[parameters[0].name], previous_type, hints["return"]

    @classmethod
    def _reducer_shape(
        cls,
        name: str,
        kind: TaskKind,
        types: tuple[Any, Any, Any],
        reduces: bool,
        associative: Any,
        tree: Any,
        merge: Any,
    ) -> ReducerShape | None:
        """How the task folds, if it is a reducer: a task of two parameters
        `(item, previous)` returning the type of `previous`. Refuses a
        combination that a reduce could not run the same way on every
        worker."""
        item_type, previous_type, output_type = types
        if previous_type is UNSET:
            if reduces:
                raise TaskDefinitionError(
                    f"reducer {name!r} must take two parameters, an item and the result so far"
                )
            if associative is not False or tree is not None:
                raise TypeError("only a reducer, a task of two parameters, takes associative or tree")
            return None
        if kind is TaskKind.COALESCING:
            raise TaskDefinitionError(
                f"task {name!r} takes two parameters, so it is a reducer, "
                "and a reducer cannot be a coalescing task"
            )
        if kind is not TaskKind.TASK:
            raise TaskDefinitionError(
                f"reducer {name!r} must be a durable task: a reduce's steps must survive the loss of a worker"
            )
        if not isinstance(associative, bool) or not (tree is None or isinstance(tree, bool)):
            raise TaskDefinitionError(
                "a reducer's associative must be True or False, and its tree True, False or None"
            )
        if is_step_type(output_type):
            raise TaskDefinitionError(f"reducer {name!r} cannot return a step: a reduce folds values")
        if previous_type != output_type:
            raise TaskDefinitionError(
                f"reducer {name!r} must return the type of its previous parameter, "
                f"{previous_type!r}, not {output_type!r}"
            )
        if associative and item_type != previous_type:
            raise TaskDefinitionError(
                "an associative reducer folds items into a result of their own type, "
                f"but {name!r} takes {item_type!r} and returns {output_type!r}"
            )
        if associative and merge is not None:
            raise TaskDefinitionError(
                "an associative reducer takes no merge: it combines two results itself, "
                "and a tree needs exactly one way to combine two results, "
                "associative=True or a merge function, not both"
            )
        tree = associative if tree is None else tree
        if tree and not associative and merge is None:
            raise TaskDefinitionError(
                "a tree reducer needs exactly one way to combine two results, "
                "associative=True or a merge function, not neither"
            )
        if merge is not None and not tree:
            raise TaskDefinitionError("merge combines the results of a tree's branches, so it needs tree=True")
        return ReducerShape(previous_type, associative, tree, merge)

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


def declare_task(
    func: Any, kind: TaskKind, options: dict[str, Any]
) -> "Task | Callable[[Callable[..., Any]], Task]":
    """What a task decorator returns: the task, if it was given a
    function, or a decorator for one if it was given only options.

    Tasks declared this way are registered in the registry of this
    process. Raises `TypeError` if given something else.
    """
    if func is None:
        return lambda target: Task(target, registry=default_registry(), kind=kind, **options)
    if not callable(func):
        raise TypeError("a task decorator takes a function, or its options as keyword arguments")
    return Task(func, registry=default_registry(), kind=kind, **options)


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
    queue: str | Unset = ...,
    serializer: str = ...,
    retries: int | Unset = ...,
    retriable: bool = ...,
    timeout: timedelta | None | Unset = ...,
    result_ttl: timedelta | None | Unset = ...,
    cancel_grace: timedelta | Unset = ...,
    recycle_process: bool = ...,
    reconnect_timeout: timedelta | None = ...,
    raise_on_failed_migration: bool = ...,
) -> Callable[[Callable[..., Any]], Task]: ...
def task(
    func: Any = None,
    /,
    *,
    name: str | Unset = UNSET,
    queue: str | Unset = UNSET,
    serializer: str = DEFAULT_SERIALIZER,
    retries: int | Unset = UNSET,
    retriable: bool = True,
    timeout: timedelta | None | Unset = UNSET,
    result_ttl: timedelta | None | Unset = UNSET,
    cancel_grace: timedelta | Unset = UNSET,
    recycle_process: bool = False,
    reconnect_timeout: timedelta | None = None,
    raise_on_failed_migration: bool = False,
) -> Any:
    """Declares a durable task: work that survives the loss of the worker
    running it. Use it bare (`@task`) or with options (`@task(queue="gpu")`).

    The function takes exactly one argument, its input (a function of two
    parameters `(item, previous)` is a reducer: see `reducer`), and both that
    and the return value must be annotated with types its serializer
    supports. Every
    worker of a shard must import the same tasks under the same names,
    because a task is sent to a worker by name.

    The options are those of `Task`; `retries` is how many more times a run
    that raises, or times out, is tried again. `recycle_process=True`
    replaces the task process after each run of this task, once it has
    drained: its other runs finish and it exits, and only then is the fresh
    one started. `name` defaults to the function's module and name, which
    must then be defined at package scope. `serializer` must be registered
    under the same name on every worker; it is checked here if it is already
    registered, and otherwise before the task first runs.

    `retriable=False` declares a task that is not safe to run twice. A run
    that was running when its worker or task process was lost is then
    orphaned instead of run again, and awaiting it raises
    `TaskOrphanedError`. Such a task takes no `retries`.

    Raises `TaskDefinitionError` or `DuplicateTaskError` as `Task` does, and
    `TypeError` if given something other than a function or keyword options.
    """
    options = {
        "name": name,
        "queue": queue,
        "serializer": serializer,
        "retries": retries,
        "retriable": retriable,
        "timeout": timeout,
        "result_ttl": result_ttl,
        "cancel_grace": cancel_grace,
        "recycle_process": recycle_process,
        "reconnect_timeout": reconnect_timeout,
        "raise_on_failed_migration": raise_on_failed_migration,
    }
    return declare_task(func, TaskKind.TASK, options)


@overload
def reducer(func: Callable[..., Any], /) -> Task: ...
@overload
def reducer(
    *,
    associative: bool = ...,
    tree: bool | None = ...,
    merge: Callable[[Any, Any], Any] | None = ...,
    name: str | Unset = ...,
    queue: str | Unset = ...,
    serializer: str = ...,
    retries: int | Unset = ...,
    retriable: bool = ...,
    timeout: timedelta | None | Unset = ...,
    cancel_grace: timedelta | Unset = ...,
    recycle_process: bool = ...,
    reconnect_timeout: timedelta | None = ...,
    raise_on_failed_migration: bool = ...,
) -> Callable[[Callable[..., Any]], Task]: ...
def reducer(
    func: Any = None,
    /,
    *,
    associative: bool = False,
    tree: bool | None = None,
    merge: Callable[[Any, Any], Any] | None = None,
    name: str | Unset = UNSET,
    queue: str | Unset = UNSET,
    serializer: str = DEFAULT_SERIALIZER,
    retries: int | Unset = UNSET,
    retriable: bool = True,
    timeout: timedelta | None | Unset = UNSET,
    cancel_grace: timedelta | Unset = UNSET,
    recycle_process: bool = False,
    reconnect_timeout: timedelta | None = None,
    raise_on_failed_migration: bool = False,
) -> Any:
    """Declares a reducer: a durable task `(item, previous) -> previous`
    that `.reduce(items)` folds over a list, left to right, one certified
    step per item. A plain `@task` of two parameters is the same as a bare
    `@reducer`.

    `associative=True` says the reducer may be evaluated as a tree (it
    must then take and return one type), and makes `tree` default to
    True. A tree folds chunks of the list side by side and combines
    adjacent results in order: with the reducer itself
    (`reducer(item=right, previous=left)`) when associative, otherwise
    with `merge(left, right)`, a synchronous module-level function or
    lambda. A seeded tree folds every chunk from `initial`, so `initial`
    must be the combiner's identity (`0` for a sum, `[]` for a
    concatenation); this is not checked. Every worker computes the same
    tree only if it imports the same reducer.

    Takes the other options of `task`, with the same defaults (but no
    `result_ttl`: a reducer runs only as the steps of a reduce, which are
    kept with the result of the reduce), and raises
    `TaskDefinitionError` for a function that is not a reducer or an
    inconsistent combination. `raise_on_failed_migration=True` leaves a step
    of the reduce whose item this worker cannot migrate for another worker
    instead of dropping it, as for `task`.
    """
    options = {
        "name": name,
        "queue": queue,
        "serializer": serializer,
        "retries": retries,
        "retriable": retriable,
        "timeout": timeout,
        "cancel_grace": cancel_grace,
        "recycle_process": recycle_process,
        "reconnect_timeout": reconnect_timeout,
        "raise_on_failed_migration": raise_on_failed_migration,
        "associative": associative,
        "tree": tree,
        "merge": merge,
        "reduces": True,
    }
    return declare_task(func, TaskKind.TASK, options)


@overload
def ephemeral_task(func: Callable[..., Any], /) -> Task: ...
@overload
def ephemeral_task(
    *,
    name: str | Unset = ...,
    queue: str | Unset = ...,
    serializer: str = ...,
    timeout: timedelta | None | Unset = ...,
    result_ttl: timedelta | None | Unset = ...,
    cancel_grace: timedelta | Unset = ...,
    recycle_process: bool = ...,
    reconnect_timeout: timedelta | None = ...,
    raise_on_failed_migration: bool = ...,
) -> Callable[[Callable[..., Any]], Task]: ...
def ephemeral_task(
    func: Any = None,
    /,
    *,
    name: str | Unset = UNSET,
    queue: str | Unset = UNSET,
    serializer: str = DEFAULT_SERIALIZER,
    timeout: timedelta | None | Unset = UNSET,
    result_ttl: timedelta | None | Unset = UNSET,
    cancel_grace: timedelta | Unset = UNSET,
    recycle_process: bool = False,
    reconnect_timeout: timedelta | None = None,
    raise_on_failed_migration: bool = False,
) -> Any:
    """Declares a best-effort task: it is never replayed after the loss of the
    worker running it, and losing every worker may lose it.

    Takes the same arguments, and raises the same errors, as `task`, whose
    documentation also covers what every worker must agree on. Its function
    takes one argument: a reducer (two parameters) must be durable, so see
    `reducer`.
    """
    options = {
        "name": name,
        "queue": queue,
        "serializer": serializer,
        "timeout": timeout,
        "result_ttl": result_ttl,
        "cancel_grace": cancel_grace,
        "recycle_process": recycle_process,
        "reconnect_timeout": reconnect_timeout,
        "raise_on_failed_migration": raise_on_failed_migration,
    }
    return declare_task(func, TaskKind.EPHEMERAL, options)


@overload
def coalescing_task(func: Callable[..., Any], /) -> Task: ...
@overload
def coalescing_task(
    *,
    name: str | Unset = ...,
    queue: str | Unset = ...,
    serializer: str = ...,
    retries: int | Unset = ...,
    timeout: timedelta | None | Unset = ...,
    result_ttl: timedelta | None | Unset = ...,
    cancel_grace: timedelta | Unset = ...,
    merge: Callable[[Any, Any], Any] | None = ...,
    drop_oldest: bool = ...,
    recycle_process: bool = ...,
    reconnect_timeout: timedelta | None = ...,
    raise_on_failed_migration: bool = ...,
) -> Callable[[Callable[..., Any]], Task]: ...
def coalescing_task(
    func: Any = None,
    /,
    *,
    name: str | Unset = UNSET,
    queue: str | Unset = UNSET,
    serializer: str = DEFAULT_SERIALIZER,
    retries: int | Unset = UNSET,
    timeout: timedelta | None | Unset = UNSET,
    result_ttl: timedelta | None | Unset = UNSET,
    cancel_grace: timedelta | Unset = UNSET,
    merge: Callable[[Any, Any], Any] | None = None,
    drop_oldest: bool = False,
    recycle_process: bool = False,
    reconnect_timeout: timedelta | None = None,
    raise_on_failed_migration: bool = False,
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

    Takes the other arguments, and raises the errors, of `task`. Its function
    takes one argument: a function of two parameters is a reducer, which
    cannot coalesce (see `reducer`).
    """
    options = {
        "name": name,
        "queue": queue,
        "serializer": serializer,
        "retries": retries,
        "timeout": timeout,
        "result_ttl": result_ttl,
        "cancel_grace": cancel_grace,
        "merge": merge,
        "drop_oldest": drop_oldest,
        "recycle_process": recycle_process,
        "reconnect_timeout": reconnect_timeout,
        "raise_on_failed_migration": raise_on_failed_migration,
    }
    return declare_task(func, TaskKind.COALESCING, options)
