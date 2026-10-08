"""Running one task body, from its serialized input to its encoded result.

A body can be asked to stop but not always made to: an async body may ignore
cancellation, and a synchronous body in a thread cannot be interrupted at all.
So a body that does not stop is abandoned, not killed: its run
fails, and it is left to finish on its own, its outcome discarded.
"""

import asyncio
import functools
from collections.abc import Sequence
from concurrent.futures import Executor
from dataclasses import dataclass
from typing import Any

from asgiref.sync import sync_to_async

from kabudachi.errors import UnknownTaskError
from kabudachi.lifecycle import RunHooks
from kabudachi.registry import TaskDefinition, TaskRegistry
from kabudachi.serializers import Serializer, SerializerRegistry


@dataclass
class RunningBody:
    """A body that has started."""

    outcome: "asyncio.Future[Any]"
    """Ends with the encoded result (or the returned step), the body's error,
    or cancelled if it was asked to stop. It can end before the body has
    really stopped."""

    exited: "asyncio.Future[None]"
    """Done once the body has really stopped: for a synchronous body, once its
    thread has returned, which cancelling `outcome` does not wait for."""


def start_body(
    definition: TaskDefinition, argument: Any, threads: Executor, hooks: RunHooks
) -> RunningBody:
    """Starts running `definition` on `argument` between its run hooks: as a
    task for an async body, and in `threads` for a synchronous one, whose
    hooks run in its thread."""
    loop = asyncio.get_running_loop()
    exited: asyncio.Future[None] = loop.create_future()

    def mark_exited() -> None:
        if not exited.done():
            exited.set_result(None)

    if definition.is_async:
        outcome = loop.create_task(hooks.around_async(lambda: definition.func(argument)))
        outcome.add_done_callback(lambda _: mark_exited())
    else:
        func = definition.func

        def in_thread(value: Any) -> Any:
            try:
                return hooks.around_sync(lambda: func(value))
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


def task_named(
    registry: TaskRegistry, definition_id: str, source_version: int | None = None
) -> TaskDefinition:
    """The task registered as `definition_id`, at `source_version` if given.
    Raises `UnknownTaskError` if this process has no such task, or has it at
    another version."""
    definition = registry.get(definition_id)
    if definition is None:
        raise UnknownTaskError(f"this process has no task named {definition_id!r}")
    if source_version is not None and definition.version != source_version:
        raise UnknownTaskError(
            f"this process has task {definition_id!r} at version {definition.version}, "
            f"not {source_version}"
        )
    return definition


def fold_payloads(
    definition: TaskDefinition, serializer: Serializer, payloads: Sequence[bytes]
) -> Any:
    """Decodes `payloads` and folds them, oldest first, with the task's merge
    (the newest wins without one). Runs on a worker, never on the leader,
    because the leader never runs user code."""
    values = [serializer.decode(payload, definition.input_type) for payload in payloads]
    if len(values) == 1:
        return values[0]
    merge = definition.merge or (lambda older, newer: newer)
    return functools.reduce(merge, values)


def run_serialized(
    registry: TaskRegistry,
    serializers: SerializerRegistry,
    threads: Executor,
    definition_id: str,
    source_version: int,
    chain: Sequence[bytes],
    serialized_input: bytes,
    hooks: RunHooks,
    after: "asyncio.Future[None] | None" = None,
) -> RunningBody:
    """Starts the task `definition_id` on its serialized input, folded onto
    the payloads of the generations it superseded (`chain`, oldest first),
    between the run hooks in `hooks`, once `after`, if given, is done. The
    outcome is the encoded result, or, for a task that returns a step, the
    step itself. Started in the caller's context, so the body sees the
    caller's context variables."""
    loop = asyncio.get_running_loop()
    exited: asyncio.Future[None] = loop.create_future()

    async def run() -> Any:
        try:
            if after is not None:
                await asyncio.wait({after})
            definition = task_named(registry, definition_id, source_version)
            serializer = serializers.get(definition.serializer)
            argument = fold_payloads(definition, serializer, [*chain, serialized_input])
            body = start_body(definition, argument, threads, hooks)
        except BaseException:
            # Nothing started, so nothing is left running.
            _mark_done(exited)
            raise
        body.exited.add_done_callback(lambda _: _mark_done(exited))
        value = await body.outcome
        if definition.continues:
            return value
        return serializer.encode(value, definition.output_type)

    outcome = loop.create_task(run())
    outcome.add_done_callback(_retrieve)
    return RunningBody(outcome, exited)


def fold_compaction(
    registry: TaskRegistry,
    serializers: SerializerRegistry,
    definition_id: str,
    payloads: Sequence[bytes],
) -> bytes:
    """A compaction run's payloads folded into one, encoded as the task's input."""
    definition = task_named(registry, definition_id)
    serializer = serializers.get(definition.serializer)
    return serializer.encode(fold_payloads(definition, serializer, payloads), definition.input_type)


def _mark_done(future: "asyncio.Future[None]") -> None:
    if not future.done():
        future.set_result(None)


def _retrieve(done: "asyncio.Future[Any]") -> None:
    # An outcome nobody awaits any more must not be logged as never retrieved.
    done.cancelled() or done.exception()
