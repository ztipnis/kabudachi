"""Flows, groups and maps: the handles of the ones this process submitted.

The shard's leader runs a composite: it starts each stage or member, settles
them as they end, and tells the process that submitted it when it is over.
This only submits composites, cancels them, and settles each handle when its
composite finishes.
"""

import asyncio
import contextvars
import functools
import logging
import threading
import weakref
from collections.abc import Awaitable, Iterable, Iterator, Sequence
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from kabudachi import _native
from kabudachi._native import CancelOutcome
from kabudachi.errors import (
    PayloadTooLargeError,
    RunStoppedError,
    RuntimeNotStartedError,
    TaskCancelledError,
    TaskInterruptedError,
)
from kabudachi.handle import TaskHandle
from kabudachi.hosted_work import LoopHostedWork
from kabudachi.native_protocol import Event, Runtime
from kabudachi.status import CompositeStatus

if TYPE_CHECKING:
    from kabudachi.session import Session

_logger = logging.getLogger("kabudachi")

_COUNTED = ("pending", "running", "succeeded", "failed", "cancelled")

# What the native runtime says to every call made after it shut down.
_CLOSED_MESSAGE = "has shut down"

MEMBER_BATCH_ITEMS: int = _native.MEMBER_BATCH_ITEMS
"""Most members one batch of a streamed group or map holds."""

MEMBER_BATCH_BYTES: int = _native.MEMBER_BATCH_BYTES
"""Most bytes of member input one batch holds; a member larger than this has
its bound values stored as blobs before it is sent."""

INLINE_VALUE_BYTES: int = _native.INLINE_VALUE_BYTES
"""The longest item a reduce is sent inline: it sits in an item page, so a
longer one is stored as blobs the reduce owns."""

# What a member costs in a batch beyond the values bound in it: its
# definition, version, options and framing.
_MEMBER_OVERHEAD_BYTES = 64


class CompositeHandle(TaskHandle):
    """A submitted flow, group or map, named by its composite id. Await it for
    the results of its stages or members, in order. If a stage fails, or a
    member does under `fail_fast`, awaiting raises that error.

    `cancel()` cancels the stage that is running, or every member that has not
    finished, and starts nothing more.
    """

    kind = "group"
    """What `status()` calls this composite."""

    def __init__(
        self,
        composite_id: str,
        runtime: Runtime,
        callback_runner: Any,
    ) -> None:
        super().__init__(
            composite_id,
            self._cancel_composite,
            callback_runner,
            shard_id=runtime.shard_id(),
            replicable=not runtime.certifies_locally,
        )
        self._runtime = runtime
        self.cancel_requested = False
        """Whether a cancel of this composite was asked, so a stream still
        sending its members stops."""
        self.streaming = False
        """Whether a stream of members has begun for this composite."""

    async def status(self) -> CompositeStatus:
        """How the composite stands: its state, its stages or members by
        state, and, for a flow, the index of the stage it is at or ended on."""
        found = await self._runtime.composite_status(self.task_id)
        if found is None:
            # The leader no longer holds it: what this handle was settled with is all there is.
            return CompositeStatus(self.task_id, self.kind, self._settled_state(), MappingProxyType({}))
        state, stage, *counts = found
        return CompositeStatus(
            self.task_id,
            self.kind,
            state,
            MappingProxyType({name: n for name, n in zip(_COUNTED, counts) if n}),
            stage if self.kind == "flow" else None,
        )

    def _settled_state(self) -> str:
        if not self.done():
            return "running"
        error = self._outcome.exception()
        if error is None:
            return "succeeded"
        return "cancelled" if isinstance(error, TaskCancelledError) else "failed"

    def _cancel_composite(self, composite_id: str) -> bool:
        # The flag first: a stream waiting to send its next batch sees it.
        self.cancel_requested = True
        outcome = self._runtime.cancel_composite(composite_id)
        # `None`: sent to a leader elsewhere, which decides.
        cancelled = outcome is None or outcome == CancelOutcome.CANCELLED
        self.cancel_requested = cancelled
        return cancelled


class FlowHandle(CompositeHandle):
    """A submitted flow: its results are those of its stages, in stage order."""

    kind = "flow"


class GroupHandle(CompositeHandle):
    """A submitted group or map: its results are those of its members, in
    member order."""


class ReduceHandle(GroupHandle):
    """A submitted reduce: it resolves to the folded value. A reduce streams
    its items as a group streams its members, so it is a `GroupHandle`
    (`Composites._stream` reads its `cancel_requested`); its composite's
    results resolve to one value, which the reducer's serializer decodes."""


async def composite_is_running(runtime: Runtime, composite_id: str) -> bool:
    """Whether the leader still has the composite running, or has lost track of it."""
    found = await runtime.composite_status(composite_id)
    return found is None or found[0] == "running"


def _largest_value(tree: Any) -> int:
    """The size of the largest value bound anywhere in the described `tree`."""
    if tree[0] == "task":
        value = tree[4]
        return len(value) if isinstance(value, bytes) else 0
    return max((_largest_value(stage) for stage in tree[2]), default=0)


def refuse_oversized_values(runtime: Runtime, tree: Any) -> None:
    """Raises `PayloadTooLargeError` if a value bound anywhere in the
    described `tree` alone is past the hard limit this process can hold."""
    limit = runtime.payload_limit
    if limit is not None and (size := _largest_value(tree)) > limit:
        raise PayloadTooLargeError(
            f"a value bound in this step is {size} bytes, past the {limit} this process can hold",
            size,
            limit,
        )


class Composites:
    """The flows, groups and maps this session submitted, by composite id.
    The leader runs them; this only submits them, cancels them, and settles
    each handle when its composite finishes."""

    def __init__(self, session: "Session", hosted: LoopHostedWork) -> None:
        self._session = session
        self._hosted = hosted
        self.batch_items = MEMBER_BATCH_ITEMS
        """At most this many members per batch; a test may lower it."""
        self._lock = threading.Lock()
        self._open: dict[str, CompositeHandle] = {}
        # Every handle still alive, finished or not, for status reads.
        self._known: weakref.WeakValueDictionary[str, CompositeHandle] = (
            weakref.WeakValueDictionary()
        )

    def submit(self, step: Any, previous: Any) -> Any:
        """Submits `step`, its first stages bound to `previous` when they take
        an input, and returns its handle, a `step.handle_type`. A flow goes
        whole. A group, or a map called with its list, sends its head now and
        its members in batches from the run's loop, waiting out SlowDown
        before each batch. Raises as a submit does: `BackpressureError` past
        the hard memory limit (for a stream, through its handle once it is
        under way), `PayloadTooLargeError` for a value bound in a flow that is
        past the hard limit alone, and `RunStoppedError` once the run is
        stopping."""
        from kabudachi.flow import Group, MappedList, ReduceStep

        if isinstance(step, ReduceStep) and isinstance(previous, list):
            # A reduce a body in a task process called: its items stream on.
            return self.stream_reduce(step, previous)
        if not self._hosted.has_loop():
            raise RuntimeNotStartedError(
                "flows and groups can only be started while kabudachi.run() is serving"
            )
        if self._session.stopping:
            raise RunStoppedError("the run is stopping, so no more flows are accepted")
        if isinstance(step, Group):
            return self._stream(step.describe_streamed(), step.members_described(self._session, previous))
        if isinstance(step, MappedList):
            return self._stream(*self._mapped(step))
        return self._submit_whole(step, previous)

    def stream_reduce(self, step: Any, items: Sequence[Any]) -> ReduceHandle:
        """Submits a reduce's head and sends `items` in batches, each encoded
        as the reducer's item, lazily, waiting out SlowDown before each batch.
        Raises `RunStoppedError` once the run is stopping."""
        if not self._hosted.has_loop():
            raise RuntimeNotStartedError("reduces can only be started while kabudachi.run() is serving")
        if self._session.stopping:
            raise RunStoppedError("the run is stopping, so no more reduces are accepted")
        definition = step.definition
        serializer = self._session.serializers.get(definition.serializer)
        encoded = (serializer.encode(item, definition.input_type) for item in items)
        return self._stream(
            step.describe_streamed(self._session), encoded, handle=ReduceHandle, inline_bytes=INLINE_VALUE_BYTES
        )

    def _submit_whole(self, step: Any, previous: Any) -> Any:
        tree = step.describe(self._session, previous)
        runtime = self._session.runtime
        refuse_oversized_values(runtime, tree)
        with self._lock:
            # Registered under the lock the finish is looked up under, so a
            # composite that ends at once is never missed.
            composite_id = runtime.submit_composite(tree)
            handle = step.handle_type(composite_id, runtime, self._session.callback_runner)
            self._open[composite_id] = handle
            self._known[composite_id] = handle
        return handle

    def _mapped(self, mapped: Any) -> tuple[tuple, Iterator[bytes]]:
        """A mapped list's head, holding its one template, and its items
        encoded as the template takes them, lazily."""
        definition = mapped.definition
        serializer = self._session.serializers.get(definition.serializer)
        settings = self._session.submission_settings(definition)
        template = ("task", definition.name, definition.version, settings, None, [])
        items = (serializer.encode(item, definition.input_type) for item in mapped.items)
        return ("map", mapped.on_error, [template], "streamed"), items

    def _stream(
        self,
        head: tuple,
        members: Iterable[Any],
        handle: type["GroupHandle"] = GroupHandle,
        inline_bytes: int = MEMBER_BATCH_BYTES,
    ) -> "GroupHandle":
        """Submits `head` and hosts what sends `members`, returning a
        `handle` for it. The bindings store the bound values of a member
        whose inline bytes pass `inline_bytes` as blobs. A run that cannot
        host it is left with nothing: the head it took is cancelled."""
        with self._lock:
            composite_id = self._session.runtime.submit_composite(head)
            streamed = handle(composite_id, self._session.runtime, self._session.callback_runner)
            self._open[composite_id] = streamed
            self._known[composite_id] = streamed
        try:
            self._host(self._send_members(composite_id, members, streamed, inline_bytes), streamed)
        except BaseException:
            with self._lock:
                self._open.pop(composite_id, None)
            self._session.runtime.cancel_composite(composite_id)
            raise
        return streamed

    def _host(self, coroutine: Any, handle: "GroupHandle") -> None:
        """Runs one stream on the run's loop. Raises, leaving the handle to
        be dropped by the caller, if the run cannot take it."""
        # Its own context, so awaiting its batches is not counted as the
        # calling task body waiting.
        if not self._hosted.spawn(
            coroutine,
            context=contextvars.Context(),
            when_done=functools.partial(self._ended, handle),
        ):
            raise RuntimeNotStartedError("the run's event loop has closed")

    def _ended(self, handle: "GroupHandle") -> None:
        """The stream's coroutine ended. One cancelled before it began, as a
        closing loop does, never sent anything: the composite is cancelled and
        the handle fails here, so nobody waits on it for ever."""
        if not handle.streaming and not handle.done():
            self._abandon(
                handle.task_id,
                handle,
                TaskInterruptedError(f"{handle.task_id} was interrupted before it was sent"),
            )

    async def _send_members(
        self, composite_id: str, members: Iterable[Any], handle: "GroupHandle", inline_bytes: int
    ) -> None:
        """Sends `members` in batches of at most `batch_items` members and
        `MEMBER_BATCH_BYTES` of inline input, the last marked so, until the
        leader says the composite is over. A stop, a cancel or a refusal ends
        the stream: what was sent is cancelled and the handle fails."""
        handle.streaming = True
        start = 0
        try:
            batches = _batches(members, self.batch_items, MEMBER_BATCH_BYTES)
            batch = next(batches, [])
            while True:
                following = next(batches, None)
                last = following is None
                if not await self._send(composite_id, start, batch, last, handle, inline_bytes) or last:
                    return
                start, batch = start + len(batch), following
        except BaseException as error:
            self._abandon(composite_id, handle, error)
            if not isinstance(error, Exception):
                raise

    async def _send(
        self,
        composite_id: str,
        start: int,
        batch: list[Any],
        last: bool,
        handle: "GroupHandle",
        inline_bytes: int,
    ) -> bool:
        """One batch, once there is room; whether the leader still takes more."""
        await self._wait_for_room(handle)
        answer = await self._session.runtime.submit_members(
            composite_id, start, batch, last, inline_bytes=inline_bytes
        )
        return answer == "accepted"

    async def _wait_for_room(self, handle: "GroupHandle") -> None:
        """Waits while the leader says memory use is past its soft limit
        (SlowDown). Ends early, by failing, if the run is stopping or the
        composite is cancelled, so waiting never outlives either."""
        # Yield first, so a SLOW_DOWN raised by the last batch, and a cancel
        # or stop that arrived meanwhile, are seen before the next batch.
        await asyncio.sleep(0)
        while True:
            if self._session.stopping:
                raise RunStoppedError("the run stopped while this was being sent")
            if handle.cancel_requested:
                raise TaskCancelledError(f"{handle.task_id} was cancelled")
            if self._session.has_room():
                return
            await self._session.wait_for_room()

    def _abandon(self, composite_id: str, handle: "GroupHandle", error: BaseException) -> None:
        """The stream ended early: the leader cancels what it took, and the
        handle fails with why, unless the cancel's notice settled it first."""
        with self._lock:
            self._open.pop(composite_id, None)
        try:
            self._session.runtime.cancel_composite(composite_id)
        except RuntimeError as refusal:
            if _CLOSED_MESSAGE in str(refusal):
                _logger.debug("%s needs no cancel: the runtime has shut down", composite_id)
            else:
                # This worker is not the leader and the composite is not
                # queued: the members it took may be left running.
                _logger.warning(
                    "the members of %s that were sent could not be cancelled: %s", composite_id, refusal
                )
        if not handle.done():
            handle._fail(
                error
                if isinstance(error, Exception)
                else TaskInterruptedError(f"{handle.task_id} was interrupted while it was being sent")
            )

    def accepted(self, composite_id: str) -> bool:
        """A networked leader stored the composite on a quorum of its shard:
        its handle is `REPLICATED`. Whether `composite_id` is one submitted
        here."""
        # Under the lock the registration is made under, so a composite
        # accepted at once is never missed.
        with self._lock:
            handle = self._known.get(composite_id)
        if handle is None:
            return False
        handle._mark_stored()
        return True

    def refused(self, composite_id: str, error: BaseException) -> bool:
        """The leader refused the composite for good: it was never stored, and
        its handle fails with `error`. Whether `composite_id` is one
        submitted here."""
        with self._lock:
            handle = self._open.pop(composite_id, None)
            known = composite_id in self._known
        if handle is not None:
            handle._fail(error)
        return known

    def finished(self, event: Event) -> None:
        """A composite is over (`COMPOSITE_FINISHED`): settles its handle, or,
        for a task's continuation, the task's own."""
        if event.parent_task_id is not None:
            self._session.continuation_finished(event)
            return
        with self._lock:
            handle = self._open.pop(event.composite_id, None)
        if handle is not None:
            self._session.settle_from_composite(handle, event)

    def status_of(self, composite_id: str) -> Awaitable[CompositeStatus] | None:
        """The status read of a flow, group or map this session submitted;
        `None` for any other id."""
        handle = self._known.get(composite_id)
        return None if handle is None else handle.status()

    def pending(self) -> bool:
        """Whether any composite submitted here has not finished."""
        with self._lock:
            return bool(self._open)

    async def stopped(self) -> None:
        """The run stopped and nothing runs here any more: a composite still
        running can never go on, since nothing will claim what is left of it.
        Its handle fails with `RunStoppedError`; one the leader has already
        finished is left to its finish."""
        with self._lock:
            open_handles = list(self._open.items())
        for composite_id, handle in open_handles:
            if not await composite_is_running(self._session.runtime, composite_id):
                continue
            with self._lock:
                if self._open.pop(composite_id, None) is None:
                    continue
            handle._fail(RunStoppedError("the run stopped before this could finish"))


def _batches(members: Iterable[Any], max_items: int, max_bytes: int) -> Iterator[list[Any]]:
    """`members` in consecutive batches of at most `max_items` members and
    `max_bytes` of inline input. A member larger than `max_bytes` goes in a
    batch of its own: the bindings store its bound values as blobs before they
    send it."""
    batch: list[Any] = []
    size = 0
    for member in members:
        length = _inline_bytes(member)
        if batch and (len(batch) == max_items or size + length > max_bytes):
            yield batch
            batch, size = [], 0
        batch.append(member)
        size += length
    if batch:
        yield batch


def _inline_bytes(member: Any) -> int:
    """What a member weighs in a batch: a map item's own bytes, or, for a
    group's member, the values and fields bound in its tree and a fixed cost
    for each task."""
    if isinstance(member, bytes):
        return len(member) + _MEMBER_OVERHEAD_BYTES
    if member[0] == "task":
        _, _, _, _, value, fields = member
        weight = _MEMBER_OVERHEAD_BYTES + (len(value) if isinstance(value, bytes) else 0)
        for _, _, field_value in fields:
            if isinstance(field_value, str):
                weight += len(field_value.encode())
            else:
                weight += len(field_value) if isinstance(field_value, bytes) else 8
        return weight
    return sum(_inline_bytes(stage) for stage in member[2])
