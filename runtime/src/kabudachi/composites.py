"""Flows, groups and maps: the handles of the ones this process submitted.

The shard's leader runs a composite: it starts each stage or member, settles
them as they end, and tells the process that submitted it when it is over.
This only submits composites, cancels them, and settles each handle when its
composite finishes.
"""

import threading
import weakref
from collections.abc import Awaitable
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from kabudachi._native import CancelOutcome
from kabudachi.errors import (
    PayloadTooLargeError,
    RunStoppedError,
    RuntimeNotStartedError,
    TaskCancelledError,
)
from kabudachi.handle import TaskHandle
from kabudachi.hosted_work import LoopHostedWork
from kabudachi.native_protocol import Event, Runtime
from kabudachi.status import CompositeStatus

if TYPE_CHECKING:
    from kabudachi.session import Session

_COUNTED = ("pending", "succeeded", "failed", "cancelled")


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
        outcome = self._runtime.cancel_composite(composite_id)
        # `None`: sent to a leader elsewhere, which decides.
        return outcome is None or outcome == CancelOutcome.CANCELLED


class FlowHandle(CompositeHandle):
    """A submitted flow: its results are those of its stages, in stage order."""

    kind = "flow"


class GroupHandle(CompositeHandle):
    """A submitted group or map: its results are those of its members, in
    member order."""


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
        self._lock = threading.Lock()
        self._open: dict[str, CompositeHandle] = {}
        # Every handle still alive, finished or not, for status reads.
        self._known: weakref.WeakValueDictionary[str, CompositeHandle] = (
            weakref.WeakValueDictionary()
        )

    def submit(self, step: Any, previous: Any) -> Any:
        """Submits `step`, its first stages bound to `previous` when they take
        an input, and returns its handle, a `step.handle_type`. Raises as a
        submit does: `BackpressureError` past the hard limit,
        `PayloadTooLargeError` for a value bound in it that is past the hard
        limit alone, and `RunStoppedError` once the run is stopping."""
        if not self._hosted.has_loop():
            raise RuntimeNotStartedError(
                "flows and groups can only be started while kabudachi.run() is serving"
            )
        if self._session.stopping:
            raise RunStoppedError("the run is stopping, so no more flows are accepted")
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
