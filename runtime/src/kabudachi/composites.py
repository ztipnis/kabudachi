"""Flows and groups: what running one is, and what its handle is.

A flow runs its stages one after another, a group runs its members side by
side, and both are orchestrated here, on the session's event loop, rather than
by the leader: only the tasks they submit are the leader's business (README
§3.4). Cancelling either cascades to what it started, which is why the handles
live here too, next to the orchestration that is the only thing allowed to
know their insides.
"""

import asyncio
import contextlib
import contextvars
import functools
import itertools
from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING, Any

from kabudachi.errors import (
    RunStoppedError,
    RuntimeNotStartedError,
    TaskCancelledError,
    TaskInterruptedError,
    interrupted,
)
# TaskCancelledError, TaskInterruptedError, interrupted
from kabudachi.handle import TaskHandle, run_callback_inline

from kabudachi.hosted_work import LoopHostedWork

if TYPE_CHECKING:
    from kabudachi.session import Session


class FlowHandle(TaskHandle):
    """A submitted flow. Await it for the results of its stages, in stage
    order. If a stage fails, awaiting raises that stage's error and no later
    stage starts.

    `cancel()` cancels the stage that is running and starts no later one.
    """

    def __init__(
        self,
        flow_id: str,
        callback_runner: Callable[[Callable[[Any], Any], Any], None] = run_callback_inline,
    ) -> None:
        super().__init__(flow_id, self._cancel_stage, callback_runner)
        # The stage now running and whether it is the last, published together
        # so a cancel from another thread never pairs one with the other's.
        self._current: tuple[TaskHandle, bool] | None = None
        self._cancel_requested = False

    @property
    def cancel_requested(self) -> bool:
        """Whether a cancel of this flow has been accepted, so no further
        stage may start."""
        return self._cancel_requested

    def stage_started(self, stage: TaskHandle, is_last: bool) -> None:
        """The stage now running, and whether it is the last one, so a cancel
        knows what to cancel and whether anything would follow it."""
        self._current = (stage, is_last)

    def _cancel_stage(self, _flow_id: str) -> bool:
        # The flag first: a stage that is being submitted right now sees it.
        self._cancel_requested = True
        current = self._current
        if current is None:
            # Nothing has started, and nothing will.
            return True
        stage, is_last = current
        stage_succeeded = stage.done() and stage._outcome.exception() is None
        if stage.cancel() or (stage_succeeded and not is_last):
            return True
        # Nothing was cancelled, so the flow runs on.
        self._cancel_requested = False
        return False


class GroupHandle(TaskHandle):
    """A submitted group. Await it for the results of its members, in member
    order. `cancel()` cancels every member that has not finished."""

    def __init__(
        self,
        group_id: str,
        callback_runner: Callable[[Callable[[Any], Any], Any], None] = run_callback_inline,
    ) -> None:
        super().__init__(group_id, self._cancel_members, callback_runner)
        self._members: list[TaskHandle] = []
        self._cancel_requested = False

    @property
    def cancel_requested(self) -> bool:
        """Whether a cancel of this group has been accepted, so no further
        member may start."""
        return self._cancel_requested

    @property
    def members(self) -> list[TaskHandle]:
        """The members started so far, as they were at this moment: a member
        can be added while the list is being walked."""
        return list(self._members)

    def member_started(self, member: TaskHandle) -> None:
        """One more member is running, so a cancel reaches it too."""
        self._members.append(member)

    def _cancel_members(self, _group_id: str) -> bool:
        # The flag first: a member that is being submitted right now sees it.
        self._cancel_requested = True
        if not self._members:
            # Nothing has started, and nothing will.
            return True
        cancelled = [member.cancel() for member in list(self._members)]
        if any(cancelled):
            return True
        # Nothing was cancelled, so the group runs on.
        self._cancel_requested = False
        return False


@contextlib.asynccontextmanager
async def _settling(handle: TaskHandle) -> AsyncIterator[None]:
    """Settles `handle` with whatever ends the orchestration inside it: its own
    error, or, for anything worse than an exception, a `TaskInterruptedError`
    that is then re-raised, so the loop still sees what really happened. The
    only way a flow or a group fails."""
    try:
        yield
    except Exception as error:
        handle._fail(error)
    except BaseException as error:
        handle._fail(interrupted(handle.task_id, error))
        raise


class Composites:
    """The flows and groups one session is running.

    A flow's stages and a group's members are ordinary tasks, so this owns
    only the sequencing, the bulk submission and what a cancel does to what
    was started. It hosts its own orchestrations on the run's loop. For
    everything else it reaches its session: submitting a stage (through the
    step), room to submit, whether the run is stopping, and the callback
    runner.
    """

    def __init__(self, session: "Session", hosted: LoopHostedWork) -> None:
        self._session = session
        self._hosted = hosted
        self._numbers = itertools.count(1)

    def submit_flow(self, flow: Any, previous: Any) -> FlowHandle:
        """Starts `flow`, whose stages run one after another, and returns its
        handle."""
        handle = FlowHandle(self._identifier("flow"), self._session.callback_runner)
        self._host(self._run_flow(flow, previous, handle), handle)
        return handle

    def submit_group(self, group: Any, previous: Any) -> GroupHandle:
        """Starts every member of `group` on `previous`, and returns the
        group's handle."""
        handle = GroupHandle(self._identifier("group"), self._session.callback_runner)
        self._host(self._run_group(group, previous, handle), handle)
        return handle

    def _identifier(self, kind: str) -> str:
        return f"{kind}-{next(self._numbers)}"

    def _host(self, coroutine: Any, handle: TaskHandle) -> None:
        """Runs one orchestration on the run's loop. Raises, leaving the
        handle to be dropped by the caller, if the run cannot take it."""
        if not self._hosted.has_loop():
            coroutine.close()
            raise RuntimeNotStartedError(
                "flows and groups can only be started while kabudachi.run() is serving"
            )
        if self._session.stopping:
            coroutine.close()
            raise RunStoppedError("the run is stopping, so no more flows are accepted")
        # Its own context, so awaiting its steps is not counted as the calling
        # task body waiting.
        if not self._hosted.spawn(
            coroutine,
            context=contextvars.Context(),
            when_done=functools.partial(self._ended, handle),
        ):
            raise RuntimeNotStartedError("the run's event loop has closed")

    @staticmethod
    def _ended(handle: TaskHandle) -> None:
        """The orchestration ended without settling its handle (it was
        cancelled before it started, as a closing loop does), so the handle
        fails here and nobody waits on it for ever."""
        if not handle.done():
            handle._fail(TaskInterruptedError(f"{handle.task_id} was interrupted before it ran"))

    async def _run_flow(self, flow: Any, previous: Any, handle: FlowHandle) -> None:
        results: list[Any] = []
        async with _settling(handle):
            for index, stage in enumerate(flow.stages):
                if handle.cancel_requested:
                    raise TaskCancelledError(f"{handle.task_id} was cancelled")
                stage_handle = stage.start(self._session, previous)
                handle.stage_started(stage_handle, index == len(flow.stages) - 1)
                if handle.cancel_requested:
                    # Cancelled while this stage was being submitted.
                    stage_handle.cancel()
                previous = await stage_handle
                results.append(previous)
            handle._resolve(results)

    async def _run_group(self, group: Any, previous: Any, handle: GroupHandle) -> None:
        async with _settling(handle):
            if handle.cancel_requested:
                raise TaskCancelledError(f"{handle.task_id} was cancelled")
            try:
                for member in group.members:
                    # Yield first, so the events the scheduler raised while the
                    # last members were submitted (SlowDown) are seen, and a
                    # cancel or stop that arrived is noticed, before the next.
                    await asyncio.sleep(0)
                    await self._wait_for_room(handle)
                    if handle.cancel_requested:
                        raise TaskCancelledError(f"{handle.task_id} was cancelled")
                    if self._session.stopping:
                        raise RunStoppedError("the run stopped while this group submitted")
                    handle.member_started(member.start(self._session, previous))
            except BaseException:
                for started in handle.members:
                    started.cancel()
                raise
            results = await _gather(handle.members, group.on_error)
            if handle.cancel_requested:
                raise TaskCancelledError(f"{handle.task_id} was cancelled")
            handle._resolve(results)

    async def _wait_for_room(self, handle: GroupHandle) -> None:
        """Waits while the scheduler says memory use is past its soft limit
        (`SlowDown`). Ends early, by failing, if the run is stopping or the
        group is cancelled, so waiting never outlives either."""
        while not self._session.has_room():
            if self._session.stopping:
                raise RunStoppedError("the run stopped while this group waited to submit")
            if handle.cancel_requested:
                raise TaskCancelledError(f"{handle.task_id} was cancelled")
            await self._session.wait_for_room()


async def _gather(members: list[TaskHandle], on_error: str) -> list[Any]:
    """The members' results in order. `fail_fast` raises the first failure
    and leaves the rest to finish, `collect_all` returns failures as
    entries."""

    async def outcome(member: TaskHandle) -> Any:
        return await member

    if not members:
        return []
    waiting = [asyncio.ensure_future(outcome(member)) for member in members]
    if on_error == "collect_all":
        return list(await asyncio.gather(*waiting, return_exceptions=True))
    try:
        done, pending = await asyncio.wait(waiting, return_when=asyncio.FIRST_EXCEPTION)
    except BaseException:
        for task in waiting:
            task.cancel()
        raise
    for task in pending:
        # Left running, so what they end with is not "never retrieved".
        task.add_done_callback(lambda ended: ended.cancelled() or ended.exception())
    # Every failure is looked at, so none is left "never retrieved".
    failures = [
        task.exception()
        for task in waiting
        if task in done and not task.cancelled() and task.exception() is not None
    ]
    if failures:
        raise failures[0]
    return [task.result() for task in waiting]
