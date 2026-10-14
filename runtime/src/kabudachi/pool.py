"""Task processes: task bodies run in child processes this worker starts, so a
body that blocks or will not stop can be killed without taking the worker
with it.

Each child imports the modules that declare tasks, says which tasks it found,
and then runs what it is sent on its own event loop. The worker sends a child
each run's serialized input and gets the encoded result back; it certifies
and settles as for a body it runs itself. Every run goes to the least busy
child with a free place. A child that dies takes its runs with it, and
another is started in its place.
"""

import asyncio
import concurrent.futures
import dataclasses
import itertools
import logging
import multiprocessing
import pickle
import threading
import time
from dataclasses import dataclass
from typing import Any

from kabudachi import _child, ipc
from kabudachi.body import RunningBody
from kabudachi.config import Settings
from kabudachi.errors import RunStoppedError, StartupError, TaskDefinitionError, body_error
from kabudachi.execution import CompactJob, NestedCalls, RunJob, TaskProcessLost
from kabudachi.handle import TaskHandle
from kabudachi.lifecycle import HookRegistry
from kabudachi.options import SubmissionOptions
from kabudachi.registry import TaskRegistry
from kabudachi.schemas import process_schemas, runnable_versions

_logger = logging.getLogger("kabudachi")

# How long a child asked to stop (SIGTERM) has before it is killed (SIGKILL):
# the grace after which it exits of its own accord.
_TERMINATE_GRACE_SECONDS = _child.STOP_GRACE_SECONDS
# How long, as the interpreter exits, the children asked to stop have in all
# before the rest are killed.
_EXIT_GRACE_SECONDS = 2.0
# How long, once a child has died, the frames it wrote just before may still
# take to be read.
_LAST_FRAMES_SECONDS = 1.0
# A child that dies sooner than this after becoming ready counts as dying
# quickly; each quick death in a row doubles the wait before the next start.
_STABLE_SECONDS = 1.0
_FIRST_RESPAWN_DELAY_SECONDS = 0.1
_MAX_RESPAWN_DELAY_SECONDS = 10.0


def task_modules(
    registry: TaskRegistry, hooks: HookRegistry, imports: tuple[str, ...] | None
) -> tuple[str, ...]:
    """The modules a task process imports to find every task and lifecycle
    hook: `imports` if given, else every module that declared one here.

    Raises `TaskDefinitionError` naming every task or hook declared in the
    script being run, which a task process cannot import, whether or not
    `imports` is given.
    """
    unreachable = [
        definition.name
        for definition in registry.definitions()
        if definition.module in ("", "__main__")
    ] + [hook.described for hook in hooks.all() if hook.module in ("", "__main__")]
    if unreachable:
        raise TaskDefinitionError(
            "tasks and hooks declared in the script being run cannot run in task processes, "
            f"which cannot import it: {', '.join(unreachable)}; declare them in a module, "
            "or set processes=0 to run task bodies in this process"
        )
    if imports is not None:
        return imports
    modules = [definition.module for definition in registry.definitions()]
    modules += [hook.module for hook in hooks.all()]
    return tuple(dict.fromkeys(modules))


@dataclass(eq=False)
class _Slot:
    """A run or compaction handed to the pool, until its body has exited."""

    job: RunJob | CompactJob
    outcome: "asyncio.Future[Any]"
    exited: "asyncio.Future[None]"
    child: "_Child | None" = None
    waiting: bool = False
    """The body waits for tasks it called and has given its place back."""
    abandoned: bool = False
    """The run ended at its hard limit while the body went on."""


class _Child:
    """One task process, the pipe to it, and the runs it holds."""

    def __init__(
        self, number: int, process: Any, connection: Any, loop: asyncio.AbstractEventLoop
    ) -> None:
        self.number = number
        self.process = process
        # Runs sent to it, compactions not counted.
        self.runs = 0
        self.connection = connection
        self.slots: dict[str, _Slot] = {}
        # Tasks bodies here called, until their outcome is sent here.
        self.handles: dict[str, TaskHandle] = {}
        self.ready = False
        self.ready_at = 0.0
        # The module it said it was importing last, until it is ready.
        self.importing: str | None = None
        self.condemned = False
        self.draining = False
        self.terminating = False
        self.became_ready: asyncio.Future[ipc.Ready] = loop.create_future()
        self.frames_ended: asyncio.Future[None] = loop.create_future()
        self.buried: asyncio.Future[None] = loop.create_future()
        self._send_lock = threading.Lock()

    def busy(self) -> int:
        return sum(1 for slot in self.slots.values() if not slot.waiting)

    def takes_runs(self, concurrency: int) -> bool:
        """Whether a run can be sent here now: ready, neither draining nor
        being stopped, and with a free place."""
        if not self.ready or self.draining or self.terminating:
            return False
        return self.busy() < concurrency

    def send(self, frame: Any) -> None:
        """Sends `frame`, from any thread. A child that is gone is noticed by
        its exit, not here."""
        try:
            with self._send_lock:
                self.connection.send(frame)
        except (OSError, ValueError):
            pass

    def close(self) -> None:
        with self._send_lock:
            self.connection.close()


class ProcessPool:
    """`processes` task processes with `concurrency` places each."""

    def __init__(
        self,
        settings: Settings,
        modules: tuple[str, ...],
        registry: TaskRegistry,
        hooks: HookRegistry,
    ) -> None:
        self._settings = settings
        self._modules = modules
        self._registry = registry
        self._hooks = hooks
        self._context = multiprocessing.get_context("spawn")
        self._loop: asyncio.AbstractEventLoop | None = None
        # The children counted against `processes`, draining ones included;
        # condemned ones are not.
        self._children: list[_Child] = []
        self._condemned: set[_Child] = set()
        self._slots: dict[str, _Slot] = {}
        # Handed over, not yet sent to a child.
        self._pending: list[_Slot] = []
        # Handed over, waiting for an earlier body of their task to exit.
        self._gated = 0
        self._freed = asyncio.Event()
        self._nested: NestedCalls | None = None
        self._numbers = itertools.count()
        self._background: set[asyncio.Task[None]] = set()
        # Status reads bodies asked for, until the worker's session answers.
        self._inspecting: set[asyncio.Task[None]] = set()
        # Replacement starts under way, stopped with the pool.
        self._replacing: set[asyncio.Task[None]] = set()
        # Dead children are replaced only between a successful start and stop.
        self._serving = False
        # Set by `stop_at_exit` and never cleared: no child is started after it.
        self._exiting = False
        # Held while a child is started and added to `_children`, and by
        # `stop_at_exit` while it ends serving, so that it never looks at the
        # children between a child's start and its being listed.
        self._spawning = threading.Lock()
        self._quick_deaths = 0

    # places

    stops_bodies_at_once = True

    def free(self) -> int:
        room = sum(
            max(0, self._settings.concurrency - child.busy())
            for child in self._children
            if child.ready and not child.draining
        )
        return room - len(self._pending) - self._gated

    async def wait_for_free(self) -> None:
        if self.free() > 0:
            return
        # No await since free() was read, and every set() runs on this loop.
        self._freed.clear()
        await self._freed.wait()

    def wake(self) -> None:
        loop = self._loop
        if loop is None:
            self._freed.set()
            return
        try:
            loop.call_soon_threadsafe(self._freed.set)
        except RuntimeError:
            pass  # the loop is closed, so nothing waits

    # lifecycle

    async def start(self) -> None:
        """Starts every task process and waits until each is ready.

        Raises `StartupError` if one cannot import the task modules, does not
        have every task this process has (at the same version, with its
        serializer), or exits first. Whatever was started is stopped then.
        """
        self._loop = asyncio.get_running_loop()
        try:
            children = []
            for _ in range(self._settings.processes):
                with self._spawning:
                    if self._exiting:
                        break
                    children.append(self._spawn())
            for child in children:
                self._accept(child, await self._until_ready(child))
        except BaseException:
            await self.stop(kill=True)
            raise
        with self._spawning:
            if self._exiting:
                return
            self._serving = True
        # A child that died after it was ready, while a sibling was still
        # starting, was not replaced then.
        for _ in range(self._settings.processes - len(self._children)):
            self._replace(None)

    def accept_nested_calls(self, calls: NestedCalls) -> None:
        self._nested = calls

    async def drain(self) -> None:
        while self._slots:
            await asyncio.wait({slot.exited for slot in self._slots.values()})

    async def stop(self, *, kill: bool) -> None:
        """Without `kill`, every body is let finish and each child exits on its
        own; with it, or for a child still there after that, SIGTERM and then
        SIGKILL. Returns once every child has exited."""
        self._serving = False
        for replacing in self._replacing:
            # A child it already spawned is among the children below.
            replacing.cancel()
        for reading in self._inspecting:
            # The body that asked is told the run is stopping.
            reading.cancel()
        children = [*self._children, *self._condemned]
        if not kill:
            await self._let_children_finish(children)
        for child in children:
            self._terminate(child)
        if children:
            await asyncio.wait({child.buried for child in children})
        self._fail_unsent()

    def stop_at_exit(self) -> None:
        """Stops every child from the calling thread, without the event loop:
        SIGTERM, then SIGKILL for any still there after a short grace. For the
        interpreter's exit, where the loop may never run again and a child
        left running would be waited for forever. Safe to call again, or after
        `stop`."""
        # The loop's thread is still running: a child that dies now must not
        # be replaced. One being started holds the lock, so it is listed by the
        # time this returns from taking it, and none starts after.
        with self._spawning:
            self._exiting = True
            self._serving = False
        deadline = time.monotonic() + _EXIT_GRACE_SECONDS
        while True:
            processes = [
                child.process
                for child in (*self._children, *self._condemned)
                if child.process.is_alive()
            ]
            if not processes:
                return
            for process in processes:
                process.terminate()
            for process in processes:
                process.join(max(0.0, deadline - time.monotonic()))
                if process.is_alive():
                    process.kill()
                    process.join(_EXIT_GRACE_SECONDS)
            if time.monotonic() > deadline + 2 * _EXIT_GRACE_SECONDS:
                return

    async def _let_children_finish(self, children: list[_Child]) -> None:
        for child in children:
            child.draining = True
            child.send(ipc.Drain())
        await self.drain()
        if children:
            # Each child is still tearing down its loop and flushing output.
            await asyncio.wait(
                {child.buried for child in children}, timeout=_TERMINATE_GRACE_SECONDS
            )

    def _fail_unsent(self) -> None:
        # Every child is gone, so a run still waiting for one (or gated behind
        # an earlier body) can never run.
        for slot in list(self._slots.values()):
            if slot.child is not None or slot.exited.done():
                continue
            if slot in self._pending:
                self._pending.remove(slot)
            if not slot.outcome.done():
                slot.outcome.set_exception(
                    TaskProcessLost("the task processes were stopped before it could run")
                )
            self._finish(slot)

    # handing over

    def run(self, job: RunJob) -> RunningBody:
        slot = self._hand_over(job)
        gates = [gate for gate in (job.after, job.given) if gate is not None and not gate.done()]
        if gates:
            # Sent only once the earlier body has exited and what the run is
            # given has arrived, so the wait does not count against this
            # run's timeout, which starts then too.
            self._gated += 1
            task = self._started_loop().create_task(self._ungate(slot, gates))
            self._background.add(task)
            task.add_done_callback(self._background.discard)
        else:
            self._queue(slot)
        return RunningBody(slot.outcome, slot.exited)

    def compact(self, job: CompactJob) -> RunningBody:
        slot = self._hand_over(job)
        self._queue(slot)
        return RunningBody(slot.outcome, slot.exited)

    def cancel(self, run_id: str) -> None:
        slot = self._slots.get(run_id)
        if slot is not None:
            slot.outcome.cancel()

    def condemn_host_of(self, run_id: str) -> None:
        """The run passed its hard limit: its child takes no new runs, stops
        counting against `processes`, and is stopped once every other body
        on it has exited or been given up on too. Its neighbours are never
        killed for it."""
        slot = self._slots.get(run_id)
        if slot is None:
            return
        if slot.child is None:
            slot.outcome.cancel()  # never sent: nothing runs, nothing to stop
            return
        slot.abandoned = True
        self._condemn(slot.child)

    def kill_host_of(self, run_id: str) -> None:
        slot = self._slots.get(run_id)
        if slot is None or slot.child is None:
            # Ended already, or never sent: its cancel ended it.
            return
        # The death is buried as any other: its runs lost, and a replacement.
        slot.child.process.kill()

    def _hand_over(self, job: RunJob | CompactJob) -> _Slot:
        loop = self._started_loop()
        slot = _Slot(job, loop.create_future(), loop.create_future())
        slot.outcome.add_done_callback(lambda outcome: self._outcome_settled(slot, outcome))
        self._slots[job.run_id] = slot
        return slot

    async def _ungate(self, slot: _Slot, gates: "list[asyncio.Future[Any]]") -> None:
        await asyncio.wait(gates)
        self._gated -= 1
        job = slot.job
        reading = job.given if isinstance(job, RunJob) else None
        if slot.exited.done():
            pass
        elif reading is not None and (reading.cancelled() or reading.exception() is not None):
            # What the run was to be given could not be read: it fails with that.
            if not slot.outcome.done():
                slot.outcome.set_exception(
                    TaskProcessLost("reading what the run is given was cancelled")
                    if reading.cancelled()
                    else reading.exception()
                )
            self._finish(slot)
        else:
            self._queue(slot)
        self._wake()

    def _queue(self, slot: _Slot) -> None:
        self._pending.append(slot)
        self._dispatch()

    def _dispatch(self) -> None:
        while self._pending:
            child = self._least_busy()
            if child is None:
                return
            slot = self._pending.pop(0)
            slot.child = child
            child.slots[slot.job.run_id] = slot
            child.send(_frame_for(slot.job))
            if isinstance(slot.job, RunJob):
                child.runs += 1
                if self._recycles_after(child, slot.job):
                    self._retire(child)

    def _recycles_after(self, child: _Child, job: RunJob) -> bool:
        """Whether `child` takes no run after `job`: it has reached
        `max_runs_per_process`, or `job`'s task asks for a fresh process
        after each run."""
        limit = self._settings.max_runs_per_process
        if limit is not None and child.runs >= limit:
            return True
        definition = self._registry.get(job.definition_id)
        return definition is not None and definition.recycle_process

    def _least_busy(self) -> _Child | None:
        open_children = [
            child for child in self._children if child.takes_runs(self._settings.concurrency)
        ]
        return min(open_children, key=lambda child: (child.busy(), child.number), default=None)

    def _outcome_settled(self, slot: _Slot, outcome: "asyncio.Future[Any]") -> None:
        if not outcome.cancelled():
            outcome.exception()  # retrieved: whoever awaits it has it already
            return
        if slot.child is not None:
            slot.child.send(ipc.Cancel(slot.job.run_id))
            if isinstance(slot.job, RunJob):
                timer = self._started_loop().call_later(
                    slot.job.cancel_grace.total_seconds(), self._give_up_on, slot
                )
                slot.exited.add_done_callback(lambda _: timer.cancel())
            return
        # Never sent to a child: nothing runs, so it is over at once.
        if slot in self._pending:
            self._pending.remove(slot)
        self._finish(slot)

    def _give_up_on(self, slot: _Slot) -> None:
        """A body asked to stop that has not within its grace is treated as
        past its hard limit: its process is condemned."""
        if not slot.exited.done():
            self.condemn_host_of(slot.job.run_id)

    def _finish(self, slot: _Slot) -> None:
        """The slot's body has exited: its place is free."""
        self._slots.pop(slot.job.run_id, None)
        if slot.child is not None:
            slot.child.slots.pop(slot.job.run_id, None)
        _set_done(slot.exited)
        self._wake()

    def _wake(self) -> None:
        self._freed.set()

    def _started_loop(self) -> asyncio.AbstractEventLoop:
        """The loop `start` ran on, which every child and slot belongs to."""
        if self._loop is None:
            raise RuntimeError("the process pool has not been started")
        return self._loop

    # children

    def _spawn(self) -> _Child:
        parent_end, child_end = self._context.Pipe(duplex=True)
        number = next(self._numbers)
        process = self._context.Process(
            target=_child.main,
            # The router runs where tasks are submitted from, in this process;
            # a body's own calls come back here to be routed.
            args=(child_end, dataclasses.replace(self._settings, router=None), self._modules),
            name=f"kabudachi-task-{number}",
        )
        try:
            process.start()
        except BaseException:
            parent_end.close()
            child_end.close()
            raise
        # Only the child holds its end now, so the pipe closes when it exits.
        child_end.close()
        loop = self._started_loop()
        child = _Child(number, process, parent_end, loop)
        child.became_ready.add_done_callback(_retrieve)
        self._children.append(child)
        threading.Thread(
            target=self._read, args=(child,), name=f"kabudachi-task-{number}-pipe", daemon=True
        ).start()
        loop.add_reader(process.sentinel, self._on_exit, child)
        return child

    def _read(self, child: _Child) -> None:
        """Runs on the child's reading thread: hands each frame to the loop."""
        loop = self._started_loop()

        def deliver(frame: Any) -> None:
            try:
                loop.call_soon_threadsafe(self._received, child, frame)
            except RuntimeError:
                pass  # the loop has closed; nobody waits for frames any more

        try:
            ipc.read_frames(child.connection, deliver)
        finally:
            child.close()
            try:
                loop.call_soon_threadsafe(_set_done, child.frames_ended)
            except RuntimeError:
                pass

    async def _until_ready(self, child: _Child) -> ipc.Ready:
        """The child's `Ready`. Raises `StartupError` if it exits first, or if
        it is not ready within `process_start_timeout`, when it is killed."""
        limit = self._settings.process_start_timeout.total_seconds()
        try:
            # Shielded: the deadline gives up waiting, it does not resolve the future.
            return await asyncio.wait_for(asyncio.shield(child.became_ready), limit)
        except TimeoutError:
            child.process.kill()
            where = f" while importing {child.importing}" if child.importing else ""
            raise StartupError(
                f"task process {child.process.name} was not ready within {limit:g} s{where}, "
                "so it was killed"
            ) from None

    def _accept(self, child: _Child, ready: ipc.Ready) -> None:
        """Checks what a child found against this process's tasks; it takes
        runs only if they agree. Raises `StartupError` otherwise."""
        if ready.error is not None:
            raise StartupError(f"a task process {ready.error}")
        problems = _disagreements(self._registry, self._hooks, ready)
        if problems:
            raise StartupError(
                "a task process does not have the tasks and hooks this process has ("
                + "; ".join(problems)
                + "); declare every task and hook, and register every serializer, in a module the "
                "task processes import, or list those modules in `imports`"
            )
        child.ready = True
        child.ready_at = time.monotonic()
        self._wake()
        self._dispatch()

    def _received(self, child: _Child, frame: Any) -> None:
        match frame:
            case ipc.Importing():
                child.importing = frame.module
            case ipc.Ready():
                if not child.became_ready.done():
                    child.became_ready.set_result(frame)
            case ipc.Result():
                slot = child.slots.get(frame.run_id)
                if slot is not None and not slot.outcome.done():
                    try:
                        value = frame.payload if frame.step is None else pickle.loads(frame.step)
                    except Exception as error:
                        slot.outcome.set_exception(error)
                    else:
                        slot.outcome.set_result(value)
            case ipc.Compacted():
                slot = child.slots.get(frame.run_id)
                if slot is not None and not slot.outcome.done():
                    slot.outcome.set_result(frame.payload)
            case ipc.Failed():
                slot = child.slots.get(frame.run_id)
                if slot is not None and not slot.outcome.done():
                    slot.outcome.set_exception(_raisable(frame))
            case ipc.Exited():
                slot = child.slots.get(frame.run_id)
                if slot is not None:
                    self._finish(slot)
                    self._dispatch()
                    self._end_if_condemned(child)
            case ipc.Waiting() | ipc.Resumed():
                slot = child.slots.get(frame.run_id)
                if slot is not None:
                    slot.waiting = isinstance(frame, ipc.Waiting)
                    self._wake()
                    self._dispatch()
            case ipc.Recycle():
                self._retire(child)
            case ipc.Condemn():
                self._condemn(child)
            case ipc.Submit():
                self._submit_for(child, frame)
            case ipc.CancelTask():
                self._cancel_for(child, frame)
            case ipc.Inspect():
                self._inspect_for(child, frame)

    def _cancel_for(self, child: _Child, frame: ipc.CancelTask) -> None:
        """Cancels what a body in `child` asked to cancel and answers whether
        it was. A task the child no longer holds a handle for (its outcome
        already settled) is cancelled by id."""
        handle = child.handles.get(frame.task_id)
        try:
            if handle is not None:
                cancelled = handle.cancel()
            else:
                cancelled = self._nested is not None and self._nested.cancel_task(frame.task_id)
        except Exception as error:
            child.send(ipc.Reply(frame.request, error=ipc.portable(error)))
        else:
            child.send(ipc.Reply(frame.request, cancelled))

    def _submit_for(self, child: _Child, frame: ipc.Submit) -> None:
        """Submits what a body in `child` called, answers at once with the
        new task's id or the error, tells it once a networked leader stored
        the task, and sends the outcome once it settles."""
        try:
            handle = self._nested_handle(frame)
        except Exception as error:
            child.send(ipc.Reply(frame.request, error=ipc.portable(error)))
            return
        child.handles[handle.task_id] = handle
        child.send(ipc.Reply(frame.request, (handle.task_id, handle.shard_id, handle._replicable)))

        def stored(outcome: "concurrent.futures.Future[None]") -> None:
            if handle.task_id in child.handles:
                child.send(ipc.Stored(handle.task_id, outcome.exception()))

        def settled(outcome: Any) -> None:
            # A dead child's handles are dropped with it: the tasks it called
            # run on, but nobody there waits for them any more.
            if child.handles.pop(handle.task_id, None) is not None:
                child.send(ipc.outcome_of(handle.task_id, outcome))

        if handle._replicable:
            handle._stored.add_done_callback(stored)
        handle._outcome.add_done_callback(settled)

    def _inspect_for(self, child: _Child, frame: ipc.Inspect) -> None:
        """Answers a body's status read once the worker's session has it, or
        as a stopped run once the pool is stopping, so a body let finish
        never waits for an answer that will never come."""
        nested = self._nested

        async def answer() -> None:
            try:
                if nested is None:
                    raise RuntimeError("the process pool takes no nested calls before a session")
                status = await nested.task_status(frame.task_id)
            except Exception as error:
                child.send(ipc.Reply(frame.request, error=ipc.portable(error)))
            else:
                # A status that would not arrive whole fails the read, so
                # the body never waits for an answer that cannot come.
                child.send(
                    ipc.answer_of(frame.request, status, f"the status of task {frame.task_id}")
                )

        def stopped(reading: "asyncio.Task[None]") -> None:
            # Told here, so a read cancelled even before its first step is
            # answered, and answered once.
            if reading.cancelled():
                child.send(ipc.Reply(frame.request, error=RunStoppedError("the run is stopping")))

        reading = asyncio.get_running_loop().create_task(answer())
        self._inspecting.add(reading)
        reading.add_done_callback(self._inspecting.discard)
        reading.add_done_callback(stopped)
        if not self._serving:
            reading.cancel()

    def _nested_handle(self, frame: ipc.Submit) -> TaskHandle:
        """The handle of the task, flow or group a body in a task process
        called. Raises what the submission raised, or why it could not be
        made."""
        nested = self._nested
        if nested is None:
            raise RuntimeError("the process pool takes no nested calls before a session")
        if frame.definition_id is not None and frame.payload is not None:
            options = frame.options or SubmissionOptions()
            return nested.submit_serialized(frame.definition_id, frame.payload, options)
        if frame.composite and frame.step is not None and frame.previous is not None:
            return nested.submit_composite(pickle.loads(frame.step), pickle.loads(frame.previous))
        raise ValueError("a submission names neither a task nor a flow or group")

    def _retire(self, child: _Child) -> None:
        """`child` takes no new runs, finishes the ones it has and exits on
        its own; nothing it runs is cut short. It still counts against
        `processes` until it has drained and exited, and its replacement
        starts then, so long runs never raise the number of processes."""
        if child.condemned or child.draining:
            return
        child.draining = True
        child.send(ipc.Drain())

    def _condemn(self, child: _Child) -> None:
        """`child` takes no new runs, and is stopped (SIGTERM, then SIGKILL)
        once every body on it has exited or been given up on; its other runs
        are never killed for it. A replacement starts at once."""
        if not child.condemned:
            child.condemned = True
            self._set_aside(child)
        self._end_if_condemned(child)

    def _set_aside(self, child: _Child) -> None:
        """Moves `child` out of the processes counted against `processes`,
        into the condemned, and starts its replacement at once if it was
        counted. The replacement starts without the wait after a quick death:
        the child did not die."""
        self._condemned.add(child)
        if child in self._children:
            self._children.remove(child)
            if self._serving:
                self._replace(None)

    def _end_if_condemned(self, child: _Child) -> None:
        if child.condemned and all(slot.abandoned for slot in child.slots.values()):
            self._terminate(child)

    def _terminate(self, child: _Child) -> None:
        """SIGTERM now (the child cancels its bodies and exits), SIGKILL if it
        is still there after a grace."""
        if child.terminating or child.buried.done():
            return
        child.terminating = True
        child.process.terminate()
        self._started_loop().call_later(_TERMINATE_GRACE_SECONDS, self._kill, child)

    def _kill(self, child: _Child) -> None:
        if not child.buried.done():
            child.process.kill()

    def _on_exit(self, child: _Child) -> None:
        loop = self._started_loop()
        loop.remove_reader(child.process.sentinel)
        task = loop.create_task(self._bury(child))
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _bury(self, child: _Child) -> None:
        # Frames the child wrote just before it died are still in the pipe:
        # a result among them counts and must not be taken for a loss.
        await asyncio.wait({child.frames_ended}, timeout=_LAST_FRAMES_SECONDS)
        child.process.join()
        code = child.process.exitcode
        if not child.became_ready.done():
            child.became_ready.set_exception(
                StartupError(f"a task process exited with code {code} before it was ready")
            )
        for slot in list(child.slots.values()):
            if not slot.outcome.done():
                slot.outcome.set_exception(
                    TaskProcessLost(f"the task process running it exited with code {code}")
                )
            self._finish(slot)
        child.handles.clear()
        replace = child in self._children and self._serving
        if child in self._children:
            self._children.remove(child)
        self._condemned.discard(child)
        _set_done(child.buried)
        if replace:
            self._replace(child, drained=child.draining and code == 0)
        self._dispatch()

    def _replace(self, dead: _Child | None, *, drained: bool = False) -> None:
        """Starts a child in place of `dead`, or of a child set aside (`None`)
        at once. A child that `drained`, exiting cleanly as asked, is replaced
        at once too; one that died while draining did not. After a child that
        died quickly the start waits, longer for each quick death in a row."""
        delay = 0.0
        if dead is not None and not drained:
            lived = time.monotonic() - dead.ready_at if dead.ready else 0.0
            self._quick_deaths = self._quick_deaths + 1 if lived < _STABLE_SECONDS else 0
            delay = self._respawn_delay()
        self._replace_later(delay)

    def _respawn_delay(self) -> float:
        if not self._quick_deaths:
            return 0.0
        return min(
            _FIRST_RESPAWN_DELAY_SECONDS * 2 ** (self._quick_deaths - 1),
            _MAX_RESPAWN_DELAY_SECONDS,
        )

    def _replace_later(self, delay: float) -> None:
        task = self._started_loop().create_task(self._replace_after(delay))
        self._replacing.add(task)
        task.add_done_callback(self._replacing.discard)

    async def _replace_after(self, delay: float) -> None:
        await asyncio.sleep(delay)
        try:
            with self._spawning:
                if not self._serving:
                    return
                child = self._spawn()
        except Exception as error:
            # Out of processes or file descriptors, say: a start that failed
            # at once, tried again later like a child that died at once.
            _logger.error("a replacement task process could not be started: %s", error)
            self._quick_deaths += 1
            if self._serving:
                self._replace_later(self._respawn_delay())
            return
        try:
            self._accept(child, await self._until_ready(child))
        except StartupError as error:
            # Its exit is buried like any other, which tries again, later.
            _logger.error("a replacement task process could not start: %s", error)
            self._terminate(child)


def _raisable(frame: ipc.Failed) -> BaseException:
    """The error a failed body raised, with the child's traceback as a note.
    One that is not an `Exception` (a task's merge or serializer raising
    `CancelledError` though nobody asked the run to stop) would end whoever
    awaits the outcome instead of failing the run, so it arrives as a
    `TaskBodyError` of its kind."""
    error = frame.error
    if not isinstance(error, Exception):
        error = body_error(error)
    error.add_note(f"raised in a task process:\n{frame.traceback}")
    return error


def _disagreements(registry: TaskRegistry, hooks: HookRegistry, ready: ipc.Ready) -> list[str]:
    """How what a task process found differs from this process's tasks and
    hooks: a task missing there, with its input at another version or without its
    serializer, and a hook missing there."""
    problems = []
    ours = runnable_versions(registry, process_schemas())
    for definition in registry.definitions():
        version = ready.definitions.get(definition.name)
        if version is None:
            problems.append(f"{definition.name} is missing")
        elif version != ours[definition.name]:
            problems.append(
                f"{definition.name}'s input is at version {version}, not {ours[definition.name]}"
            )
        elif definition.serializer not in ready.serializers:
            problems.append(
                f"{definition.name}'s serializer {definition.serializer!r} is not registered"
            )
    for described in (hook.described for hook in hooks.all()):
        if described not in ready.hooks:
            problems.append(f"{described} is missing")
    return problems


def _frame_for(job: RunJob | CompactJob) -> ipc.Run | ipc.Compact:
    if isinstance(job, CompactJob):
        return ipc.Compact(job.run_id, job.definition_id, job.payloads)
    return ipc.Run(
        job.run_id,
        job.definition_id,
        job.source_version,
        job.serialized_input,
        job.chain,
        job.queue,
        job.attempt,
        job.task_id,
        job.shard,
        job.worker_id,
        job.given.result() if job.given is not None else None,
    )


def _set_done(future: "asyncio.Future[None]") -> None:
    if not future.done():
        future.set_result(None)


def _retrieve(done: "asyncio.Future[Any]") -> None:
    # Nobody may await a child's readiness once the start gave up on it.
    if not done.cancelled():
        done.exception()
