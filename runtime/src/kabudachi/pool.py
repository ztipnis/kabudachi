"""Task processes: task bodies run in child processes this worker starts, so a
body that blocks or will not stop can be killed without taking the worker
with it.

Each child imports the modules that declare tasks, says which tasks it found,
and then runs what it is sent on its own event loop. The worker sends a child
each run's serialized input and gets the encoded result back; it certifies
and settles as for a body it runs itself. Every run goes to the least busy
child with a free place. A child that dies takes its runs with it.
"""

import asyncio
import itertools
import multiprocessing
import pickle
import threading
import time
from dataclasses import dataclass
from typing import Any

from kabudachi import _child, ipc
from kabudachi.body import RunningBody
from kabudachi.config import Settings
from kabudachi.errors import StartupError, TaskBodyError, TaskDefinitionError
from kabudachi.execution import CompactJob, NestedCalls, RunJob, TaskProcessLost
from kabudachi.handle import TaskHandle
from kabudachi.options import SubmissionOptions
from kabudachi.registry import TaskRegistry

# How long a child asked to stop (SIGTERM) has before it is killed (SIGKILL).
_TERMINATE_GRACE_SECONDS = 1.0
# How long, once a child has died, the frames it wrote just before may still
# take to be read.
_LAST_FRAMES_SECONDS = 1.0


def task_modules(registry: TaskRegistry, imports: tuple[str, ...] | None) -> tuple[str, ...]:
    """The modules a task process imports to find every task: `imports` if
    given, else every module that declared one here.

    Raises `TaskDefinitionError` naming every task declared in the script
    being run, which a task process cannot import, whether or not `imports`
    is given.
    """
    unreachable = [
        definition.name
        for definition in registry.definitions()
        if definition.module in ("", "__main__")
    ]
    if unreachable:
        raise TaskDefinitionError(
            "tasks declared in the script being run cannot run in task processes, "
            f"which cannot import it: {', '.join(unreachable)}; declare them in a module, "
            "or set processes=0 to run task bodies in this process"
        )
    if imports is not None:
        return imports
    return tuple(dict.fromkeys(definition.module for definition in registry.definitions()))


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

    def __init__(self, settings: Settings, modules: tuple[str, ...], registry: TaskRegistry) -> None:
        self._settings = settings
        self._modules = modules
        self._registry = registry
        self._context = multiprocessing.get_context("spawn")
        self._loop: asyncio.AbstractEventLoop | None = None
        # The children counted against `processes`; condemned ones are not.
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

    # places

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
            children = [self._spawn() for _ in range(self._settings.processes)]
            for child in children:
                self._accept(child, await self._until_ready(child))
        except BaseException:
            await self.stop(kill=True)
            raise

    def accept_nested_calls(self, calls: NestedCalls) -> None:
        self._nested = calls

    async def drain(self) -> None:
        while self._slots:
            await asyncio.wait({slot.exited for slot in self._slots.values()})

    async def stop(self, *, kill: bool) -> None:
        """Without `kill`, every body is let finish and each child exits on its
        own; with it, or for a child still there after that, SIGTERM and then
        SIGKILL. Returns once every child has exited."""
        children = [*self._children, *self._condemned]
        if not kill:
            for child in children:
                child.draining = True
                child.send(ipc.Drain())
            await self.drain()
        for child in children:
            self._terminate(child)
        if children:
            await asyncio.wait({child.buried for child in children})

    # handing over

    def run(self, job: RunJob) -> RunningBody:
        slot = self._hand_over(job)
        if job.after is not None and not job.after.done():
            # Sent only once the earlier body has exited, so the wait does not
            # count against this run's timeout, which starts then too.
            self._gated += 1
            job.after.add_done_callback(lambda _: self._ungate(slot))
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
        child = slot.child
        if not child.condemned:
            child.condemned = True
            if child in self._children:
                self._children.remove(child)
            self._condemned.add(child)
        self._end_if_condemned(child)

    def _hand_over(self, job: RunJob | CompactJob) -> _Slot:
        loop = self._started_loop()
        slot = _Slot(job, loop.create_future(), loop.create_future())
        slot.outcome.add_done_callback(lambda outcome: self._outcome_settled(slot, outcome))
        self._slots[job.run_id] = slot
        return slot

    def _ungate(self, slot: _Slot) -> None:
        self._gated -= 1
        if not slot.exited.done():
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

    def _least_busy(self) -> _Child | None:
        open_children = [
            child
            for child in self._children
            if child.ready and not child.draining and child.busy() < self._settings.concurrency
        ]
        return min(open_children, key=lambda child: (child.busy(), child.number), default=None)

    def _outcome_settled(self, slot: _Slot, outcome: "asyncio.Future[Any]") -> None:
        if not outcome.cancelled():
            outcome.exception()  # retrieved: whoever awaits it has it already
            return
        if slot.child is not None:
            slot.child.send(ipc.Cancel(slot.job.run_id))
            return
        # Never sent to a child: nothing runs, so it is over at once.
        if slot in self._pending:
            self._pending.remove(slot)
        self._finish(slot)

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
            args=(child_end, self._settings, self._modules),
            name=f"kabudachi-task-{number}",
        )
        process.start()
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
        problems = []
        for definition in self._registry.definitions():
            version = ready.definitions.get(definition.name)
            if version is None:
                problems.append(f"{definition.name} is missing")
            elif version != definition.version:
                problems.append(f"{definition.name} is at version {version}, not {definition.version}")
            elif definition.serializer not in ready.serializers:
                problems.append(
                    f"{definition.name}'s serializer {definition.serializer!r} is not registered"
                )
        if problems:
            raise StartupError(
                "a task process does not have the tasks this process has ("
                + "; ".join(problems)
                + "); declare every task, and register its serializer, in a module the task "
                "processes import, or list those modules in `imports`"
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
            case ipc.Submit():
                self._submit_for(child, frame)
            case ipc.CancelTask():
                handle = child.handles.get(frame.task_id)
                try:
                    cancelled = handle is not None and handle.cancel()
                except Exception as error:
                    child.send(ipc.Reply(frame.request, error=ipc.portable(error)))
                else:
                    child.send(ipc.Reply(frame.request, cancelled))

    def _submit_for(self, child: _Child, frame: ipc.Submit) -> None:
        """Submits what a body in `child` called, answers at once with the
        new task's id or the error, and sends the outcome once it settles."""
        nested = self._nested
        try:
            if nested is None:
                raise RuntimeError("the process pool takes no nested calls before a session")
            if frame.definition_id is not None and frame.payload is not None:
                options = frame.options or SubmissionOptions()
                handle = nested.submit_serialized(frame.definition_id, frame.payload, options)
            elif frame.composite is not None and frame.step is not None and frame.previous is not None:
                handle = nested.submit_composite(
                    frame.composite, pickle.loads(frame.step), pickle.loads(frame.previous)
                )
            else:
                raise ValueError("a submission names neither a task nor a flow or group")
        except Exception as error:
            child.send(ipc.Reply(frame.request, error=ipc.portable(error)))
            return
        child.handles[handle.task_id] = handle
        child.send(ipc.Reply(frame.request, (handle.task_id, handle.shard_id)))

        def settled(outcome: Any) -> None:
            child.handles.pop(handle.task_id, None)
            child.send(ipc.outcome_of(handle.task_id, outcome))

        handle._outcome.add_done_callback(settled)

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
        if child in self._children:
            self._children.remove(child)
        self._condemned.discard(child)
        _set_done(child.buried)
        self._dispatch()


def _raisable(frame: ipc.Failed) -> BaseException:
    """The error a failed body raised, with the child's traceback as a note.
    One that is not an `Exception` (a body raising `CancelledError` though
    nobody asked it to stop) would end whoever awaits the outcome instead of
    failing the run, so it arrives as a `TaskBodyError` of its kind."""
    error = frame.error
    if not isinstance(error, Exception):
        kind = type(error).__name__
        error = TaskBodyError(f"{kind}: {error}" if str(error) else kind, kind)
    error.add_note(f"raised in a task process:\n{frame.traceback}")
    return error


def _frame_for(job: RunJob | CompactJob) -> ipc.Run | ipc.Compact:
    if isinstance(job, CompactJob):
        return ipc.Compact(job.run_id, job.definition_id, job.payloads)
    return ipc.Run(job.run_id, job.definition_id, job.source_version, job.serialized_input, job.chain)


def _set_done(future: "asyncio.Future[None]") -> None:
    if not future.done():
        future.set_result(None)


def _retrieve(done: "asyncio.Future[Any]") -> None:
    # Nobody may await a child's readiness once the start gave up on it.
    if not done.cancelled():
        done.exception()
