"""Where a session's task bodies run: in this process, or in task processes.

A session hands an executor each claimed run as its serialized input and gets
back the encoded result, or the step a task that continues returns. The
executor owns the places bodies run in: a place is taken when a run is handed
over and given back once its body has really stopped, which for a body that
ignored a request to stop can be long after its run ended.
"""

import asyncio
import contextvars
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol

from kabudachi.body import RunningBody, fold_compaction, run_serialized
from kabudachi.concurrency_places import ConcurrencyPlaces
from kabudachi.errors import TaskTimeoutError
from kabudachi.handle import TaskHandle, current_body
from kabudachi.lifecycle import HookRegistry, RunContext, hooks_for_run, initialize_process
from kabudachi.options import SubmissionOptions
from kabudachi.registry import TaskRegistry
from kabudachi.serializers import SerializerRegistry


class TaskProcessLost(Exception):
    """The task process running a body died before the body finished."""


@dataclass(frozen=True)
class RunJob:
    """One claimed run to execute."""

    run_id: str
    definition_id: str
    source_version: int
    serialized_input: bytes
    chain: tuple[bytes, ...]
    """Payloads of the generations it superseded, folded in first, oldest first."""
    queue: str
    """The queue the task was sent to, which picks the run hooks."""
    attempt: int
    """1 for the task's first run, then one more for each retry."""
    cancel_grace: timedelta
    """How long its body has to stop once asked."""
    after: "asyncio.Future[None] | None" = None
    """Done once an earlier body of the same task has exited; this body
    starts only then, so one task never has two bodies running."""


@dataclass(frozen=True)
class CompactJob:
    """A compaction run: the payloads to fold, oldest first."""

    run_id: str
    definition_id: str
    payloads: tuple[bytes, ...]


class NestedCalls(Protocol):
    """What a body in a task process reaches when it calls a task, flow or group."""

    def submit_serialized(
        self, definition_id: str, payload: bytes, options: SubmissionOptions
    ) -> TaskHandle: ...

    def submit_composite(self, kind: str, composite: Any, previous: Any) -> TaskHandle: ...

    def cancel_task(self, task_id: str) -> bool:
        """Cancels a task such a body called whose handle is no longer kept,
        by its id, and says whether it was cancelled."""
        ...


class Executor(Protocol):
    @property
    def stops_bodies_at_once(self) -> bool:
        """Whether `stop(kill=True)` ends running bodies, as killing their processes does."""

    def free(self) -> int:
        """Places free now, across the executor; can be negative."""

    async def wait_for_free(self) -> None:
        """Returns once a place may be free, or when `wake` is called."""

    def wake(self) -> None:
        """Makes a pending `wait_for_free` return. Safe from any thread."""

    async def start(self) -> None:
        """Gets ready to run bodies. Raises `StartupError` if it cannot."""

    def accept_nested_calls(self, calls: NestedCalls) -> None:
        """Where calls made by bodies the executor cannot reach directly go."""

    def run(self, job: RunJob) -> RunningBody:
        """Hands `job` over; its place is taken now."""

    def compact(self, job: CompactJob) -> RunningBody:
        """Hands a compaction over; its outcome is the folded payload."""

    def cancel(self, run_id: str) -> None:
        """Asks the body to stop, as cancelling its outcome does."""

    def condemn_host_of(self, run_id: str) -> None:
        """The run passed its hard limit: whatever hosts its body is given up on."""

    def kill_host_of(self, run_id: str) -> None:
        """The run's abort deadline passed with its body still running: what
        hosts the body is killed now, with every body it hosts, and each of
        their runs is lost. Runs sharing a host share its lost contact, so
        they share the deadline too."""

    async def drain(self) -> None:
        """Waits until every body handed over so far has exited."""

    async def stop(self, *, kill: bool) -> None:
        """Releases what the executor holds; with `kill`, without waiting for bodies."""


class InProcessExecutor:
    """Runs bodies on the run's own event loop: an async body as a task, a
    synchronous one in a thread of a pool with one thread per place. Nothing
    here can be killed, so a body that ignores a request to stop is left to
    finish on its own, and keeps its place until it does."""

    def __init__(
        self,
        registry: TaskRegistry,
        serializers: SerializerRegistry,
        concurrency: int,
        hooks: HookRegistry,
    ) -> None:
        self._registry = registry
        self._serializers = serializers
        self._hooks = hooks
        self._places = ConcurrencyPlaces(concurrency)
        # As many threads as places, or synchronous bodies would queue behind
        # the event loop's small default pool.
        self._threads = ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix="kabudachi-task"
        )
        self._bodies: dict[str, RunningBody] = {}

    @property
    def stops_bodies_at_once(self) -> bool:
        return False

    def free(self) -> int:
        return self._places.free()

    async def wait_for_free(self) -> None:
        await self._places.wait_for_free()

    def wake(self) -> None:
        self._places.wake()

    async def start(self) -> None:
        """Runs the `process_init` hooks here, where the bodies run. Raises
        `StartupError` if one raises."""
        await initialize_process(self._hooks)

    def accept_nested_calls(self, calls: NestedCalls) -> None:
        """Bodies here call the session directly."""

    def run(self, job: RunJob) -> RunningBody:
        # A body that waits for a task it called gives its place back meanwhile.
        context = contextvars.copy_context()
        context.run(current_body.set, self._places.watch_body())
        hooks = hooks_for_run(
            self._hooks,
            RunContext(job.definition_id, job.run_id, job.attempt),
            job.queue,
            recycle=_no_process_to_replace,
            condemn=_no_process_to_replace,
        )
        body = context.run(
            run_serialized,
            self._registry,
            self._serializers,
            self._threads,
            job.definition_id,
            job.source_version,
            job.chain,
            job.serialized_input,
            hooks,
            job.after,
        )
        self._hold(job.run_id, body)
        return body

    def compact(self, job: CompactJob) -> RunningBody:
        loop = asyncio.get_running_loop()
        outcome = loop.create_task(self._fold(job))
        exited: asyncio.Future[None] = loop.create_future()
        outcome.add_done_callback(lambda _: exited.done() or exited.set_result(None))
        body = RunningBody(outcome, exited)
        self._hold(job.run_id, body)
        return body

    async def _fold(self, job: CompactJob) -> bytes:
        return fold_compaction(self._registry, self._serializers, job.definition_id, job.payloads)

    def _hold(self, run_id: str, body: RunningBody) -> None:
        self._places.occupy(body.exited)
        self._bodies[run_id] = body
        body.exited.add_done_callback(lambda _: self._bodies.pop(run_id, None))

    def cancel(self, run_id: str) -> None:
        body = self._bodies.get(run_id)
        if body is not None:
            body.outcome.cancel()

    def condemn_host_of(self, run_id: str) -> None:
        """Nothing here can be killed: the body is left to finish on its own."""

    def kill_host_of(self, run_id: str) -> None:
        """Nothing here can be killed. A networked worker, whose runs have
        abort deadlines, always runs bodies in task processes."""

    async def drain(self) -> None:
        await self._places.wait_until_running_finish()

    async def stop(self, *, kill: bool) -> None:
        self._threads.shutdown(wait=False, cancel_futures=True)


def _no_process_to_replace() -> None:
    """Bodies here run in the worker itself, which is never replaced: a
    failed hook is only logged, and a body that raised `SystemExit`, or a
    `KeyboardInterrupt` that cannot be a Ctrl-C (one raised off the main
    thread), only fails its run."""


async def run_within(
    body: RunningBody,
    timeout: timedelta | None,
    cancel_grace: timedelta,
    condemn: Callable[[], None],
) -> Any:
    """The body's outcome, or its own error. If it takes longer than
    `timeout` it is asked to stop, and `TaskTimeoutError` is raised as soon as
    it has stopped or `cancel_grace` has passed, whichever is first; in the
    second case `condemn` is called first. Afterwards `body.exited` says
    whether it is still running."""
    if timeout is None:
        return await body.outcome
    done, _ = await asyncio.wait({body.outcome}, timeout=timeout.total_seconds())
    if done:
        return body.outcome.result()
    body.outcome.cancel()
    await asyncio.wait({body.exited}, timeout=cancel_grace.total_seconds())
    if not body.exited.done():
        condemn()
    raise TaskTimeoutError(f"the task ran longer than its timeout of {timeout}")
