"""Tasks at module scope, for tests whose bodies run in task processes: the
processes find them by importing this module. Bodies leave marks in the
directory `KABUDACHI_TEST_MARKERS` names, which a test shares with its task
processes."""

import asyncio
import os
import signal
import threading
import time
from datetime import timedelta
from pathlib import Path

import kabudachi
from kabudachi.errors import TaskCancelledError, UnknownTaskError
from kabudachi.session import current_session
from proto_messages import Greeting

MARKERS = "KABUDACHI_TEST_MARKERS"


def marker(name: str) -> Path:
    return Path(os.environ[MARKERS]) / name


def first_time(name: str) -> bool:
    """True for the first caller of `name` in any process: it makes the mark."""
    try:
        marker(name).open("x").close()
    except FileExistsError:
        return False
    return True


@kabudachi.task(name="pool.where_async")
async def where_async(request: Greeting) -> Greeting:
    """Sleeps `request.text` seconds; gives this process's id and the time it ended."""
    await asyncio.sleep(float(request.text or 0))
    return Greeting(times=os.getpid(), text=repr(time.time()))


@kabudachi.task(name="pool.where_sync")
def where_sync(request: Greeting) -> Greeting:
    time.sleep(float(request.text or 0))
    return Greeting(times=os.getpid(), text=repr(time.time()))


@kabudachi.task(name="pool.dies_once")
def dies_once(request: Greeting) -> Greeting:
    """Kills its own process the first time it runs for `request.text`."""
    if first_time(f"dies-once-{request.text}"):
        os.kill(os.getpid(), signal.SIGKILL)
    return Greeting(times=os.getpid())


@kabudachi.ephemeral_task(name="pool.ephemeral_dies")
def ephemeral_dies(request: Greeting) -> Greeting:
    os.kill(os.getpid(), signal.SIGKILL)
    return request


@kabudachi.task(name="pool.sized")
def sized(request: Greeting) -> Greeting:
    return Greeting(text="x" * request.times)


@kabudachi.task(name="pool.refuses")
def refuses(request: Greeting) -> Greeting:
    raise ValueError(f"refused {request.text}")


def fold_left(older: Greeting, newer: Greeting) -> Greeting:
    marker("folded-in").write_text(str(os.getpid()))
    return Greeting(text=f"({older.text}>{newer.text})")


@kabudachi.coalescing_task(name="pool.refresh", merge=fold_left)
async def refresh(request: Greeting) -> Greeting:
    if request.text == "holder":
        marker("holding").touch()
        for _ in range(500):  # holds the key until a compaction has folded
            if marker("folded-in").exists():
                return Greeting(text="held until folded")
            await asyncio.sleep(0.01)
    return request


def fold_once_released(older: Greeting, newer: Greeting) -> Greeting:
    """Leaves its process id in `merging`, then blocks its whole task process
    until `release merge` exists (at most 30 s), and leaves `merged` once it has."""
    marker("merging").write_text(str(os.getpid()))
    end = time.monotonic() + 30
    while time.monotonic() < end and not marker("release merge").exists():
        time.sleep(0.01)
    marker("merged").touch()
    return Greeting(text=f"({older.text}>{newer.text})")


@kabudachi.coalescing_task(
    name="pool.compacted_slowly", merge=fold_once_released, cancel_grace=timedelta(seconds=3)
)
async def compacted_slowly(request: Greeting) -> Greeting:
    """The `holder` generation holds the key until a fold has begun, so the
    payloads queued behind it are compacted. The cancel grace is longer than
    the abort deadline a test gives the compaction."""
    if request.text == "holder":
        marker("holding").touch()
        for _ in range(3000):
            if marker("merging").exists():
                break
            await asyncio.sleep(0.01)
    return request


@kabudachi.task(name="pool.leaf")
async def leaf(request: Greeting) -> Greeting:
    return Greeting(times=request.times + 1)


async def note_slowly(result: Greeting) -> None:
    await asyncio.sleep(0.3)
    marker("called-back").write_text(str(result.times))


def declared_late(request: Greeting) -> Greeting:
    return request


@kabudachi.task(name="pool.calls_others")
async def calls_others(request: Greeting) -> Greeting:
    # Still running when this body has returned: the process waits for it.
    leaf(Greeting(times=100)).callback(note_slowly)
    one = await leaf(request)
    try:
        await refuses(Greeting(text="nested"))
    except ValueError as error:
        failed = type(error).__name__
    # Only this process has it, so the worker refuses the call.
    late = kabudachi.task(name="pool.declared_late")(declared_late)
    try:
        late(request)
    except UnknownTaskError as error:
        refused = type(error).__name__
    stages = await kabudachi.flow(leaf, leaf)(request)
    mapped = await leaf.map([Greeting(times=10), Greeting(times=20)])
    later = leaf.options(delay=timedelta(seconds=30))(request)
    cancelled = later.cancel()
    try:
        await later
        ended = "ran"
    except TaskCancelledError:
        ended = "cancelled"
    total = one.times + stages[-1].times + sum(result.times for result in mapped)
    return Greeting(times=total, text=f"{cancelled} {ended} {failed} {refused}")


LIMIT = {"timeout": timedelta(milliseconds=300), "cancel_grace": timedelta(milliseconds=200)}


@kabudachi.task(name="pool.stubborn_then_quick", retries=1, **LIMIT)
def stubborn_then_quick(request: Greeting) -> Greeting:
    if first_time(f"stubborn-{request.text}"):
        time.sleep(30)  # a synchronous body cannot be cancelled, only killed
    return Greeting(times=os.getpid(), text=repr(time.time()))


@kabudachi.task(name="pool.stubborn", **LIMIT)
def stubborn(request: Greeting) -> Greeting:
    time.sleep(30)
    return request


@kabudachi.task(name="pool.cancellable")
async def cancellable(request: Greeting) -> Greeting:
    marker(f"started-{request.text}").write_text(str(os.getpid()))
    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:
        marker(f"cancelled-{request.text}").touch()
        raise
    return request


@kabudachi.task(name="pool.reports_and_sleeps")
async def reports_and_sleeps(request: Greeting) -> Greeting:
    """Sleeps `request.text` seconds, saying on stdout, which a task process
    shares with its worker, when it starts (with its process id), ends, or
    is cancelled. It ignores the cancel and sleeps on: only its process's
    end stops it."""
    print(f"task started {os.getpid()}", flush=True)
    while True:
        try:
            await asyncio.sleep(float(request.text))
            break
        except asyncio.CancelledError:
            print("task cancelled", flush=True)
    print("task done", flush=True)
    return request


@kabudachi.task(name="pool.ignores_cancel", cancel_grace=timedelta(milliseconds=200))
async def ignores_cancel(request: Greeting) -> Greeting:
    marker("ignoring").touch()
    while True:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            continue


@kabudachi.ephemeral_task(name="pool.outlives_its_deadline", cancel_grace=timedelta(seconds=3))
async def outlives_its_deadline(request: Greeting) -> Greeting:
    """Leaves its process id in `outliving`, and runs on for 30 s whatever
    it is asked: only its process's end stops it. Its cancel grace is longer
    than the abort deadline a test gives it, so a cancel that it ignores
    costs its process only after that deadline.

    First it calls two tasks that will not start for 30 s, on a worker whose
    results never come back, and cancels one: `nested` says whether the
    cancel took, and how many handles this process still keeps for them."""
    later = leaf.options(delay=timedelta(seconds=30))
    later(request)
    cancelled = later(request).cancel()
    kept = len(current_session()._process._handles)
    marker("nested").write_text(f"{cancelled} {kept}")
    marker("outliving").write_text(str(os.getpid()))
    end = time.monotonic() + 30
    while time.monotonic() < end:
        try:
            await asyncio.sleep(end - time.monotonic())
        except asyncio.CancelledError:
            marker("asked to stop").touch()
    return request


@kabudachi.task(name="pool.recycles", recycle_process=True)
async def recycles(request: Greeting) -> Greeting:
    """Still running when its process stops taking runs, so it shows the
    process finished it instead of cutting it short."""
    await asyncio.sleep(0.3)
    return Greeting(times=os.getpid())


HOOKED = "hooks"
# What this process's process_init hook prepared.
PROCESS: dict[str, int] = {}
_per_thread = threading.local()


@kabudachi.process_init
def open_resources() -> None:
    """Fails, leaving a mark, while the test's directory asks it to;
    otherwise notes that this process prepared itself."""
    if MARKERS not in os.environ:
        return  # a program that shares no directory with its task processes
    if marker("process-init-fails").exists():
        with marker("process-init-failures").open("a") as failures:
            failures.write(f"{os.getpid()}\n")
        raise RuntimeError("resources unavailable")
    PROCESS["opened_in"] = os.getpid()


@kabudachi.before_run(queues=[HOOKED])
async def log_start(context: kabudachi.RunContext) -> None:
    _log(f"before {context.task_name} {context.attempt}")
    if context.task_name in ("pool.refused", "pool.refused_once") and context.attempt == 1:
        raise LookupError(f"{context.task_name} is not ready")


@kabudachi.before_run(queues=[HOOKED])
def check_out(context: kabudachi.RunContext) -> None:
    _per_thread.context = context


@kabudachi.after_run(queues=[HOOKED])
def check_in(context: kabudachi.RunContext, outcome: object) -> None:
    _log(f"after {context.task_name} {context.attempt} {type(outcome).__name__}")
    if isinstance(outcome, Greeting) and outcome.text == "spoil":
        raise ValueError("could not return the resources")


def _log(line: str) -> None:
    with marker("hooks.log").open("a") as log:
        log.write(line + "\n")


def _prepared() -> str:
    """The run the before_run hook left on this thread, and whether this
    process ran its process_init hook."""
    context = _per_thread.context
    return f"{context.task_name} {context.attempt} {PROCESS.get('opened_in') == os.getpid()}"


@kabudachi.task(name="pool.hooked", queue=HOOKED)
def hooked(request: Greeting) -> Greeting:
    return Greeting(times=os.getpid(), text=request.text or _prepared())


@kabudachi.task(name="pool.hooked_async", queue=HOOKED)
async def hooked_async(request: Greeting) -> Greeting:
    return Greeting(times=os.getpid(), text=request.text or _prepared())


@kabudachi.task(name="pool.refused_once", queue=HOOKED, retries=1)
def refused_once(request: Greeting) -> Greeting:
    return Greeting(text=str(_per_thread.context.attempt))


@kabudachi.task(name="pool.refused", queue=HOOKED)
def refused(request: Greeting) -> Greeting:
    return request


@kabudachi.task(name="pool.steady")
async def steady(request: Greeting) -> Greeting:
    """Says it started, then runs for a second: long enough to be running
    beside bodies that fail."""
    marker(f"steady-{request.text}").touch()
    await asyncio.sleep(1)
    return Greeting(times=os.getpid())


LEAVING = {"SystemExit": SystemExit, "KeyboardInterrupt": KeyboardInterrupt}


@kabudachi.task(name="pool.leaves")
def leaves(request: Greeting) -> Greeting:
    """Raises the exception `request.text` names, in its own thread."""
    raise LEAVING[request.text]("leaving")


@kabudachi.task(name="pool.leaves_async")
async def leaves_async(request: Greeting) -> Greeting:
    """Raises the exception `request.text` names, on its process's event loop."""
    raise LEAVING[request.text]("leaving")
