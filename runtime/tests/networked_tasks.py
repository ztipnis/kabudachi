"""Tasks for the networked worker tests. A body that holds leaves a marker
file named for its text and its process id in `KABUDACHI_TEST_MARKERS`, a
directory the test owns, so the test can find the task process running it;
only the first run of a text holds."""

import asyncio
import os
import random
import time
from pathlib import Path

import kabudachi
from kabudachi.errors import ResultUnavailableError
from proto_messages import Greeting


def _markers() -> Path:
    return Path(os.environ["KABUDACHI_TEST_MARKERS"])


@kabudachi.task(name="cluster.spin")
def spin(request: Greeting) -> Greeting:
    """Keeps one CPU busy for `times` milliseconds."""
    end = time.monotonic() + request.times / 1000
    while time.monotonic() < end:
        pass
    return Greeting(text="spun")


@kabudachi.task(name="cluster.hold")
async def hold(request: Greeting) -> Greeting:
    """Holds for `times` milliseconds the first time `text` runs, or, for a
    negative `times`, until the test creates a `<text>.go` marker; a later
    run of the same text returns at once. Its start marker holds the process
    id of the worker running it. A hold asked to stop leaves a
    `<text>.stopped` marker, and one that returns a `<text>.ended` marker."""
    first = not any(_markers().glob(f"{request.text}.*"))
    started = _markers() / f"{request.text}.{os.getpid()}"
    # Written whole under a name nothing matches, then renamed: the test
    # never reads a start marker without its worker.
    written = _markers() / f"_{request.text}.{os.getpid()}"
    written.write_text(str(os.getppid()))
    written.rename(started)
    if first:
        try:
            await _held(request)
        except asyncio.CancelledError:
            (_markers() / f"{request.text}.stopped").touch()
            raise
    (_markers() / f"{request.text}.ended").touch()
    return Greeting(text=request.text)


async def _held(request: Greeting) -> None:
    if request.times >= 0:
        await asyncio.sleep(request.times / 1000)
        return
    go = _markers() / f"{request.text}.go"
    while not go.exists():
        await asyncio.sleep(0.01)


@kabudachi.task(name="cluster.nests")
async def nests(request: Greeting) -> Greeting:
    """Calls `hold` and awaits its result, and says what that did."""
    try:
        await hold(Greeting(text=request.text))
    except ResultUnavailableError as error:
        return Greeting(text=type(error).__name__)
    return Greeting(text="delivered")


@kabudachi.task(name="cluster.awaits")
async def awaits(request: Greeting) -> Greeting:
    """Calls `hold`, held until its leader has stored it, then lets it go and
    awaits its result; what that raises fails the run, as does a call never
    heard stored. Says how durably the call is kept."""
    called = hold(Greeting(text=request.text, times=-1))
    # Before the call can settle: only the worker's word that its leader
    # stored the task can answer this.
    await asyncio.wait_for(called.durable(kabudachi.Durability.REPLICATED), 20)
    (_markers() / f"{request.text}.go").touch()
    await called
    return Greeting(text=called.durability.name)


@kabudachi.ephemeral_task(name="cluster.fleeting")
async def fleeting(request: Greeting) -> Greeting:
    """As `hold`, for a task that is never replayed once its worker is lost."""
    first = not any(_markers().glob(f"{request.text}.*"))
    (_markers() / f"{request.text}.{os.getpid()}").touch()
    if first:
        await asyncio.sleep(request.times / 1000)
    return Greeting(text=request.text)


@kabudachi.task(name="cluster.echo")
def echo(request: Greeting) -> Greeting:
    """Returns its input as it came."""
    return request


@kabudachi.task(name="cluster.fails")
async def fails(request: Greeting) -> Greeting:
    """Fails with `text` as its error's message."""
    raise ValueError(request.text)


@kabudachi.task(name="cluster.inflate")
def inflate(request: Greeting) -> Greeting:
    """Returns its input's text twice over."""
    return Greeting(text=request.text * 2)


@kabudachi.task(name="cluster.weigh")
async def weigh(request: Greeting) -> Greeting:
    """Holds until the test creates a `<key>.go` marker, `key` being the
    first eight characters of its text, which is otherwise ballast to make the
    input large. Leaves a `<key>.started` marker once running."""
    key = request.text[:8]
    (_markers() / f"{key}.started").touch()
    go = _markers() / f"{key}.go"
    while not go.exists():
        await asyncio.sleep(0.01)
    return Greeting(text=key)


BULK_BYTES = 4_500_000


def varied_text(length: int) -> str:
    """`length` characters that differ from chunk to chunk, so none of the
    chunks a payload is cut into is stored once for all."""
    return random.Random(length).randbytes(length // 2).hex()


@kabudachi.task(name="cluster.bulk")
def bulk(request: Greeting) -> Greeting:
    """Returns a fixed result of `BULK_BYTES`, whatever its input."""
    return Greeting(text=varied_text(BULK_BYTES))


def _join(older: Greeting, newer: Greeting) -> Greeting:
    return Greeting(text=f"{older.text}+{newer.text}", times=newer.times)


@kabudachi.coalescing_task(name="cluster.gather", merge=_join)
async def gather(request: Greeting) -> Greeting:
    """Holds `times` milliseconds, or, for a negative `times`, until the test
    creates a `<text>.go` marker, then returns the input it was given, the
    superseded generations' payloads folded in."""
    await _held(request)
    return request


@kabudachi.task(name="cluster.load")
async def load(request: Greeting) -> Greeting:
    """Says `loaded` before its input's text."""
    return Greeting(text=f"loaded {request.text}")


@kabudachi.task(name="cluster.shout")
async def shout(request: Greeting) -> Greeting:
    """Upper-cases its input's text after `KABUDACHI_TEST_STAGE_SECONDS`, and
    leaves a `shout.<pid>.<n>` marker, holding the process id of the worker
    running it, once it starts, so the test can find the worker."""
    started = f"shout.{os.getpid()}.{time.monotonic_ns()}"
    written = _markers() / f"_{started}"
    written.write_text(str(os.getppid()))
    written.rename(_markers() / started)
    await asyncio.sleep(float(os.environ.get("KABUDACHI_TEST_STAGE_SECONDS", "0")))
    return Greeting(text=request.text.upper())


@kabudachi.task(name="cluster.plan")
async def plan(request: Greeting) -> kabudachi.Flow:
    """Returns a flow, which runs as this task's continuation."""
    return kabudachi.flow(load.bind(request), shout)


@kabudachi.task(name="cluster.pipeline")
async def pipeline(request: Greeting) -> list[Greeting]:
    """Awaits a flow, a group and a returned step from inside a body: on a
    networked worker each is a composite the leader runs."""
    flowed = await kabudachi.flow(load.bind(request), shout)()
    grouped = await kabudachi.group(load.bind(request), shout.bind(request))()
    planned = await plan(request)
    return [flowed[-1], *grouped, *planned]


TASKS = {
    "spin": spin,
    "hold": hold,
    "nests": nests,
    "awaits": awaits,
    "fleeting": fleeting,
    "echo": echo,
    "fails": fails,
    "inflate": inflate,
    "weigh": weigh,
    "bulk": bulk,
    "gather": gather,
    "pipeline": pipeline,
    "plan": plan,
}
