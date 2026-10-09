"""Tasks for the networked worker tests. A body that holds leaves a marker
file named for its text and its process id in `KABUDACHI_TEST_MARKERS`, a
directory the test owns, so the test can find the task process running it;
only the first run of a text holds."""

import asyncio
import os
import time
from pathlib import Path

import kabudachi
from kabudachi.errors import RemoteResultUnavailableError
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
    """Holds for `times` milliseconds the first time `text` runs; a later run
    of the same text returns at once. A hold asked to stop leaves a
    `<text>.stopped` marker."""
    first = not any(_markers().glob(f"{request.text}.*"))
    (_markers() / f"{request.text}.{os.getpid()}").touch()
    if first:
        try:
            await asyncio.sleep(request.times / 1000)
        except asyncio.CancelledError:
            (_markers() / f"{request.text}.stopped").touch()
            raise
    return Greeting(text=request.text)


@kabudachi.task(name="cluster.nests")
async def nests(request: Greeting) -> Greeting:
    """Calls `hold` and awaits its result, and says what that did."""
    try:
        await hold(Greeting(text=request.text))
    except RemoteResultUnavailableError as error:
        return Greeting(text=type(error).__name__)
    return Greeting(text="delivered")


@kabudachi.task(name="cluster.awaits")
async def awaits(request: Greeting) -> Greeting:
    """Calls `hold` and awaits its result; what that raises fails the run."""
    await hold(Greeting(text=request.text))
    return Greeting(text="delivered")


def _join(older: Greeting, newer: Greeting) -> Greeting:
    return Greeting(text=f"{older.text}+{newer.text}", times=newer.times)


@kabudachi.coalescing_task(name="cluster.gather", merge=_join)
async def gather(request: Greeting) -> Greeting:
    """Holds `times` milliseconds, then returns the input it was given, the
    superseded generations' payloads folded in."""
    await asyncio.sleep(request.times / 1000)
    return request


TASKS = {"spin": spin, "hold": hold, "nests": nests, "awaits": awaits, "gather": gather}
