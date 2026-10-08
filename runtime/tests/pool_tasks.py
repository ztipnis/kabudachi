"""Tasks at module scope, for tests whose bodies run in task processes: the
processes find them by importing this module. Bodies leave marks in the
directory `KABUDACHI_TEST_MARKERS` names, which a test shares with its task
processes."""

import asyncio
import os
import signal
import time
from datetime import timedelta
from pathlib import Path

import kabudachi
from kabudachi.errors import TaskCancelledError
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


@kabudachi.task(name="pool.cancels_itself")
async def cancels_itself(request: Greeting) -> Greeting:
    """Raises CancelledError though nobody asked it to stop."""
    raise asyncio.CancelledError


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


@kabudachi.task(name="pool.leaf")
async def leaf(request: Greeting) -> Greeting:
    return Greeting(times=request.times + 1)


@kabudachi.task(name="pool.calls_others")
async def calls_others(request: Greeting) -> Greeting:
    one = await leaf(request)
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
    return Greeting(times=total, text=f"{cancelled} {ended}")
