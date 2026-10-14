"""Application tasks shared by the functional caller and its worker processes."""

import asyncio
import os
import hashlib
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import kabudachi
from networked_tasks import echo, load
from proto_messages import Greeting, Receipt


class FileBackend:
    """An expiring test backend shared by separate processes through files."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)

    async def store(self, key: str, entry: bytes, expires_at: datetime) -> None:
        path = self.directory / hashlib.sha256(key.encode()).hexdigest()
        with tempfile.NamedTemporaryFile(dir=self.directory, delete=False) as written:
            written.write(str(expires_at.timestamp()).encode() + b"\n" + entry)
            temporary = written.name
        os.replace(temporary, path)

    async def load(self, key: str) -> bytes | None:
        path = self.directory / hashlib.sha256(key.encode()).hexdigest()
        try:
            expires, entry = path.read_bytes().split(b"\n", 1)
        except FileNotFoundError:
            return None
        return entry if datetime.now(timezone.utc).timestamp() < float(expires) else None


@kabudachi.task(name="functional.ordered")
async def ordered(request: Greeting) -> Greeting:
    if request.text.startswith("mapped-"):
        directory = Path(os.environ["KABUDACHI_TEST_MARKERS"])
        (directory / f"{request.text}.started").write_text(str(os.getppid()))
        while not (directory / "mapped.go").exists():
            await asyncio.sleep(0.01)
    await asyncio.sleep(request.times / 1000)
    return Greeting(text=request.text, times=os.getppid())


@kabudachi.reducer(name="functional.subtract")
def subtract(item: Greeting, previous: Greeting) -> Greeting:
    return Greeting(text=f"({previous.text}-{item.text})")


@kabudachi.task(name="functional.items")
def items(request: Greeting) -> list[Greeting]:
    return [Greeting(text=letter) for letter in request.text]


@kabudachi.task(name="functional.receipt")
def receipt(request: Receipt) -> Receipt:
    return request


@kabudachi.task(name="functional.nothing")
def nothing(request: Greeting) -> None:
    return None


@kabudachi.task(name="functional.retry", retries=1)
def retry(request: Greeting) -> Greeting:
    attempted = Path(os.environ["KABUDACHI_TEST_MARKERS"], f"retry.{request.text}")
    try:
        attempted.touch(exist_ok=False)
    except FileExistsError:
        return request
    raise ValueError("retry the first attempt")


@kabudachi.task(name="functional.timeout", timeout=timedelta(milliseconds=100))
async def timed_out(request: Greeting) -> Greeting:
    await asyncio.sleep(60)
    return request


@kabudachi.task(name="functional.group")
def returned_group(request: Greeting) -> kabudachi.Group:
    return kabudachi.group(echo.bind(request), load.bind(request))


@kabudachi.task(name="functional.bound")
def returned_bound(request: Greeting) -> kabudachi.BoundTask:
    return echo.bind(request)


@dataclass
class Number:
    value: int


@kabudachi.task(name="functional.double", serializer="dataclass")
def double(request: Number) -> Number:
    return Number(request.value * 2)


@kabudachi.reducer(name="functional.concatenate", associative=True)
def concatenate(item: Greeting, previous: Greeting) -> Greeting:
    return Greeting(text=previous.text + item.text)


@kabudachi.reducer(name="functional.gated_concatenate", associative=True)
async def gated_concatenate(item: Greeting, previous: Greeting) -> Greeting:
    directory = Path(os.environ["KABUDACHI_TEST_MARKERS"])
    (directory / f"reduce-start.{os.getpid()}").write_text(str(os.getppid()))
    while not (directory / "reduce.go").exists():
        await asyncio.sleep(0.01)
    return Greeting(text=previous.text + item.text)


if os.environ.get("KABUDACHI_TEST_SCHEMA") == "v3":
    @dataclass
    class Order:
        quantity: int
        rush: bool = False

    kabudachi.schema(Order, family="functional.Order", version=3, version_path="$.version")

    @kabudachi.migration(Order, from_version=1)
    def double_quantity(order: dict) -> dict:
        return {**order, "quantity": order["quantity"] * 2}
else:
    @dataclass
    class Order:
        quantity: int
        note: str = ""

    kabudachi.schema(Order, family="functional.Order", version=1, version_path="$.version")


@kabudachi.task(name="functional.migrated", serializer="dataclass", queue="priority")
def migrated(request: Order) -> Number:
    return Number(request.quantity)


@kabudachi.task(name="functional.unsubscribed", queue="unsubscribed")
def unsubscribed(request: Greeting) -> Greeting:
    raise AssertionError("a worker must not claim a queue it does not subscribe to")


@kabudachi.process_init
def initialized() -> None:
    directory = Path(os.environ["KABUDACHI_TEST_MARKERS"])
    (directory / f"initialized.{os.getpid()}").write_text(str(os.getppid()))
