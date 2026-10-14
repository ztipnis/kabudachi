"""A program using an installed kabudachi the way a user's does, run by the
wheel test. Its settings come from the environment (`KABUDACHI_*`), and it
prints JSON lines on standard output.

    python wheel_smoke.py flow    runs a flow, prints its result and where
                                  kabudachi was imported from, and exits
    python wheel_smoke.py serve   prints that it serves, then serves until
                                  its standard input closes

With `KABUDACHI_LISTEN` set it is a worker of a networked shard; without, a
worker on its own."""

import asyncio
import json
import sys

from google.protobuf.wrappers_pb2 import Int64Value

import kabudachi
from kabudachi import _native
from wheel_smoke_tasks import double_then_add_one


def say(**fields) -> None:
    print(json.dumps(fields), flush=True)


async def flow() -> None:
    stages = await double_then_add_one(Int64Value(value=20))
    say(
        result=stages[-1].value,
        package=kabudachi.__file__,
        native=_native.__file__,
    )


async def serve() -> None:
    say(serving=True)
    # Returns once the test closes this program's standard input.
    await asyncio.to_thread(sys.stdin.read)


if __name__ == "__main__":
    # Task processes start with spawn and import this script again: only the
    # worker itself may run.
    kabudachi.run(serve if sys.argv[1] == "serve" else flow)
