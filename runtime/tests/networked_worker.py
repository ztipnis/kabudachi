"""A networked worker, driven over its standard streams by the tests: it
prints one JSON line once it has joined its shard, then answers one JSON
command per line on stdin until stdin closes. Its settings come from the
environment (`KABUDACHI_*`), as a deployed worker's would. Each task's
handle is awaited once, and a `settled` line says how that ended."""

import asyncio
import json
import sys

import kabudachi
from kabudachi import _native, session

import networked_tasks
from proto_messages import Greeting

_STATES = {
    getattr(_native.RunState, name): name
    for name in (
        "SCHEDULED", "QUEUED", "CLAIMED", "RUNNING", "SUCCEEDED", "FAILED",
        "EXPIRED", "SUPERSEDED", "CANCELLED", "LOST", "ORPHANED",
    )
}


def say(**message) -> None:
    print(json.dumps(message), flush=True)


async def settled(handle) -> None:
    try:
        await handle
        say(settled=handle.task_id, error=None)
    except Exception as error:
        say(settled=handle.task_id, error=type(error).__name__)


# The handle of every task submitted here, by task id.
HANDLES = {}


async def answer(native, command: dict) -> dict:
    if "submit" in command:
        task = networked_tasks.TASKS[command["submit"]]
        handle = task(Greeting(text=command.get("text", ""), times=command.get("times", 0)))
        HANDLES[handle.task_id] = handle
        asyncio.create_task(settled(handle))
        return {"task_id": handle.task_id}
    if "record" in command:
        record = await native.task_record(command["record"])
        if record is None:
            return {"record": None}
        runs = [
            {
                "id": run.task_run_id,
                "state": _STATES[run.state],
                "worker": run.worker,
                "failure_kind": run.failure_kind,
                "digest": None if run.result_digest is None else run.result_digest.hex(),
            }
            for run in record.runs
        ]
        return {"record": {"finished": record.finished, "folded": record.folded, "runs": runs}}
    if "leader" in command:
        return {"leader": native.leader(), "voters": native.voters()}
    if "cancel" in command:
        # Through the handle the task was submitted with, as a caller would.
        return {"cancelled": HANDLES[command["cancel"]].cancel()}
    raise ValueError(f"unknown command {command!r}")


async def main() -> None:
    native = session.current_session().runtime
    say(ready={"worker": native.worker_id(), "address": native.address(), "shard": native.shard_id()})
    while line := await asyncio.to_thread(sys.stdin.readline):
        command = json.loads(line)
        say(id=command.pop("id"), **(await answer(native, command)))


if __name__ == "__main__":
    # Task processes are started with spawn, which imports this script again
    # in each of them: only the worker itself may run.
    kabudachi.run(main)
