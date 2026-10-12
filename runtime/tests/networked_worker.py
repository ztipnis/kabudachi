"""A networked worker, driven over its standard streams by the tests: it
prints one JSON line once it has joined its shard, then answers one JSON
command per line on stdin until stdin closes. Its settings come from the
environment (`KABUDACHI_*`), as a deployed worker's would. Each task's
handle is awaited once, and a `settled` line says how that ended."""

import asyncio
import ctypes
import json
import os
import signal
import sys
import threading

import kabudachi
from kabudachi import _native, runner, session

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

# The client each run claimed here names, by task id.
CLIENTS = {}


class NotingClaims:
    """The worker's runtime, noting the client named by each claim made
    through it."""

    def __init__(self, runtime) -> None:
        self._runtime = runtime

    def __getattr__(self, name: str):
        return getattr(self._runtime, name)

    async def claim_pending(self, limit: int):
        claims = await self._runtime.claim_pending(limit)
        CLIENTS.update((claim.task_id, claim.client) for claim in claims)
        return claims

# How long the drain after stdin closes may take before the worker exits
# anyway, so one whose test was killed (by a Bazel timeout, say) cannot linger.
DRAIN_SECONDS = 10.0

# Linux's prctl option that signals this process when its parent dies.
_PR_SET_PDEATHSIG = 1


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
                "delivered": run.delivered,
            }
            for run in record.runs
        ]
        return {"record": {"finished": record.finished, "folded": record.folded, "runs": runs}}
    if "leader" in command:
        return {"leader": native.leader(), "voters": native.voters()}
    if "cancel" in command:
        # Through the handle the task was submitted with, as a caller would.
        return {"cancelled": HANDLES[command["cancel"]].cancel()}
    if "cancel_by_id" in command:
        # A task submitted elsewhere, by its id alone.
        return {"cancelled": session.current_session().cancel_task(command["cancel_by_id"])}
    if "durable" in command:
        handle = HANDLES[command["durable"]]
        await asyncio.wait_for(handle.durable(kabudachi.Durability.REPLICATED), 20)
        return {"durability": handle.durability.name}
    if "status" in command:
        status = await HANDLES[command["status"]].status()
        return {"status": {"state": status.state, "attempt": status.attempt, "delivered": status.delivered}}
    if "claimed" in command:
        return {"client": CLIENTS.get(command["claimed"])}
    raise ValueError(f"unknown command {command!r}")


async def main() -> None:
    native = session.current_session().runtime
    say(ready={
        "worker": native.worker_id(),
        "address": native.address(),
        "shard": native.shard_id(),
        "certifies_locally": native.certifies_locally,
    })
    while line := await asyncio.to_thread(sys.stdin.readline):
        command = json.loads(line)
        say(id=command.pop("id"), **(await answer(native, command)))
    # Stdin closed: `kabudachi.run` drains after this returns, within a bound.
    bound = threading.Timer(DRAIN_SECONDS, os._exit, (3,))
    bound.daemon = True
    bound.start()


def die_with_parent() -> None:
    """On Linux, a SIGKILL once the test process that started this one dies."""
    if sys.platform.startswith("linux"):
        ctypes.CDLL(None, use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGKILL)


if __name__ == "__main__":
    # Task processes are started with spawn, which imports this script again
    # in each of them: only the worker itself may run.
    die_with_parent()
    native_runtime = runner._native_runtime
    runner._native_runtime = lambda settings: NotingClaims(native_runtime(settings))
    kabudachi.run(main)
