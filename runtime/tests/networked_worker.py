"""A networked worker, driven over its standard streams by the tests: it
prints one JSON line once it has joined its shard, then answers one JSON
command per line on stdin until stdin closes. Its settings come from the
environment (`KABUDACHI_*`), as a deployed worker's would. Each task's
handle is awaited once, and a `settled` line says how that ended."""

import asyncio
import ctypes
import importlib
import json
import os
import signal
import sys
import threading
import time
import traceback

import kabudachi
from kabudachi import _native, runner, session

from proto_messages import Greeting

# The tasks this worker's program declares: `networked_tasks`, or the module
# a test names to run its own.
tasks = importlib.import_module(os.environ.get("KABUDACHI_TEST_TASKS", "networked_tasks"))

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
        result = await handle
        results = result if isinstance(result, list) else [result]
        say(
            settled=handle.task_id,
            error=None,
            texts=[item.text for item in results],
            value=_dump(result),
        )
    except Exception as error:
        say(settled=handle.task_id, error=type(error).__name__, value=None)


def _dump(result):
    """A JSON form of the result: its `model_dump()` when it is a model, else its `text`."""
    if isinstance(result, list):
        return [_dump(item) for item in result]
    return result.model_dump() if hasattr(result, "model_dump") else result.text


# The handle of every task submitted here, by task id.
HANDLES = {}

# The client each run claimed here names, by task id.
CLIENTS = {}

# How many times SlowDown went from cleared to raised here, as sampled.
RAISES = 0
# The sampling task, held here so it is not garbage collected while it runs.
SAMPLING: asyncio.Task | None = None
SLOW_DOWN_SAMPLE_SECONDS = 0.02


async def count_slow_down_raises(native) -> None:
    global RAISES
    raised = False
    while True:
        now = native.slow_down()
        RAISES += now and not raised
        raised = now
        await asyncio.sleep(SLOW_DOWN_SAMPLE_SECONDS)


class NotingClaims:
    """The worker's runtime, noting the client named by each claim made
    through it. A private seam: the worker swaps it in for the runtime
    `kabudachi.run` makes (`runner._native_runtime`), since no public API
    shows which client a claim named."""

    def __init__(self, runtime) -> None:
        self._runtime = runtime
        self._claimed_the_last = False

    def __getattr__(self, name: str):
        return getattr(self._runtime, name)

    async def claim_pending(self, limit: int):
        if os.environ.get("KABUDACHI_TEST_CLAIMS_NOTHING") or self._claimed_the_last:
            # Never asks the runtime for claims, so Python offers its driver
            # no places and the worker runs nothing more: a pure client.
            await asyncio.Event().wait()
        claims = await self._runtime.claim_pending(limit)
        CLIENTS.update((claim.task_id, claim.client) for claim in claims)
        # Written for the test, which reads them after this worker may have
        # been killed: the tasks a pipeline made, and so which records to read.
        markers = os.environ.get("KABUDACHI_TEST_MARKERS")
        for claim in claims:
            if markers and claim.definition_id.startswith(("cluster.", "functional.")):
                name = f"claimed.{claim.definition_id.removeprefix('cluster.')}.{claim.task_run_id}"
                with open(os.path.join(markers, name), "w") as written:
                    written.write(claim.task_id)
        # A worker that claimed the task named here runs nothing besides it,
        # so what that task calls runs on the other workers.
        last = os.environ.get("KABUDACHI_TEST_CLAIMS_NOTHING_AFTER")
        self._claimed_the_last = bool(last) and any(claim.definition_id == last for claim in claims)
        return claims

# How long the drain after stdin closes may take before the worker exits
# anyway, so one whose test was killed (by a Bazel timeout, say) cannot linger.
DRAIN_SECONDS = 10.0

# Linux's prctl option that signals this process when its parent dies.
_PR_SET_PDEATHSIG = 1


async def answer(native, command: dict) -> dict:
    if "submit" in command:
        task = tasks.TASKS[command["submit"]]
        if "input" in command:
            argument = task.definition.input_type.model_validate(command["input"])
        else:
            argument = Greeting(text=command.get("text", ""), times=command.get("times", 0))
        handle = task(argument)
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
                "failure_message": run.failure_message,
                "claimed_at": run.claimed_at_ms,
                "started_at": run.started_at_ms,
                "digest": None if run.result_digest is None else run.result_digest.hex(),
                "delivered": run.delivered,
                "persistence": run.persistence,
                "source_version": run.source_version,
                "execution_version": run.execution_version,
            }
            for run in record.runs
        ]
        return {
            "record": {
                "finished": record.finished,
                "folded": record.folded,
                "blocked_reason": record.blocked_reason,
                "runs": runs,
            }
        }
    if "resume" in command:
        try:
            value = await asyncio.wait_for(kabudachi.resume(command["resume"]), 30)
        except Exception as error:
            return {"resumed_error": type(error).__name__}
        # A composite resumes as the list of its members' results.
        return {"resumed": [item.text for item in value] if isinstance(value, list) else value.text}
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
        # A record read waits on a shard peer that is stopped but connected.
        status = await asyncio.wait_for(HANDLES[command["status"]].status(), 20)
        assert isinstance(status, kabudachi.TaskStatus)
        return {"status": {"state": status.state, "attempt": status.attempt, "delivered": status.delivered}}
    if "slow_down" in command:
        return {"slow_down": native.slow_down(), "raises": RAISES}
    if "claimed" in command:
        return {"client": CLIENTS.get(command["claimed"])}
    if "reported" in command:
        # Whether the leader took the last report on every run claimed here,
        # waited for up to the reconnect timeout.
        return {"reported": await native.wait_until_reported()}
    raise ValueError(f"unknown command {command!r}")


async def main() -> None:
    global SAMPLING
    native = session.current_session().runtime
    SAMPLING = asyncio.create_task(count_slow_down_raises(native))
    say(ready={
        "worker": native.worker_id(),
        "address": native.address(),
        "shard": native.shard_id(),
        "certifies_locally": native.certifies_locally,
        "health": native.health_address(),
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
    if backend := os.environ.get("KABUDACHI_TEST_BACKEND"):
        kabudachi.configure(result_backend=tasks.FileBackend(backend))
    native_runtime = runner._native_runtime
    runner._native_runtime = lambda settings: NotingClaims(native_runtime(settings))
    # Control belongs to the test harness. Production startup receives no
    # caller function and the shard's workers execute work independently.
    def control() -> None:
        while (active := session.active_session()) is None:
            time.sleep(0.01)
        future = asyncio.run_coroutine_threadsafe(main(), active._hosted._loop)
        try:
            future.result()
        except BaseException:
            traceback.print_exc()
            raise
        finally:
            os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=control, daemon=True).start()
    kabudachi.run()
