"""Real networked worker processes for the tests: started on loopback in a
shard of their own, driven over their standard streams, and killed whole or
through their task processes. Every process is killed when the test ends."""

import itertools
import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from kabudachi import _native
from kabudachi.serializers import process_serializers

WORKER = Path(__file__).with_name("networked_worker.py")
STARTUP_SECONDS = 60.0
POLL_SECONDS = 0.05

# Short enough for tests, in the ratios a deployment keeps: three heartbeats
# to a suspicion, and a reconnect timeout past it.
TIMINGS = {
    "KABUDACHI_HEARTBEAT_INTERVAL": "0.1",
    "KABUDACHI_HEARTBEAT_TIMEOUT": "1",
    "KABUDACHI_RECONNECT_TIMEOUT": "2",
    "KABUDACHI_CANCEL_GRACE": "0.5",
}
HEARTBEAT_TIMEOUT = 1.0
RECONNECT_TIMEOUT = 2.0


def eventually(description, probe, timeout=20.0):
    """The first truthy value `probe()` returns, polled; fails naming
    `description` if none comes within `timeout` seconds."""
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for {description}")
        time.sleep(POLL_SECONDS)


class Worker:
    """One worker process and its standard streams."""

    def __init__(self, environment: dict) -> None:
        self.process = subprocess.Popen(
            [sys.executable, str(WORKER)],
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path), **environment},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            # Its own process group, so the test can kill its task processes with it.
            start_new_session=True,
        )
        self.pid = self.process.pid
        self._lines: queue.Queue = queue.Queue()
        self.notices: list[dict] = []
        self._ids = itertools.count()
        threading.Thread(target=self._read, daemon=True).start()
        try:
            ready = self._next(lambda line: "ready" in line, STARTUP_SECONDS)["ready"]
        except BaseException:
            self.close()
            raise
        self.id, self.address, self.shard = ready["worker"], ready["address"], ready["shard"]

    def _read(self) -> None:
        for line in self.process.stdout:
            if line.startswith("{"):
                self._lines.put(json.loads(line))
        self._lines.put(None)

    def _next(self, wanted, timeout: float) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            try:
                line = self._lines.get(timeout=max(0.0, deadline - time.monotonic()))
            except queue.Empty:
                raise AssertionError(f"worker {self.pid} did not answer in time") from None
            if line is None:
                raise AssertionError(f"worker {self.pid} exited with {self.process.wait()}")
            if wanted(line):
                return line
            self.notices.append(line)

    def ask(self, **command) -> dict:
        asked = next(self._ids)
        self.process.stdin.write(json.dumps({"id": asked, **command}) + "\n")
        self.process.stdin.flush()
        return self._next(lambda line: line.get("id") == asked, 20.0)

    def submit(self, task: str, text: str = "", times: int = 0) -> str:
        return self.ask(submit=task, text=text, times=times)["task_id"]

    def record(self, task_id: str) -> dict | None:
        return self.ask(record=task_id)["record"]

    def leader(self) -> tuple[str, int] | None:
        answer = self.ask(leader=None)["leader"]
        return None if answer is None else tuple(answer)

    def voters(self) -> int:
        return self.ask(leader=None)["voters"]

    def kill(self) -> None:
        """SIGKILL to the worker alone; its task processes see their pipe close."""
        self.process.kill()
        self.process.wait()

    def stop(self, timeout: float = 30.0) -> int:
        """Closes its standard input, which ends its program's main: it then
        drains as `kabudachi.run` does after main (a signal would not, since
        `run` with main leaves signals alone). The exit code once it has."""
        self.process.stdin.close()
        return self.process.wait(timeout=timeout)

    def close(self) -> None:
        try:
            os.killpg(self.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.process.wait()


class Cluster:
    """Three workers of one shard, seeded from the first, which founds it."""

    def __init__(self, workers: list[Worker]) -> None:
        self.workers = workers

    @classmethod
    def start(cls, markers: Path, size: int = 3) -> "Cluster":
        environment = {
            **TIMINGS,
            "KABUDACHI_LISTEN": "/ip4/127.0.0.1/tcp/0",
            "KABUDACHI_PROCESSES": "1",
            "KABUDACHI_CONCURRENCY": "1",
            # Past a few hundred bytes of pending input the leader compacts
            # a coalescing key's retained chain; far below anything refused.
            "KABUDACHI_MEMORY_SOFT_LIMIT": "300",
            "KABUDACHI_MEMORY_HARD_LIMIT": "1000000",
            "KABUDACHI_TEST_MARKERS": str(markers),
        }
        founder = Worker(environment)
        workers = [founder]
        try:
            seeded = {**environment, "KABUDACHI_SEEDS": founder.address}
            # One at a time, so a start that fails leaves the earlier ones
            # listed, and closed below.
            for _ in range(size - 1):
                workers.append(Worker(seeded))
            cluster = cls(workers)
            eventually("every worker to follow one leader of three voters", cluster.led)
        except BaseException:
            for worker in workers:
                worker.close()
            raise
        return cluster

    def led(self) -> bool:
        leaders = {worker.leader() for worker in self.workers}
        if len(leaders) != 1 or None in leaders:
            return False
        return self.leader().voters() == len(self.workers)

    def live(self) -> list[Worker]:
        return [worker for worker in self.workers if worker.process.poll() is None]

    def leader(self) -> Worker:
        leader_id, _ = self.live()[0].leader()
        return self.by_id(leader_id)

    def by_id(self, worker_id: str) -> Worker:
        return next(worker for worker in self.workers if worker.id == worker_id)

    def record(self, task_id: str) -> dict | None:
        return self.live()[0].record(task_id)

    def close(self) -> None:
        for worker in self.workers:
            worker.close()


def digest_of(task, value) -> str:
    """The hex digest a run of `task` returning `value` is certified by."""
    definition = task.definition
    encoded = process_serializers().get(definition.serializer).encode(value, definition.output_type)
    return _native.result_digest(encoded).hex()


def states(record) -> list[str]:
    return [] if record is None else [run["state"] for run in record["runs"]]


def certified_once(shard, task_id: str, digest: str) -> dict:
    """The task's record, read through `shard` (a `Cluster` or a `Worker`),
    once it is finished with exactly one run succeeded, by `digest`."""

    def finished():
        record = shard.record(task_id)
        return record if record is not None and record["finished"] else None

    record = eventually(f"task {task_id} to finish", finished, timeout=30)
    succeeded = [run for run in record["runs"] if run["state"] == "SUCCEEDED"]
    assert [run["digest"] for run in succeeded] == [digest], record
    return record


def running_on(cluster: Cluster, task_id: str) -> Worker | None:
    """The worker holding the task's running run, once there is one."""
    record = cluster.record(task_id)
    runs = [] if record is None else record["runs"]
    if runs and runs[-1]["state"] == "RUNNING":
        return cluster.by_id(runs[-1]["worker"])
    return None
