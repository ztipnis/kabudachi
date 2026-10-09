"""Three real networked workers on loopback, seeded from one, each with one
task process of one place. Runs are found through the shard's Task records.
The tests kill a task process, a worker and a leader with SIGKILL and check
each run certifies once, by its result's digest."""

import os
import signal
import time

import pytest

from cluster import (
    HEARTBEAT_TIMEOUT,
    RECONNECT_TIMEOUT,
    Cluster,
    certified_once,
    digest_of,
    eventually,
    running_on,
    states,
)
from networked_tasks import gather, hold, spin
from proto_messages import Greeting


def finished(cluster, task_id):
    """The task's record, once it is finished."""

    def probe():
        record = cluster.record(task_id)
        return record if record is not None and record["finished"] else None

    return eventually(f"task {task_id} to finish", probe)


@pytest.fixture
def cluster(tmp_path):
    started = Cluster.start(tmp_path)
    yield started
    started.close()


def test_a_cluster_compacts_and_keeps_its_runs_through_a_busy_task_process_a_killed_one_a_cancel_and_a_killed_worker(
    cluster, tmp_path
):
    leader = cluster.leader()
    before = leader.leader()

    # A task process that keeps a CPU busy past the heartbeat timeout: the
    # heartbeats run in the worker, so no leader is suspected and no run aborted.
    spun = leader.submit("spin", times=2000)
    record = certified_once(cluster, spun, digest_of(spin, Greeting(text="spun")))
    assert states(record) == ["SUCCEEDED"]
    assert all(worker.leader() == before for worker in cluster.workers)

    # A body that awaits a task it called: the result cannot come back to it,
    # so the await raises in its task process, and the error crosses the pipe
    # to fail the run by its own type.
    awaiting = leader.submit("awaits", text="nested")
    record = finished(cluster, awaiting)
    assert [(run["state"], run["failure_kind"]) for run in record["runs"]] == [
        ("FAILED", "RemoteResultUnavailableError")
    ]

    # A coalescing key held by a running generation retains the payloads of
    # those superseded behind it, past the soft memory limit: the leader makes
    # a compaction run, a worker folds the chain in a task process, and the
    # newest generation's record keeps the fold. It then runs on the whole
    # chain, folded in order.
    holding = leader.submit("gather", text="a", times=2000)
    # Running, so it holds the key: the generations after it wait behind it.
    eventually("the first generation to run", lambda: running_on(cluster, holding))
    parts = [letter * 200 for letter in "bcd"]
    newest = [leader.submit("gather", text=part) for part in parts][-1]
    eventually("the retained chain to be compacted", lambda: (cluster.record(newest) or {}).get("folded"))

    # The compaction run's own record entry, if the record lists it, is not
    # the generation's result: the last run that succeeded is.
    record = finished(cluster, newest)
    succeeded = [run["digest"] for run in record["runs"] if run["state"] == "SUCCEEDED"]
    assert succeeded[-1] == digest_of(gather, Greeting(text="+".join(parts))), record

    # The task process running a body is killed: the run is lost within a
    # heartbeat, with no wait for the reconnect timeout, and its retry certifies.
    held = leader.submit("hold", text="child", times=30_000)
    marker = eventually("the body to start", lambda: next(tmp_path.glob("child.*"), None))
    killed_at = time.monotonic()
    os.kill(int(marker.suffix[1:]), signal.SIGKILL)
    eventually("the run to be lost", lambda: "LOST" in states(cluster.record(held)))
    assert time.monotonic() - killed_at < HEARTBEAT_TIMEOUT
    record = certified_once(cluster, held, digest_of(hold, Greeting(text="child")))
    assert states(record) == ["LOST", "SUCCEEDED"]

    # A stored, running task is cancelled through its handle: the cancel goes
    # to the leader, and the worker holding the run stops its body.
    cancelled = leader.submit("hold", text="cancelled", times=30_000)
    eventually("the run to start", lambda: running_on(cluster, cancelled))
    assert leader.ask(cancel=cancelled)["cancelled"] is True
    eventually("the run to be cancelled", lambda: states(cluster.record(cancelled)) == ["CANCELLED"])

    # A worker holding a run is killed: its run is replaced only once the
    # leader has stopped hearing it and its reconnect timeout has passed.
    tasks = [leader.submit("hold", text=f"worker-{n}", times=4000) for n in range(3)]

    def on_a_follower():
        for task in tasks:
            holder = running_on(cluster, task)
            if holder is not None and holder.id != leader.id:
                return task, holder

    task, holder = eventually("a follower to run one of them", on_a_follower)
    killed_at = time.monotonic()
    holder.kill()

    def replaced():
        record = cluster.record(task)
        return record is not None and len(record["runs"]) > 1

    eventually("the run to be replaced", replaced, timeout=15)
    waited = time.monotonic() - killed_at
    assert HEARTBEAT_TIMEOUT + RECONNECT_TIMEOUT - 0.5 <= waited <= HEARTBEAT_TIMEOUT + RECONNECT_TIMEOUT + 6
    for n, each in enumerate(tasks):
        certified_once(cluster, each, digest_of(hold, Greeting(text=f"worker-{n}")))
