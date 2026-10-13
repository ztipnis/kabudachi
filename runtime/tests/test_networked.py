"""Three real networked workers on loopback, seeded from one, each with one
task process of one place. Runs are found through the shard's Task records,
or, while a worker is stopped, through the start markers of the bodies that
run them, and each handle's end through the `settled` notice of the worker that
submitted it. The tests kill a task process, a worker and a leader with
SIGKILL, and stop workers with SIGSTOP, and check each run certifies once,
by its result's digest, and each handle hears how its task ended."""

import os
import signal
import time
from contextlib import contextmanager, suppress

import pytest

from cluster import (
    HEARTBEAT_TIMEOUT,
    RECONNECT_TIMEOUT,
    Cluster,
    certified_once,
    digest_of,
    eventually,
    running_on,
    settled,
    states,
)
from networked_tasks import awaits, fleeting, gather, hold, measure, spin
from proto_messages import Greeting


def finished(shard, task_id):
    """The task's record, read through `shard` (a `Cluster` or a `Worker`),
    once it is finished."""

    def probe():
        record = shard.record(task_id)
        return record if record is not None and record["finished"] else None

    return eventually(f"task {task_id} to finish", probe)


def all_running(cluster, tasks):
    """Each task's worker, once every task runs: with one place a worker,
    each on a worker of its own."""
    holders = {task: running_on(cluster, task) for task in tasks}
    return holders if None not in holders.values() else None


@contextmanager
def stopped(*workers):
    """The workers stopped with SIGSTOP until the block ends; one killed
    meanwhile is left dead."""
    for worker in workers:
        os.kill(worker.pid, signal.SIGSTOP)
    try:
        yield
    finally:
        for worker in workers:
            with suppress(ProcessLookupError):
                os.kill(worker.pid, signal.SIGCONT)


@pytest.fixture
def cluster(tmp_path):
    started = Cluster.start(tmp_path)
    yield started
    started.close()


def test_a_cluster_delivers_results_compacts_and_keeps_its_runs_through_a_busy_task_process_a_killed_one_a_cancel_and_a_killed_worker(
    cluster, tmp_path
):
    leader = cluster.leader()
    delivered_to_its_submitter(cluster, leader)
    carried_as_blobs(cluster, leader)
    awaited_across_workers(cluster, leader)
    compacted(cluster, leader)
    task_process_killed(cluster, leader, tmp_path)
    cancelled_while_running(cluster, leader, tmp_path)
    worker_killed(cluster, leader)


def delivered_to_its_submitter(cluster, leader):
    # A task process that keeps a CPU busy past the heartbeat timeout: the
    # heartbeats run in the worker, so no leader is suspected and no run aborted.
    # A follower submits it, through the leader.
    before = leader.leader()
    follower = next(worker for worker in cluster.workers if worker is not leader)
    spun = follower.submit("spin", times=2000)
    record = certified_once(cluster, spun, digest_of(spin, Greeting(text="spun")))
    assert states(record) == ["SUCCEEDED"]
    assert all(worker.leader() == before for worker in cluster.workers)

    # The follower that submitted it awaits it: the result came back from
    # whichever worker ran it, after the leader stored and certified it.
    assert not follower.certifies_locally
    assert eventually("the follower's handle to settle", lambda: settled(follower, spun))["error"] is None
    assert follower.ask(durable=spun)["durability"] == "REPLICATED"
    assert follower.ask(status=spun)["status"] == {"state": "succeeded", "attempt": 1, "delivered": True}
    # The worker that ran it was told whom to deliver to.
    runner = cluster.by_id(record["runs"][0]["worker"])
    assert runner.ask(claimed=spun)["client"] == follower.id


def carried_as_blobs(cluster, leader):
    # An input larger than one message travels as blobs: the follower stores
    # its chunks where the leader will place the task's record, and whichever
    # worker runs it reads them back whole.
    follower = next(worker for worker in cluster.workers if worker is not leader)
    big = ("0123456789" * (3 * 1024 * 1024 // 10 + 1))[: 3 * 1024 * 1024]
    measured = follower.submit("measure", text=big)
    certified_once(cluster, measured, digest_of(measure, Greeting(text=big[-5:], times=len(big))))


def awaited_across_workers(cluster, leader):
    # A body that awaits a task it called gets its result, wherever each ran,
    # and sees the call kept by the shard.
    awaiting = leader.submit("awaits", text="nested")
    record = certified_once(cluster, awaiting, digest_of(awaits, Greeting(text="REPLICATED")))
    assert [run["delivered"] for run in record["runs"]] == [True]
    assert eventually("the caller's handle to settle", lambda: settled(leader, awaiting))["error"] is None

    # A result too large for one frame fails its run, and its awaiter hears why.
    too_big = leader.submit("big")
    record = finished(cluster, too_big)
    assert [(run["state"], run["failure_kind"]) for run in record["runs"]] == [
        ("FAILED", "ResultTooLargeError")
    ]
    assert eventually("the big task's handle", lambda: settled(leader, too_big))["error"] == (
        "ResultTooLargeError"
    )


def compacted(cluster, leader):
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


def task_process_killed(cluster, leader, tmp_path):
    # The task process running a body is killed: the run is lost as soon as
    # its worker sees the process die, with no wait for the body's long hold,
    # and its retry certifies.
    held = leader.submit("hold", text="child", times=30_000)
    marker = eventually("the body to start", lambda: next(tmp_path.glob("child.*"), None))
    killed_at = time.monotonic()
    os.kill(int(marker.suffix[1:]), signal.SIGKILL)
    eventually("the run to be lost", lambda: "LOST" in states(cluster.record(held)))
    assert time.monotonic() - killed_at < HEARTBEAT_TIMEOUT + RECONNECT_TIMEOUT
    record = certified_once(cluster, held, digest_of(hold, Greeting(text="child")))
    assert states(record) == ["LOST", "SUCCEEDED"]


def cancelled_while_running(cluster, leader, tmp_path):
    # A stored, running task is cancelled through its handle: the cancel goes
    # to the leader, and the worker holding the run stops its body.
    cancelled = leader.submit("hold", text="cancelled", times=30_000)
    eventually("the run to start", lambda: running_on(cluster, cancelled))
    leader.ask(cancel=cancelled)
    eventually("the run to be cancelled", lambda: states(cluster.record(cancelled)) == ["CANCELLED"])
    eventually("the body to stop", lambda: (tmp_path / "cancelled.stopped").exists(), timeout=5)


def worker_killed(cluster, leader):
    # A worker holding a run of a task that is never replayed is killed: the
    # run is lost only once the leader has stopped hearing it and its
    # reconnect timeout has passed, and its handle hears so. One of the three
    # runs on the leader's own worker, which delivers its result to itself.
    tasks = [leader.submit("fleeting", text=f"worker-{n}", times=8000) for n in range(3)]
    holders = eventually("all three to run", lambda: all_running(cluster, tasks))
    assert leader in holders.values()
    task, holder = next((task, holder) for task, holder in holders.items() if holder is not leader)
    killed_at = time.monotonic()
    holder.kill()

    eventually("the killed run to be lost", lambda: states(cluster.record(task)) == ["LOST"], timeout=15)
    waited = time.monotonic() - killed_at
    assert HEARTBEAT_TIMEOUT + RECONNECT_TIMEOUT - 0.5 <= waited <= HEARTBEAT_TIMEOUT + RECONNECT_TIMEOUT + 6
    assert eventually("its handle", lambda: settled(leader, task))["error"] == "TaskLostError"
    for n, each in enumerate(tasks):
        if each != task:
            record = certified_once(cluster, each, digest_of(fleeting, Greeting(text=f"worker-{n}")))
            assert [run["delivered"] for run in record["runs"]] == [True]
            assert eventually("its handle", lambda: settled(leader, each))["error"] is None


def test_a_cut_off_worker_stops_its_run_by_its_deadline_an_unreached_client_still_hears_its_tasks_ends_and_runs_certify_once_when_their_leader_is_killed(
    cluster, tmp_path
):
    cut_off_by_its_deadline(cluster, tmp_path)
    eventually("the shard to settle on one leader", cluster.led)
    unreached_while_the_leader_is_killed(cluster, tmp_path)


def cut_off_by_its_deadline(cluster, tmp_path):
    leader = cluster.leader()
    held = {leader.submit("hold", text=f"cut-{n}", times=30_000): f"cut-{n}" for n in range(3)}

    def on_a_follower():
        for task in held:
            holder = running_on(cluster, task)
            if holder is not None and holder is not leader:
                return task, holder

    cut_off, holder = eventually("a follower to run one of them", on_a_follower)
    for task in held:
        if task != cut_off:
            leader.ask(cancel=task)
    text = held[cut_off]
    # With the other two stopped, nobody can tell the holder to stop its run:
    # only its own abort deadline can.
    stopped_at = time.monotonic()
    with stopped(*(worker for worker in cluster.workers if worker is not holder)):
        eventually("the cut-off run to stop", lambda: (tmp_path / f"{text}.stopped").exists(), timeout=5)
        waited = time.monotonic() - stopped_at
    # The abort deadline falls (suspicion + reconnect timeout) x 0.9 after
    # the last heartbeat the leader acknowledged, about 2.7 s here; the body
    # is asked to stop its cancel grace before that, about 2.2 s.
    assert 0.5 <= waited <= RECONNECT_TIMEOUT + 1
    record = certified_once(cluster, cut_off, digest_of(hold, Greeting(text=text)))
    assert states(record)[0] == "LOST"


# A hold's `times` that holds it until the test lets it go.
GATED = -1


def started_on(cluster, markers, texts):
    """The worker running the first run of each text, by its start marker,
    once each has started."""
    holders = {}
    for text in texts:
        found = [marker for marker in markers.glob(f"{text}.*") if marker.suffix[1:].isdigit()]
        if not found:
            return None
        worker_pid = int(found[0].read_text())
        holders[text] = next(worker for worker in cluster.workers if worker.pid == worker_pid)
    return holders


def let_go(markers, texts):
    """Lets each gated hold return, and waits until each has."""
    for text in texts:
        (markers / f"{text}.go").touch()
    eventually("the bodies to end", lambda: all((markers / f"{text}.ended").exists() for text in texts))


def unreached_while_the_leader_is_killed(cluster, tmp_path):
    # A follower, the client, submits three tasks, one running on each
    # worker, and is stopped once its own is cancelled. The other two runs
    # cannot deliver to it within their reconnect timeout: one certifies
    # with its result undelivered, the other is cancelled while it delivers.
    # The leader is then killed while it and the other follower each run a
    # task, so the client, resumed, learns how its tasks ended only by
    # asking the leader the two survivors elect, under which every run
    # certifies once.
    leader = cluster.leader()
    client, other = [worker for worker in cluster.workers if worker is not leader]
    texts, holders, own, unreached, interrupted = one_on_each_worker(cluster, tmp_path, client)
    client.ask(cancel=own)
    eventually("its own body to stop", lambda: (tmp_path / f"{texts[own]}.stopped").exists())

    with stopped(client):
        let_go(tmp_path, [texts[unreached], texts[interrupted]])
        # Its body has ended and its result is on its way to the stopped client.
        assert leader.ask(cancel_by_id=interrupted)["cancelled"]
        reported(holders[texts[unreached]])
        # Held until let go, so the leader is killed while it and the other
        # follower each run one; started while the leader's record is read.
        tasks = {other.submit("hold", text=f"leader-{n}", times=GATED): f"leader-{n}" for n in range(3)}
        undelivered(leader, unreached)
        killed_while_running(cluster, tmp_path, leader, other, tasks.values())
    let_go(tmp_path, tasks.values())

    unreached_and_interrupted_heard(cluster, tmp_path, client, unreached, interrupted, texts)
    certified_once_and_settled(cluster, other, tasks)


def one_on_each_worker(cluster, tmp_path, client):
    """Three gated holds `client` submits, once one runs on each worker:
    each task's text, each text's worker, the task `client` runs itself, and
    the other two."""
    texts = {client.submit("hold", text=f"unreached-{n}", times=GATED): f"unreached-{n}" for n in range(3)}
    holders = eventually("all three to run", lambda: started_on(cluster, tmp_path, texts.values()))
    [own] = [task for task, text in texts.items() if holders[text] is client]
    unreached, interrupted = [task for task in texts if task != own]
    return texts, holders, own, unreached, interrupted


def unreached_and_interrupted_heard(cluster, tmp_path, client, unreached, interrupted, texts):
    """`client`, resumed, hears its undelivered task's result is unavailable
    and its other task was cancelled before its body ended."""
    assert eventually("the unreached task's handle", lambda: settled(client, unreached), timeout=30)["error"] == (
        "ResultUnavailableError"
    )
    assert eventually("the cancelled task's handle", lambda: settled(client, interrupted))["error"] == (
        "TaskCancelledError"
    )
    assert states(cluster.record(interrupted)) == ["CANCELLED"]
    assert not (tmp_path / f"{texts[interrupted]}.stopped").exists(), "cancelled before its body ended"


def certified_once_and_settled(cluster, follower, tasks):
    """Each of `tasks` certifies once, and `follower`, which submitted them,
    hears each end with its result."""
    for task, text in tasks.items():
        certified_once(cluster, task, digest_of(hold, Greeting(text=text)))
        # The follower that submitted them awaits all three: each settles
        # with its result, told by the new leader or asked of it.
        assert eventually("the follower's handle", lambda: settled(follower, task), timeout=30)["error"] is None


def reported(holder):
    """Once `holder` gave up delivering the run it holds and the leader took
    its report."""
    eventually("the unreached run's report to be taken", lambda: holder.ask(reported=None)["reported"])


def undelivered(leader, unreached):
    """The leader has the run it took the report on certified, undelivered.
    A record read waits out a query timeout on the stopped client, so it is
    read once."""
    record = leader.record(unreached)
    assert record is not None and record["finished"], record
    assert [(run["state"], run["delivered"]) for run in record["runs"]] == [("SUCCEEDED", False)]


def killed_while_running(cluster, tmp_path, leader, follower, texts):
    """Kills the leader once it and `follower` each run a hold of one of
    `texts`."""

    def spread():
        running = [started_on(cluster, tmp_path, [text]) for text in texts]
        return {leader, follower} <= {worker for holders in running if holders for worker in holders.values()}

    eventually("the leader and the follower to each run one", spread)
    leader.kill()
