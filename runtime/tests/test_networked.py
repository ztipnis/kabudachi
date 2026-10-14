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
    STAGE_SECONDS,
    TIMINGS,
    Cluster,
    Worker,
    certified_once,
    digest_of,
    eventually,
    free_port,
    outcome,
    probe,
    running_on,
    settled,
    states,
)
from networked_tasks import BULK_BYTES, awaits, bulk, echo, fleeting, gather, hold, maps, spin, varied_text
from proto_messages import Greeting


# What `pipeline` of "a" gives: a flow's last result, a group's results, and
# a returned flow's results, and what a failed group member's client was told.
PIPELINE_OF_A = ["LOADED A", "loaded a", "A", "loaded a", "LOADED A", "ValueError: refused a"]


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
    # Every worker of a led shard is alive and ready: the leader leads, and
    # each follower has been heard by it.
    for worker in cluster.workers:
        assert (probe(worker.health, "/healthz"), probe(worker.health, "/readyz")) == (200, 200)
    delivered_to_its_submitter(cluster, leader)
    carried_as_blobs(cluster, leader)
    awaited_across_workers(cluster, leader)
    compacted(cluster, leader, tmp_path)
    slow_down_followed_across_workers(cluster, leader, tmp_path)
    task_process_killed(cluster, leader, tmp_path)
    cancelled_while_running(cluster, leader, tmp_path)
    worker_killed(cluster, leader)


def test_a_worker_that_has_not_joined_its_shard_is_alive_but_not_ready(tmp_path):
    """A worker whose only seed is silent keeps asking it (many seed rounds),
    so it never founds a shard of its own: its health answers 200 and its
    readiness 503 for as long as it waits."""
    health = f"127.0.0.1:{free_port()}"
    worker = Worker(
        {
            **TIMINGS,
            "KABUDACHI_LISTEN": "/ip4/127.0.0.1/tcp/0",
            "KABUDACHI_SEEDS": f"/ip4/127.0.0.1/tcp/{free_port()}",
            "KABUDACHI_SEED_ROUNDS": "1000",
            "KABUDACHI_HEALTH_LISTEN": health,
            "KABUDACHI_PROCESSES": "1",
            "KABUDACHI_CONCURRENCY": "1",
            "KABUDACHI_TEST_MARKERS": str(tmp_path),
        },
        wait_until_ready=False,
    )
    try:
        eventually("the health listener to answer", lambda: probe(health, "/healthz") == 200)
        # Three silent seed rounds with the default would have founded the
        # shard by now; a thousand keep it waiting.
        until = time.monotonic() + 3.0
        while time.monotonic() < until:
            assert (probe(health, "/healthz"), probe(health, "/readyz")) == (200, 503)
            time.sleep(0.1)
    finally:
        worker.close()


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

    # A body that fails: its run keeps the error's type and message (no
    # traceback), and when it was claimed and started.
    failing = follower.submit("fails", text="card declined")
    record = finished(cluster, failing)
    [run] = record["runs"]
    assert (run["state"], run["failure_kind"], run["failure_message"]) == (
        "FAILED",
        "ValueError",
        "card declined",
    )
    assert run["claimed_at"] is not None and run["started_at"] >= run["claimed_at"]


def carried_as_blobs(cluster, leader):
    # An input and a result each larger than one message travel as blobs: the
    # follower stores the input's chunks where the leader places the task's
    # record, the worker that runs it reads them back, and its result comes
    # back to the follower as chunks owned by the task.
    follower = next(worker for worker in cluster.workers if worker is not leader)
    big = ("0123456789" * (3 * 1024 * 1024 // 10 + 1))[: 3 * 1024 * 1024]
    # Three in a row: the in-flight share of a worker is 8,000,000 bytes, so
    # the submitter, which counts each input while it stores it, refuses the
    # third if the first two stayed counted. This guards the submitter's side
    # alone: the claimants of the three runs may be different workers, so a
    # claimant that leaves a run's input counted is not caught here.
    for _ in range(3):
        echoed = follower.submit("echo", text=big)
        certified_once(cluster, echoed, digest_of(echo, Greeting(text=big)))
        held = eventually("the follower to hold the echoed result", lambda: settled(follower, echoed))
        assert held["error"] is None, held


def awaited_across_workers(cluster, leader):
    # A body that awaits a task it called gets its result, wherever each ran,
    # and sees the call kept by the shard.
    awaiting = leader.submit("awaits", text="nested")
    record = certified_once(cluster, awaiting, digest_of(awaits, Greeting(text="REPLICATED")))
    assert [run["delivered"] for run in record["runs"]] == [True]
    assert eventually("the caller's handle to settle", lambda: settled(leader, awaiting))["error"] is None


def compacted(cluster, leader, tmp_path):
    # A coalescing key held by a running generation retains the payloads of
    # those superseded behind it, past the soft memory limit: the leader makes
    # a compaction run, a worker folds the chain in a task process, and the
    # newest generation's record keeps the fold. It then runs on the whole
    # chain, folded in order.
    holding = leader.submit("gather", text="a", times=GATED)
    # Running, so it holds the key: the generations after it wait behind it,
    # for as long as the test holds it, however long an idle worker takes to
    # claim the compaction run.
    eventually("the first generation to run", lambda: running_on(cluster, holding))
    parts = [letter * 200 for letter in "bcd"]
    newest = [leader.submit("gather", text=part) for part in parts][-1]
    eventually("the retained chain to be compacted", lambda: (cluster.record(newest) or {}).get("folded"))
    (tmp_path / "a.go").touch()

    # The compaction run's own record entry, if the record lists it, is not
    # the generation's result: the last run that succeeded is.
    record = finished(cluster, newest)
    succeeded = [run["digest"] for run in record["runs"] if run["state"] == "SUCCEEDED"]
    assert succeeded[-1] == digest_of(gather, Greeting(text="+".join(parts))), record


def slow_down_followed_across_workers(cluster, leader, tmp_path):
    follower = next(worker for worker in cluster.workers if worker is not leader)

    # A blob input held in flight past the soft limit raises the submitter's
    # own SlowDown, and its release clears it: the flag follows the payloads
    # the submitter holds, with no word from the leader.
    ballast = "ballast0" + "z" * (3 * 1024 * 1024)
    weighed = follower.submit("weigh", text=ballast)
    eventually("the submitter to hear SlowDown", lambda: follower.ask(slow_down=None)["slow_down"])
    (tmp_path / "ballast0.go").touch()
    eventually("the weighed task to finish", lambda: finished(cluster, weighed))
    eventually("the submitter's SlowDown to clear", lambda: not follower.ask(slow_down=None)["slow_down"])

    # A body maps over 300 items from its task process: the members go to the
    # leader in one batch, which passes the cluster's 300-byte soft limit, so
    # the worker that sent them hears SlowDown and polls until it clears; the
    # results come back in order, and no worker is left asking submitters to
    # wait.
    raised_before = [worker.ask(slow_down=None)["raises"] for worker in cluster.workers]
    mapped = follower.submit("maps", times=300)
    expected = Greeting(text=",".join(str(index) for index in range(300)))
    certified_once(cluster, mapped, digest_of(maps, expected))
    for worker in cluster.workers:
        eventually("SlowDown to clear", lambda: not worker.ask(slow_down=None)["slow_down"])
    # The cleared state above is no proof SlowDown was ever raised: a worker
    # heard it go up during the map, and the check above saw it come down.
    heard = [
        worker.ask(slow_down=None)["raises"] - before
        for worker, before in zip(cluster.workers, raised_before)
    ]
    assert sum(heard) >= 1, f"no worker heard SlowDown raised during the map: {heard}"


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


# The leader of a pipeline's cluster claims nothing, so killing it loses no
# run. Of the others, the one that claims the pipeline's body claims nothing
# besides it, so the stages it awaits run on the rest.
LEADER_RUNS_NOTHING = {0: {"KABUDACHI_TEST_CLAIMS_NOTHING": "1"}}
BODY_HOST_RUNS_ONLY_IT = {"KABUDACHI_TEST_CLAIMS_NOTHING_AFTER": "cluster.pipeline"}


def first_stage_started_on(cluster, markers):
    """The worker running the first `shout` stage, once it has started."""
    marker = next(markers.glob("shout.*"), None)
    if marker is None:
        return None
    return next(worker for worker in cluster.workers if worker.pid == int(marker.read_text()))


def committed_at_most_once(cluster, markers, definitions):
    """No task a pipeline made of the `definitions`, found by the claim markers
    the workers left, has more than one run that succeeded; returns how many
    have one. A stage whose run is lost, or whose result a shard with no leader
    could not store, is replayed, so a stage may run more than once; what must
    hold is that no result is committed twice."""
    claimed = {
        marker.read_text()
        for definition in definitions
        for marker in markers.glob(f"claimed.{definition}.*")
    }
    committed = 0
    for task_id in claimed:
        record = finished(cluster, task_id)
        succeeded = states(record).count("SUCCEEDED")
        assert succeeded <= 1, record
        committed += succeeded
    return committed


def running_a_stage_besides(cluster, markers, kept):
    """A worker not among `kept` that started a `shout` stage a moment ago, so
    is still in it: a stage lasts a second."""
    for marker in markers.glob("shout.*"):
        if time.time() - marker.stat().st_mtime > STAGE_SECONDS * 0.9:
            continue
        worker = next(worker for worker in cluster.workers if worker.pid == int(marker.read_text()))
        if worker not in kept:
            return worker
    return None


def test_a_stage_running_on_a_killed_worker_is_replayed_once_and_the_pipeline_ends_as_it_would_have(tmp_path):
    # The leader submits a body that awaits a flow, a group and a returned
    # step, each a composite it runs. A worker besides the body's, running a
    # stage, is killed mid-stage: the stage's run is lost and replayed once.
    cluster = Cluster.start(
        tmp_path, size=4, overrides={**LEADER_RUNS_NOTHING, **{n: BODY_HOST_RUNS_ONLY_IT for n in (1, 2, 3)}}
    )
    try:
        leader = cluster.workers[0]
        assert cluster.leader() is leader
        piped = leader.submit("pipeline", text="a")
        body = eventually("the body to run", lambda: running_on(cluster, piped))
        victim = eventually(
            "a stage to run on another worker",
            lambda: running_a_stage_besides(cluster, tmp_path, {leader, body}),
        )
        victim.kill()
        assert outcome(leader, piped) == PIPELINE_OF_A
        assert states(cluster.record(piped)) == ["SUCCEEDED"], "the body never ran twice"
        # Three stages, and at least the killed one's replay. No stage or
        # planning task commits a result twice.
        assert len(list(tmp_path.glob("shout.*"))) >= 4
        assert committed_at_most_once(cluster, tmp_path, ["shout", "plan", "load"]) >= 8
    finally:
        cluster.close()


def test_a_pipeline_whose_leader_is_killed_between_its_stages_ends_as_it_would_have_with_each_stage_run_at_least_once_and_committed_at_most_once(
    tmp_path,
):
    cluster = Cluster.start(tmp_path, overrides=LEADER_RUNS_NOTHING)
    try:
        leader, client, _ = cluster.workers
        assert cluster.leader() is leader
        piped = client.submit("pipeline", text="a")
        eventually("its first stage to start", lambda: first_stage_started_on(cluster, tmp_path))
        leader.kill()
        assert outcome(client, piped) == PIPELINE_OF_A
        # A slow election (a second term) can outlast the body's worker's abort
        # deadline, so the new leader replays the body.
        runs = states(cluster.record(piped))
        assert runs[-1] == "SUCCEEDED" and set(runs[:-1]) <= {"LOST"}, runs
        # At least once each: a stage in flight when the leader died may be
        # replayed. None commits a result twice.
        assert len(list(tmp_path.glob("shout.*"))) >= 3
        assert committed_at_most_once(cluster, tmp_path, ["shout", "plan", "load"]) >= 8
    finally:
        cluster.close()


def test_a_client_too_small_to_hold_a_result_has_its_handle_fail_while_the_run_completes_elsewhere(tmp_path):
    leader, client = 0, 1
    cluster = Cluster.start(
        tmp_path,
        overrides={
            # The client holds at most half its limit in flight, 1.5 MB, and
            # claims nothing, so every run is on a larger worker.
            client: {"KABUDACHI_MEMORY_HARD_LIMIT": "3000000", "KABUDACHI_TEST_CLAIMS_NOTHING": "1"},
        },
    )
    try:
        workers = cluster.workers
        assert cluster.leader() is workers[leader]
        # A 0.9 MB input travels inline and fits the client; its 1.8 MB
        # result does not.
        inflated = workers[client].submit("inflate", text="x" * 900_000)
        eventually("the client's handle to settle", lambda: settled(workers[client], inflated))
        assert settled(workers[client], inflated)["error"] == "ResultTooLargeError"
        # The result reaches the client before the leader certifies the run.
        [run] = finished(cluster, inflated)["runs"]
        assert run["state"] == "SUCCEEDED"
        assert run["worker"] != workers[client].id
    finally:
        cluster.close()


def test_a_run_whose_result_no_store_has_room_for_is_lost_at_its_bound_and_its_replay_stores_it_once_room_is_made(
    tmp_path,
):
    # Stores hold 6 MB each, inputs at most half of that. A held task's 2.5 MB
    # input, past one message so it travels as blobs placed on all three
    # workers, leaves 3.5 MB: a 4.5 MB result, under the payload limit,
    # cannot be stored.
    sized = {"KABUDACHI_MEMORY_HARD_LIMIT": "12000000"}
    cluster = Cluster.start(tmp_path, overrides={index: sized for index in range(3)})
    try:
        leader = cluster.leader()
        leader.submit("weigh", text="heavy001" + varied_text(2_500_000))
        eventually("the weighty task to run", lambda: (tmp_path / "heavy001.started").exists())

        bulked = leader.submit("bulk")
        # Refused until the run's reconnect timeout, the run is then lost
        # rather than left undelivered, and replayed.
        eventually("the bulk run to be lost", lambda: "LOST" in states(cluster.record(bulked)), timeout=15)
        assert not cluster.record(bulked)["finished"]

        (tmp_path / "heavy001.go").touch()
        record = certified_once(cluster, bulked, digest_of(bulk, Greeting(text=varied_text(BULK_BYTES))))
        assert states(record)[0] == "LOST" and states(record)[-1] == "SUCCEEDED"
        assert eventually("its handle", lambda: settled(leader, bulked))["error"] is None
    finally:
        cluster.close()


def test_a_worker_whose_run_is_cancelled_before_python_takes_it_offers_its_place_again(tmp_path):
    # Only the last worker claims, with two places and an in-flight share of
    # 6 MB: of two tasks queued together (the claimant is held still until both
    # are), whichever its first claim reaches first runs, and the other's input does not fit beside it, so that run
    # waits for room before Python can take it. The waiting run is cancelled,
    # and the place it held must be offered again: a third task runs on that
    # worker while the first still runs.
    roomy = {"KABUDACHI_MEMORY_HARD_LIMIT": "40000000"}
    claims_nothing = {**roomy, "KABUDACHI_TEST_CLAIMS_NOTHING": "1"}
    claimant = {"KABUDACHI_MEMORY_HARD_LIMIT": "12000000", "KABUDACHI_CONCURRENCY": "2"}
    cluster = Cluster.start(tmp_path, overrides={0: claims_nothing, 1: claims_nothing, 2: claimant})
    try:
        submitter = cluster.workers[0]
        with stopped(cluster.workers[2]):
            texts = {
                submitter.submit("weigh", text="weighA01" + varied_text(2_500_000)): "weighA01",
                submitter.submit("weigh", text="weighB01" + varied_text(3_800_000)): "weighB01",
            }
            for task in texts:
                eventually("the task to be queued", lambda task=task: cluster.record(task) is not None)
        [running] = eventually(
            "one of them to run",
            lambda: [task for task, key in texts.items() if (tmp_path / f"{key}.started").exists()],
            timeout=30,
        )
        [waiting] = [task for task in texts if task != running]
        eventually("the other to be claimed", lambda: states(cluster.record(waiting)) == ["CLAIMED"], timeout=30)
        assert submitter.ask(cancel=waiting)["cancelled"]
        eventually("its run to end", lambda: "CANCELLED" in states(cluster.record(waiting)))

        next_in_line = submitter.submit("hold", text="after001")
        certified_once(cluster, next_in_line, digest_of(hold, Greeting(text="after001")))
        assert not cluster.record(running)["finished"]
        (tmp_path / f"{texts[running]}.go").touch()
    finally:
        cluster.close()


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
    others = [task for task in held if task != cut_off]
    for task in others:
        leader.ask(cancel=task)
    # Decided before the other workers stop: a cancel in flight to a stopped
    # worker would be neither heard nor decided.
    for task in others:
        finished(cluster, task)
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
    # The undelivered one must run on the leader, which is then killed with
    # its buffered delivery: a surviving worker holding it would still
    # deliver the buffered result once the client resumes, and a matching
    # certified digest would settle the handle with that result instead of
    # leaving it unavailable. The leader is then killed while it and the other follower each run a
    # task, so the client, resumed, learns how its tasks ended only by
    # asking the leader the two survivors elect, under which every run
    # certifies once.
    leader = cluster.leader()
    client, other = [worker for worker in cluster.workers if worker is not leader]
    texts, holders, own, unreached, interrupted = one_on_each_worker(cluster, tmp_path, client, leader)
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


def one_on_each_worker(cluster, tmp_path, client, leader):
    """Three gated holds `client` submits, once one runs on each worker:
    each task's text, each text's worker, the task `client` runs itself, the
    one `leader` runs, and the one the other follower runs."""
    texts = {client.submit("hold", text=f"unreached-{n}", times=GATED): f"unreached-{n}" for n in range(3)}
    # Idle workers poll for work at growing intervals, up to 5 s, so a claim
    # can wait one.
    holders = eventually("all three to run", lambda: started_on(cluster, tmp_path, texts.values()), timeout=45)
    [own] = [task for task, text in texts.items() if holders[text] is client]
    [unreached] = [task for task, text in texts.items() if holders[text] is leader]
    [interrupted] = [task for task in texts if task not in (own, unreached)]
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


def test_a_stopped_leader_hands_over_and_leaves_and_the_two_left_keep_certifying(cluster):
    leader = cluster.leader()
    others = [worker for worker in cluster.workers if worker is not leader]

    assert leader.stop() == 0

    def handed_over():
        leaders = {worker.leader() for worker in others}
        if len(leaders) != 1 or None in leaders:
            return False
        ((new_leader, _),) = leaders
        # Two voters: it left through its successor rather than vanishing.
        return new_leader != leader.id and cluster.by_id(new_leader).voters() == 2

    eventually("the two left to follow a new leader of two voters", handed_over, timeout=10)
    task_id = others[0].submit("spin", times=0)
    certified_once(cluster, task_id, digest_of(spin, Greeting(text="spun")))
