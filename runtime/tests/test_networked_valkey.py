"""One networked worker, cold, against a valkey server of the test's own: it
founds its shard through the authority (once the authority's TTL, cut to 3 s
by the URL's `ttl`, has passed, so that any worker registered before it would
show), runs a task that calls another, and when its program ends lets the run
it still holds finish before it exits."""

import os
import socket
import subprocess
from pathlib import Path

import pytest

from cluster import TIMINGS, Worker, certified_once, digest_of, eventually, settled
from networked_tasks import nests
from proto_messages import Greeting

PREFIX = "py-e2e:"
# The authority derives a call timeout of a tenth of this, 300 ms: ample for a
# server on loopback, and well below the third of the TTL it must stay under.
TTL_SECONDS = 3
SERVER_STARTS = 5


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def command(port: int, *words: str) -> bytes:
    """One RESP command's raw reply."""
    request = f"*{len(words)}\r\n" + "".join(f"${len(word)}\r\n{word}\r\n" for word in words)
    with socket.create_connection(("127.0.0.1", port), timeout=2) as connection:
        connection.sendall(request.encode())
        return connection.recv(65536)


def start_server(directory: Path) -> tuple[subprocess.Popen, int] | None:
    """A valkey server answering on a free port, or `None` if another process
    took the port between choosing it and the server binding it."""
    binary = Path(os.environ["TEST_SRCDIR"], os.environ["VALKEY_SERVER"])
    port = free_port()
    server = subprocess.Popen(
        [str(binary), "--port", str(port), "--bind", "127.0.0.1", "--dir", str(directory),
         "--save", "", "--appendonly", "no", "--protected-mode", "no"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )

    def answers():
        # Exited (the port was taken), or this very server answers.
        if server.poll() is not None:
            return "exited"
        try:
            return "up" if command(port, "PING").startswith(b"+PONG") else None
        except OSError:
            return None

    try:
        if eventually("valkey to answer", answers, timeout=10) == "up":
            return server, port
    except BaseException:
        server.kill()
        server.wait()
        raise
    server.wait()
    return None


@pytest.fixture
def valkey(tmp_path):
    directory = tmp_path / "valkey"
    directory.mkdir()
    for _ in range(SERVER_STARTS):
        if (started := start_server(directory)) is not None:
            break
    else:
        raise AssertionError(f"valkey found no free port in {SERVER_STARTS} tries")
    server, port = started
    try:
        yield port
    finally:
        server.kill()
        server.wait()


RESULTS_PREFIX = "py-results:"


def resumed(worker: Worker, task_id: str) -> dict:
    """What `kabudachi.resume(task_id)` gave the worker: `{"resumed": text}`
    (a list of texts for a composite's results) or
    `{"resumed_error": <its type's name>}`."""
    reply = worker.ask(resume=task_id)
    del reply["id"]
    return reply


def test_a_client_killed_while_awaiting_has_its_result_resumed_from_valkey_by_a_fresh_process(
    valkey, tmp_path
):
    """Two workers run tasks; a third only submits and awaits. It is
    killed while a task and a composite run. The task still certifies,
    with no one to deliver to, and its worker mirrors the result to
    valkey. Nobody mirrors the composite when it finishes: its client is
    dead. A process started afterwards resumes both: the task from valkey,
    the composite from the shard, which it then mirrors as the client
    would have. A task that keeps nothing leaves nothing to resume."""
    markers = tmp_path / "markers"
    markers.mkdir()
    environment = {
        **TIMINGS,
        "KABUDACHI_LISTEN": "/ip4/127.0.0.1/tcp/0",
        "KABUDACHI_PROCESSES": "1",
        "KABUDACHI_CONCURRENCY": "1",
        "KABUDACHI_TEST_MARKERS": str(markers),
        "KABUDACHI_RESULT_BACKEND": f"redis://127.0.0.1:{valkey}/0?key_prefix={RESULTS_PREFIX}",
    }
    # The client and the fresh process take no task of the default queue.
    clients = {"KABUDACHI_QUEUES": "elsewhere"}
    founder = Worker(environment)
    workers = [founder]
    try:
        seeded = {**environment, "KABUDACHI_SEEDS": founder.address}
        workers.append(Worker(seeded))
        client = Worker({**seeded, **clients})
        workers.append(client)
        eventually("three voters to follow one leader", lambda: founder.voters() == 3 and client.leader())

        quiet = client.submit("unkept", text="gone")
        eventually("the unkept task to settle", lambda: settled(client, quiet))
        kept = client.submit("hold", text="kept", times=1500)
        relayed = client.submit("relay", text="flowing", times=1500)
        eventually(
            "both held runs to start",
            lambda: any(markers.glob("kept.*")) and any(markers.glob("flowing.*")),
        )
        client.kill()

        def mirrored():
            runs = founder.record(kept)["runs"]
            return runs[-1] if runs and runs[-1]["persistence"] == "stored" else None

        run = eventually("the result to be certified and mirrored", mirrored, timeout=30)
        assert run["state"] == "SUCCEEDED"
        assert run["delivered"] is False, "the killed client could not take it"
        assert command(valkey, "EXISTS", f"{RESULTS_PREFIX}result:task/{kept}") == b":1\r\n"
        assert founder.record(quiet)["runs"][-1]["persistence"] == "disabled"

        fresh = Worker({**seeded, **clients})
        workers.append(fresh)
        eventually("the fresh process to follow the leader", fresh.leader)
        assert resumed(fresh, kept) == {"resumed": "kept"}
        assert resumed(fresh, quiet) == {"resumed_error": "ResultUnavailableError"}

        composites = f"{RESULTS_PREFIX}result:composite/*"
        assert command(valkey, "KEYS", composites) == b"*0\r\n", "the dead client mirrored nothing"
        assert resumed(fresh, relayed) == {"resumed": ["flowing", "flowing"]}
        eventually(
            "the resumer to mirror the composite",
            lambda: command(valkey, "KEYS", composites) != b"*0\r\n",
        )
    finally:
        for worker in workers:
            worker.close()


def test_a_worker_founds_its_shard_through_valkey_runs_a_task_that_calls_another_and_drains(
    valkey, tmp_path
):
    markers = tmp_path / "markers"
    markers.mkdir()
    worker = Worker({
        **TIMINGS,
        "KABUDACHI_LISTEN": "/ip4/127.0.0.1/tcp/0",
        "KABUDACHI_AUTHORITY": f"redis://127.0.0.1:{valkey}/0?key_prefix={PREFIX}&ttl={TTL_SECONDS}",
        "KABUDACHI_PROCESSES": "1",
        "KABUDACHI_TEST_MARKERS": str(markers),
    })
    try:
        assert command(valkey, "KEYS", f"{PREFIX}*") != b"*0\r\n", "the founding wrote nothing"

        task = worker.submit("nests", text="inner")
        certified_once(worker, task, digest_of(nests, Greeting(text="delivered")))
        assert eventually("the caller's handle to settle", lambda: settled(worker, task))["error"] is None

        # A run still held when the program ends finishes before the worker exits.
        worker.submit("hold", text="held", times=1000)
        eventually("the held run to start", lambda: any(markers.glob("held.*")))
        assert worker.stop() == 0
        assert not (markers / "held.stopped").exists(), "the held run was cancelled"
    finally:
        worker.close()
