"""One networked worker, cold, against a valkey server of the test's own: it
founds its shard through the authority (once the authority's 30 s TTL has
passed, so that any worker registered before it would show), runs a task that
calls another, and when its program ends lets the run it still holds finish
before it exits."""

import os
import socket
import subprocess
from pathlib import Path

import pytest

from cluster import TIMINGS, Worker, certified_once, digest_of, eventually
from networked_tasks import nests
from proto_messages import Greeting

PREFIX = "py-e2e:"
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


def test_a_worker_founds_its_shard_through_valkey_runs_a_task_that_calls_another_and_drains(
    valkey, tmp_path
):
    markers = tmp_path / "markers"
    markers.mkdir()
    worker = Worker({
        **TIMINGS,
        "KABUDACHI_LISTEN": "/ip4/127.0.0.1/tcp/0",
        "KABUDACHI_AUTHORITY": f"redis://127.0.0.1:{valkey}/0?key_prefix={PREFIX}",
        "KABUDACHI_PROCESSES": "1",
        "KABUDACHI_TEST_MARKERS": str(markers),
    })
    try:
        assert command(valkey, "KEYS", f"{PREFIX}*") != b"*0\r\n", "the founding wrote nothing"

        task = worker.submit("nests", text="inner")
        certified_once(worker, task, digest_of(nests, Greeting(text="RemoteResultUnavailableError")))

        def settled():
            worker.leader()  # an answer reads the lines printed before it
            return next((n for n in worker.notices if n.get("settled") == task), None)

        assert eventually("the caller's handle to settle", settled)["error"] == (
            "RemoteResultUnavailableError"
        )

        # A run still held when the program ends finishes before the worker exits.
        worker.submit("hold", text="held", times=1000)
        eventually("the held run to start", lambda: any(markers.glob("held.*")))
        assert worker.stop() == 0
        assert not (markers / "held.stopped").exists(), "the held run was cancelled"
    finally:
        worker.close()
