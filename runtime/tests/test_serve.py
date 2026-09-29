"""`kabudachi.run()` with no `main`: serves as a worker until SIGINT/SIGTERM.
The first signal drains gracefully, a second one gives up waiting."""

import json
import select
import signal
import subprocess
import sys
import textwrap
import time

import pytest

PROGRAM = textwrap.dedent(
    """
    import json
    import signal
    import sys
    import threading
    import time

    sys.path[:0] = json.loads(sys.argv[1])

    import asyncio

    import kabudachi
    from kabudachi import session
    from proto_messages import Greeting


    @kabudachi.task(name="serve.slow")
    async def slow(request: Greeting) -> Greeting:
        print("task started", flush=True)
        if sys.argv[3] == "blocks_the_loop":
            time.sleep(float(request.text))
        else:
            await asyncio.sleep(float(request.text))
        print("task done", flush=True)
        return request


    @kabudachi.task(name="serve.slow_sync")
    def slow_sync(request: Greeting) -> Greeting:
        print("task started", flush=True)
        time.sleep(float(request.text))
        print("task done", flush=True)
        return request


    def call_once_serving():
        while session.active_session() is None:
            time.sleep(0.01)
        if sys.argv[3] == "idle":
            print("task started", flush=True)  # the marker: serving, with nothing to run
        elif sys.argv[3] == "sync":
            slow_sync(Greeting(text=sys.argv[2]))
        else:
            slow(Greeting(text=sys.argv[2]))


    threading.Thread(target=call_once_serving, daemon=True).start()
    before = [signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)]
    try:
        print("serving result:", kabudachi.run(), flush=True)
    except KeyboardInterrupt:
        print("forced", flush=True)
    after = [signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)]
    print("handlers restored:", before == after, flush=True)
    """
)


STARTUP_SECONDS = 30


@pytest.fixture
def served():
    """Starts the program and gives back a function that waits for its task to
    start; the process is always killed and reaped afterwards."""
    processes = []

    def start(task_seconds, kind="awaits"):
        process = subprocess.Popen(
            [sys.executable, "-c", PROGRAM, json.dumps(sys.path), str(task_seconds), kind],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        processes.append(process)
        ready, _, _ = select.select([process.stdout], [], [], STARTUP_SECONDS)
        assert ready, "the task did not start in time"
        assert process.stdout.readline().strip() == "task started"
        return process

    yield start
    for process in processes:
        process.kill()
        process.communicate()


def finish(process, timeout=60):
    output, errors = process.communicate(timeout=timeout)
    for symptom in ("Task was destroyed", "GeneratorExit", "never awaited", "Traceback"):
        assert symptom not in errors, errors
    return output.split("\n")[:-1], errors


@pytest.mark.parametrize("stop", [signal.SIGTERM, signal.SIGINT])
def test_the_first_signal_lets_running_tasks_finish_then_returns(served, stop):
    process = served(task_seconds=1)
    process.send_signal(stop)

    lines, errors = finish(process)

    assert lines == ["task done", "serving result: None", "handlers restored: True"], errors
    assert process.returncode == 0, errors


@pytest.mark.parametrize("kind", ["awaits", "blocks_the_loop"])
def test_a_second_signal_stops_waiting_for_running_tasks(served, kind):
    process = served(task_seconds=60, kind=kind)
    process.send_signal(signal.SIGTERM)
    time.sleep(0.5)
    assert process.poll() is None, "the first signal must wait for the running task"
    process.send_signal(signal.SIGTERM)

    lines, errors = finish(process, timeout=30)

    assert lines[0] == "forced", errors
    assert "task done" not in lines, errors
    assert process.returncode == 0, errors


def test_a_serve_with_nothing_to_run_ends_cleanly_on_the_first_signal(served):
    process = served(task_seconds=0, kind="idle")
    process.send_signal(signal.SIGTERM)

    lines, errors = finish(process)

    assert lines == ["serving result: None", "handlers restored: True"], errors
    assert process.returncode == 0, errors


def test_a_second_signal_while_a_synchronous_task_runs_raises_at_once_and_the_thread_finishes_alone(
    served,
):
    process = served(task_seconds=1, kind="sync")
    process.send_signal(signal.SIGTERM)
    time.sleep(0.5)
    assert process.poll() is None, "the first signal must wait for the running task"
    process.send_signal(signal.SIGTERM)

    lines, errors = finish(process, timeout=30)

    # `run()` gave up waiting, but the interpreter still waits for the thread.
    assert lines[0] == "forced", errors
    assert process.returncode == 0, errors
