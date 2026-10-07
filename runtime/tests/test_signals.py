"""Signals during `kabudachi.run()`, each in a child process so a real signal
can be sent. With no `main` it serves as a worker until SIGINT or SIGTERM: the
first signal drains gracefully and a second gives up waiting. With a `main`,
Ctrl-C cancels it, everything is cleaned up, the KeyboardInterrupt reaches the
caller, and `run()` works again afterwards."""

import json
import select
import signal
import subprocess
import sys
import textwrap
import time

import pytest

SERVE_PROGRAM = textwrap.dedent(
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
        elif sys.argv[3] == "swallows_one_interrupt":
            swallowed = False
            while True:
                try:
                    time.sleep(float(request.text))
                    break
                except BaseException:
                    if swallowed:
                        raise
                    swallowed = True
                    print("swallowed", flush=True)
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
    print("handlers restored:", before == after and signal.set_wakeup_fd(-1) == -1, flush=True)
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
            [sys.executable, "-c", SERVE_PROGRAM, json.dumps(sys.path), str(task_seconds), kind],
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
    process = served(task_seconds=0.3)
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

    assert lines == ["forced", "handlers restored: True"], errors
    assert process.returncode == 0, errors


@pytest.mark.parametrize("kind", ["awaits", "blocks_the_loop"])
def test_two_mixed_signals_back_to_back_stop_waiting_for_running_tasks(served, kind):
    process = served(task_seconds=60, kind=kind)
    # Distinct signal numbers: two identical standard signals sent back to back
    # coalesce at the OS while the first is pending, so only one would arrive.
    process.send_signal(signal.SIGTERM)
    process.send_signal(signal.SIGINT)

    lines, errors = finish(process, timeout=30)

    assert lines == ["forced", "handlers restored: True"], errors
    assert process.returncode == 0, errors


def test_a_signal_after_a_swallowed_interrupt_still_stops_run(served):
    process = served(task_seconds=60, kind="swallows_one_interrupt")
    process.send_signal(signal.SIGTERM)
    time.sleep(0.5)
    process.send_signal(signal.SIGTERM)
    # The task body swallows the interrupt the second signal raised.
    ready, _, _ = select.select([process.stdout], [], [], 30)
    assert ready, "the second signal did not interrupt the task body"
    assert process.stdout.readline().strip() == "swallowed"
    # Re-sent signals are attributed exactly, so none can count as a real third
    # signal and interrupt the body a second time; the run must still be up.
    time.sleep(0.5)
    assert process.poll() is None, "a re-sent signal was taken for a real one"
    process.send_signal(signal.SIGTERM)

    lines, errors = finish(process, timeout=30)

    assert lines == ["forced", "handlers restored: True"], errors
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
    assert "handlers restored: True" in lines, errors
    assert process.returncode == 0, errors


SERVE_SLOW_START_PROGRAM = textwrap.dedent(
    """
    import json
    import signal
    import sys
    from types import SimpleNamespace

    sys.path[:0] = json.loads(sys.argv[1])

    import kabudachi
    import kabudachi.runner as runner
    from faulting_runtime import FaultingNative

    SUSPECT_TIMEOUT_MS = int(sys.argv[2])


    class SlowToLead(FaultingNative):
        # The real runtime, built to wait out a suspicion timeout before it
        # leads, so a signal sent once it exists lands before leadership.
        def __init__(self, *args, **options):
            super().__init__(*args, suspect_timeout_ms=SUSPECT_TIMEOUT_MS, **options)
            print("started", flush=True)

        async def wait_until_leader(self):
            await super().wait_until_leader()
            print("leading", flush=True)


    runner._native = SimpleNamespace(NativeRuntime=SlowToLead)
    before = [signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)]
    try:
        print("serving result:", kabudachi.run(), flush=True)
    except KeyboardInterrupt:
        print("forced", flush=True)
    after = [signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)]
    print("handlers restored:", before == after, flush=True)
    """
)

# Wide enough that the signal, sent as soon as the runtime exists, always
# lands before leadership, even on a loaded host.
SLOW_START_SUSPECT_TIMEOUT_MS = 500


def test_a_signal_before_leadership_stops_the_worker_once_it_is_up():
    process = subprocess.Popen(
        [sys.executable, "-c", SERVE_SLOW_START_PROGRAM, json.dumps(sys.path),
         str(SLOW_START_SUSPECT_TIMEOUT_MS)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        ready, _, _ = select.select([process.stdout], [], [], STARTUP_SECONDS)
        assert ready, "the runtime did not start in time"
        assert process.stdout.readline().strip() == "started"
        process.send_signal(signal.SIGTERM)

        lines, errors = finish(process)
    finally:
        if process.poll() is None:
            process.kill()

    assert lines == ["leading", "serving result: None", "handlers restored: True"], errors
    assert process.returncode == 0, errors


# --- Ctrl-C with a main ----------------------------------------------------

INTERRUPT_PROGRAM = textwrap.dedent(
    """
    import json
    import sys

    sys.path[:0] = json.loads(sys.argv[1])

    import asyncio

    import kabudachi


    async def waits_to_be_interrupted():
        print("ready", flush=True)
        await asyncio.sleep(60)


    async def finishes():
        return "second run ok"


    try:
        kabudachi.run(waits_to_be_interrupted)
    except KeyboardInterrupt:
        print("interrupted", flush=True)

    print(kabudachi.run(finishes), flush=True)
    """
)


def test_ctrl_c_interrupts_run_cleanly_and_run_works_again():
    process = subprocess.Popen(
        [sys.executable, "-c", INTERRUPT_PROGRAM, json.dumps(sys.path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        ready, _, _ = select.select([process.stdout], [], [], STARTUP_SECONDS)
        assert ready, "the program did not become ready in time"
        assert process.stdout.readline().strip() == "ready"
        process.send_signal(signal.SIGINT)
        output, errors = process.communicate(timeout=60)
    finally:
        if process.poll() is None:
            process.kill()

    assert output.split() == ["interrupted", "second", "run", "ok"], errors
    assert process.returncode == 0, errors
    for symptom in ("Task was destroyed", "GeneratorExit", "never awaited", "Traceback"):
        assert symptom not in errors, errors


INTERRUPT_BEFORE_LEADERSHIP_PROGRAM = textwrap.dedent(
    """
    import json
    import sys
    from types import SimpleNamespace

    sys.path[:0] = json.loads(sys.argv[1])

    import kabudachi
    import kabudachi.runner as runner
    from faulting_runtime import FaultingNative

    real_native = runner._native


    class SlowToLead(FaultingNative):
        def __init__(self, *args, **options):
            super().__init__(*args, suspect_timeout_ms=2000, **options)
            print("started", flush=True)


    async def never_runs():
        print("main ran", flush=True)


    async def finishes():
        return "second run ok"


    runner._native = SimpleNamespace(NativeRuntime=SlowToLead)
    try:
        kabudachi.run(never_runs)
    except KeyboardInterrupt:
        print("interrupted", flush=True)
    runner._native = real_native
    print(kabudachi.run(finishes), flush=True)
    """
)


def test_ctrl_c_before_leadership_interrupts_run_cleanly_and_run_works_again():
    process = subprocess.Popen(
        [sys.executable, "-c", INTERRUPT_BEFORE_LEADERSHIP_PROGRAM, json.dumps(sys.path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        ready, _, _ = select.select([process.stdout], [], [], STARTUP_SECONDS)
        assert ready, "the runtime did not start in time"
        assert process.stdout.readline().strip() == "started"
        process.send_signal(signal.SIGINT)
        output, errors = process.communicate(timeout=60)
    finally:
        if process.poll() is None:
            process.kill()

    assert output.split("\n")[:-1] == ["interrupted", "second run ok"], errors
    assert process.returncode == 0, errors
    for symptom in ("Task was destroyed", "GeneratorExit", "never awaited", "Traceback"):
        assert symptom not in errors, errors
