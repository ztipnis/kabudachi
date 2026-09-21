"""Ctrl-C during `run()`: `main` is cancelled, everything is cleaned up, the
KeyboardInterrupt reaches the caller, and `run()` works again afterwards."""

import json
import select
import signal
import subprocess
import sys
import textwrap

STARTUP_SECONDS = 30

PROGRAM = textwrap.dedent(
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
        [sys.executable, "-c", PROGRAM, json.dumps(sys.path)],
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
