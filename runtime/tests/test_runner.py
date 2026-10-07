"""`run()`'s lifecycle over a real native runtime: the native runtime is
shut down and the session ended however the run ends, and a worker that dies
is reported instead of waited on."""

import asyncio
import os
import signal
import socket
import sys
import threading
import time
import traceback
from types import SimpleNamespace

import pytest

import kabudachi
from faulting_runtime import FaultingNative
from kabudachi import config as config_module
from kabudachi import registry as registry_module
from kabudachi import runner as runner_module
from kabudachi import session as session_module
from kabudachi.config import Configuration
from kabudachi.errors import RunStoppedError
from kabudachi.registry import TaskRegistry
from proto_messages import Greeting


@pytest.fixture(autouse=True)
def faulting_native(monkeypatch):
    FaultingNative.instances = []
    monkeypatch.setattr(registry_module, "_default_registry", TaskRegistry())
    monkeypatch.setattr(config_module, "_process_configuration", Configuration())
    monkeypatch.setattr(runner_module, "_native", SimpleNamespace(NativeRuntime=FaultingNative))
    return FaultingNative


def only_native():
    [native] = FaultingNative.instances
    return native


def test_a_run_that_completes_shuts_the_native_runtime_down_and_ends_the_session():
    async def main():
        return "done"

    assert kabudachi.run(main) == "done"

    assert only_native().shutdowns == 1
    assert session_module.active_session() is None


def test_a_run_that_completes_refuses_late_submissions_to_its_session():
    @kabudachi.task(name="tests.echo")
    def echo(request: Greeting) -> Greeting:
        return request

    async def main():
        return session_module.active_session()

    session = kabudachi.run(main)

    with pytest.raises(RunStoppedError):
        session.submit(echo.definition, Greeting())


def test_a_main_that_raises_still_shuts_down_and_ends_the_session():
    async def main():
        raise KeyError("main failed")

    with pytest.raises(KeyError, match="main failed"):
        kabudachi.run(main)

    assert only_native().shutdowns == 1
    assert session_module.active_session() is None


def test_a_main_that_is_cancelled_still_shuts_down_and_ends_the_session():
    async def main():
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        kabudachi.run(main)

    assert only_native().shutdowns == 1
    assert session_module.active_session() is None


def test_a_runtime_that_never_becomes_leader_is_shut_down_and_the_error_raised(monkeypatch):
    original_init = FaultingNative.__init__

    def failing_init(self, *arguments, **options):
        original_init(self, *arguments, **options)
        self.leader_error = RuntimeError("no leader")

    monkeypatch.setattr(FaultingNative, "__init__", failing_init)

    async def main():
        raise AssertionError("main must not start without a leader")

    with pytest.raises(RuntimeError, match="no leader"):
        kabudachi.run(main)

    assert only_native().shutdowns == 1
    assert session_module.active_session() is None


def test_a_worker_that_dies_cancels_main_and_raises_its_error(monkeypatch):
    original_init = FaultingNative.__init__

    def failing_init(self, *arguments, **options):
        original_init(self, *arguments, **options)
        self.claim_error = RuntimeError("the runtime broke")

    monkeypatch.setattr(FaultingNative, "__init__", failing_init)
    cancelled = []

    async def main():
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="the runtime broke"):
        kabudachi.run(main)

    assert time.monotonic() - started < 5
    assert cancelled == [True]
    assert only_native().shutdowns == 1
    assert session_module.active_session() is None


def test_a_worker_that_dies_while_run_waits_for_tasks_is_reported():
    @kabudachi.task(name="tests.slow")
    async def slow(request: Greeting) -> Greeting:
        await asyncio.sleep(0.2)
        return request

    async def main():
        slow(Greeting())
        native = only_native()
        native.claim_error = RuntimeError("the runtime broke later")

    with pytest.raises(RuntimeError, match="the runtime broke later"):
        kabudachi.run(main)

    assert only_native().shutdowns == 1


def test_serving_without_main_needs_the_main_thread():
    errors = []

    def serve():
        try:
            kabudachi.run()
        except RuntimeError as error:
            errors.append(str(error))

    thread = threading.Thread(target=serve)
    thread.start()
    thread.join()

    assert len(errors) == 1 and "main thread" in errors[0]


def test_the_configured_result_ttl_is_given_to_the_native_runtime_in_milliseconds():
    kabudachi.configure(result_ttl=5)

    async def main():
        return None

    kabudachi.run(main)

    assert only_native().options["result_ttl_ms"] == 5000


def test_a_worker_that_ended_is_reported_even_if_the_work_finished_at_the_same_moment():
    async def scenario():
        async def dies():
            raise RuntimeError("worker died")

        async def finishes():
            return "a value that must not hide the failure"

        worker = asyncio.ensure_future(dies())
        with pytest.raises(RuntimeError, match="worker died"):
            await runner_module._until_done_or_worker_stops(finishes(), worker)

    asyncio.run(scenario())


@pytest.fixture
def main_thread_loop():
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signal handlers need the main thread")
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# Private seam: CPython's reentrant handler call cannot be timed through `run()`.
def test_a_signal_handler_running_inside_a_handler_never_raises(main_thread_loop, monkeypatch):
    signals = runner_module._StopSignals(main_thread_loop)
    signals._arrived = 2
    nested = []

    def interrupted_while_checking(frame):
        # CPython can run the handler again at any eval-breaker check, here
        # included, and a raise from there would skip restoring the handlers.
        # Nest once, so an unguarded handler fails by raising, not by recursing.
        if not nested:
            nested.append("entered")
            try:
                signals._handle(signal.SIGINT, sys._getframe())
            except KeyboardInterrupt:
                nested.append("raised")
        return False

    with monkeypatch.context() as patch:
        patch.setattr(runner_module, "_is_stopping", interrupted_while_checking)
        with pytest.raises(KeyboardInterrupt):
            signals._handle(signal.SIGINT, sys._getframe())

    assert nested == ["entered"]
    # The guard is released, so a later signal is handled again.
    signals._discard()
    with pytest.raises(KeyboardInterrupt):
        signals._handle(signal.SIGINT, sys._getframe())


class _DiscardsAnInterrupt:
    def __del__(self):
        raise KeyboardInterrupt


# Private seam: a raise discarded by CPython while the run is serving cannot be
# produced on demand through `run()`.
def test_an_interrupt_discarded_by_python_is_delivered_again(main_thread_loop, monkeypatch):
    reported = []

    def report(unraisable):
        reported.append(unraisable.exc_type)

    monkeypatch.setattr(sys, "unraisablehook", report)
    signals = runner_module._StopSignals(main_thread_loop)
    signals._arrived = 2
    signals._raised()

    with signals:
        with pytest.raises(KeyboardInterrupt):
            # Raised in a finalizer, so CPython prints it as ignored and it
            # never propagates; the run must not stay quiet for good.
            _DiscardsAnInterrupt()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                time.sleep(0.01)

    assert reported == [KeyboardInterrupt]
    assert sys.unraisablehook is report


# Private seam: an unwind that outlasts any timer cannot be staged through `run()`.
# No timer re-arms the interrupt any more; this guards against any time-based
# re-arm coming back, so it only needs to outlast a few watcher polls.
def test_a_slow_unwind_is_not_interrupted_again_while_it_has_not_been_discarded(main_thread_loop):
    signals = runner_module._StopSignals(main_thread_loop)
    signals._arrived = 2
    signals._raised()

    with signals:
        # A raise that is still unwinding in a slow `finally` has not been
        # discarded, and a second raise would cut the cleanup short.
        try:
            time.sleep(0.5)
        except KeyboardInterrupt:
            pytest.fail("the interrupt was raised again without a discard")


@pytest.fixture
def wakeup_sockets():
    arrivals, wakeup = socket.socketpair()
    arrivals.settimeout(0.05)
    wakeup.setblocking(False)
    yield arrivals, wakeup
    arrivals.close()
    wakeup.close()


class _LockThatLetsTheMainThreadInFirst:
    """A lock whose first acquire first runs `interleave`, as the main thread
    would between the watcher's read of the state and its clear."""

    def __init__(self, lock, interleave):
        self._lock = lock
        self._interleave = interleave

    def __enter__(self):
        interleave, self._interleave = self._interleave, None
        if interleave is not None:
            interleave()
        return self._lock.__enter__()

    def __exit__(self, *exc_info):
        return self._lock.__exit__(*exc_info)


# Private seam: the main thread acting between the watcher's read and its clear
# cannot be timed through `run()`.
@pytest.mark.parametrize("raises_again", [True, False])
def test_the_watcher_clears_only_the_raise_whose_mark_it_read(main_thread_loop, raises_again):
    signals = runner_module._StopSignals(main_thread_loop)
    kicks = []
    signals._kick = lambda: kicks.append("re-sent")

    def discards_and_maybe_raises_again():
        signals._discard()
        if raises_again:
            signals._raised()

    signals._lock = _LockThatLetsTheMainThreadInFirst(
        signals._lock, discards_and_maybe_raises_again
    )
    signals._state = 0
    signals._arrived = 2

    signals._watch_pass(b"")

    # A raise made after the read must stay raised, so the watcher does not
    # re-send into its unwind; a discard alone is re-armed at once.
    assert signals._is_raised() == raises_again
    assert kicks == ([] if raises_again else ["re-sent"])


# Private seam: the main thread acting between the watcher's read of the raised
# state and its lock cannot be timed through `run()`.
def test_a_discard_between_the_watcher_read_and_its_lock_is_re_armed(
    main_thread_loop, wakeup_sockets
):
    signals = runner_module._StopSignals(main_thread_loop)
    signals._arrivals, signals._wakeup = wakeup_sockets
    kicks = []
    signals._kick = lambda: kicks.append("re-sent")
    signals._lock = _LockThatLetsTheMainThreadInFirst(signals._lock, signals._discard)
    signals._state = runner_module._RAISED
    signals._arrived = 2

    signals._watch_pass(b"")

    # No mark for a raise that is gone, so the interrupt is delivered again.
    assert not signals._is_raised()
    assert kicks == ["re-sent"]


# Private seam: as above, with a real press that raises again.
def test_a_press_counted_for_a_newer_raise_does_not_clear_it(main_thread_loop, wakeup_sockets):
    signals = runner_module._StopSignals(main_thread_loop)
    kicks = []
    signals._kick = lambda: kicks.append("re-sent")
    arrivals, wakeup = wakeup_sockets
    signals._arrivals, signals._wakeup = arrivals, wakeup

    def discards_and_a_new_press_raises_again():
        signals._discard()
        wakeup.send(bytes([signal.SIGINT]))
        signals._raised()

    signals._lock = _LockThatLetsTheMainThreadInFirst(
        signals._lock, discards_and_a_new_press_raises_again
    )
    signals._state = runner_module._RAISED
    signals._arrived = 2

    signals._watch_pass(b"")
    # The next poll reads whatever is left, as the watcher does; a press it
    # has already counted must not be taken as arriving after the raise.
    try:
        data = arrivals.recv(4096)
    except TimeoutError:
        data = b""
    signals._watch_pass(data)

    assert signals._is_raised()
    assert kicks == []


# Private seam: an unraisable report from another thread cannot be aimed at a
# serving run through `run()`.
def test_an_interrupt_discarded_on_a_worker_thread_does_not_re_arm(main_thread_loop):
    signals = runner_module._StopSignals(main_thread_loop)
    signals._previous_unraisable = lambda unraisable: None
    signals._raised()

    thread = threading.Thread(
        target=signals._unraisable, args=(SimpleNamespace(exc_type=KeyboardInterrupt),)
    )
    thread.start()
    thread.join()

    # Signal-raised interrupts only land on the main thread, so this discard
    # is not one the outstanding raise could have met.
    assert signals._is_raised()


# Private seam: a failing signal send cannot be staged through `run()`.
def test_a_failed_re_send_leaves_none_outstanding_so_the_watcher_can_send_again(
    main_thread_loop, monkeypatch
):
    def failing_send(*arguments):
        raise OSError("no such thread")

    monkeypatch.setattr(signal, "pthread_kill", failing_send, raising=False)
    monkeypatch.setattr(runner_module._thread, "interrupt_main", failing_send)
    signals = runner_module._StopSignals(main_thread_loop)

    with pytest.raises(OSError):
        signals._kick()

    assert signals._resent and not signals._outstanding


# Private seam: a watcher pass that keeps failing cannot be staged through `run()`.
def test_a_watcher_whose_error_report_fails_keeps_running_and_reports_only_once(
    main_thread_loop, monkeypatch
):
    class AlwaysFailing(runner_module._StopSignals):
        passes = 0
        enough = threading.Event()

        def _watch_pass(self, data):
            self.passes += 1
            if self.passes >= 3:
                self.enough.set()
            raise RuntimeError("pass failed")

    errors = []
    monkeypatch.setattr(threading, "excepthook", lambda args: errors.append(args.exc_value))
    reports = []

    def broken_stderr():
        reports.append(sys.exc_info()[1])
        raise OSError("stderr closed")

    monkeypatch.setattr(traceback, "print_exc", broken_stderr)
    signals = AlwaysFailing(main_thread_loop)

    with signals:
        assert signals.enough.wait(5)

    assert errors == []
    assert len(reports) == 1


# Private seam: a foreign hook that raises cannot be staged through `run()`.
def test_a_previous_hook_that_raises_still_leaves_the_interrupt_armed(main_thread_loop, monkeypatch):
    seen = []

    def raising_hook(unraisable):
        # The watcher re-sends while the interrupt is raised, so it must
        # still be marked raised while a foreign hook runs.
        seen.append(signals._is_raised())
        raise RuntimeError("foreign hook")

    monkeypatch.setattr(sys, "unraisablehook", raising_hook)
    signals = runner_module._StopSignals(main_thread_loop)
    signals._raised()

    with signals:
        with pytest.raises(RuntimeError, match="foreign hook"):
            signals._unraisable(SimpleNamespace(exc_type=KeyboardInterrupt))

    assert seen == [True]
    assert not signals._is_raised()


# Private seam: a stale hook left installed after restore cannot be staged through `run()`.
def test_a_hook_left_installed_after_the_run_still_chains_and_does_not_re_arm(
    main_thread_loop, monkeypatch
):
    reported = []
    monkeypatch.setattr(sys, "unraisablehook", reported.append)
    signals = runner_module._StopSignals(main_thread_loop)
    with signals:
        pass
    signals._raised()

    unraisable = SimpleNamespace(exc_type=KeyboardInterrupt)
    signals._unraisable(unraisable)

    assert reported == [unraisable]
    assert signals._is_raised()


def test_serving_refuses_to_start_when_a_handler_cannot_be_put_back(main_thread_loop, monkeypatch):
    before = signal.set_wakeup_fd(-1)
    try:
        monkeypatch.setattr(signal, "getsignal", lambda number: None)
        # A regression would serve until a signal, so fail fast instead of hanging.
        watchdog = threading.Timer(10, os.kill, (os.getpid(), signal.SIGTERM))
        watchdog.start()
        try:
            with pytest.raises(RuntimeError, match="handler"):
                kabudachi.run()
        finally:
            watchdog.cancel()
        monkeypatch.undo()
    finally:
        assert signal.set_wakeup_fd(before) == -1


# Private seam: `run()` cannot make one restore step fail on demand.
def test_every_restore_step_runs_even_if_one_fails(main_thread_loop, monkeypatch):
    original = signal.signal
    original_int = signal.getsignal(signal.SIGINT)
    original_term = signal.getsignal(signal.SIGTERM)

    def failing_for_sigterm(number, handler):
        if number == signal.SIGTERM and handler is original_term:
            raise TypeError("embedded interpreter")
        return original(number, handler)

    signals = runner_module._StopSignals(main_thread_loop)
    signals.__enter__()
    monkeypatch.setattr(signal, "signal", failing_for_sigterm)
    try:
        with pytest.raises(TypeError):
            signals.__exit__(None, None, None)
        assert signal.getsignal(signal.SIGINT) is original_int
        assert signal.set_wakeup_fd(-1) == -1
    finally:
        monkeypatch.undo()
        original(signal.SIGTERM, original_term)
