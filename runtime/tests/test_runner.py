"""`run()`'s lifecycle over a real native runtime: the native runtime is
shut down and the session ended however the run ends, and a worker that dies
is reported instead of waited on."""

import asyncio
import threading
import time
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


def test_run_says_so_when_the_native_extension_is_missing(monkeypatch):
    monkeypatch.setattr(runner_module, "_native", None)

    async def main():
        return None

    with pytest.raises(RuntimeError, match="native extension"):
        kabudachi.run(main)


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

