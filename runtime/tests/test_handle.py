"""A task handle: awaitable, settled once from any thread, on the awaiting loop."""

import asyncio
import threading

import pytest

from kabudachi.handle import TaskHandle, current_body


def test_a_handle_knows_its_task_and_shard():
    handle = TaskHandle("task-1")

    assert handle.task_id == "task-1"
    assert handle.shard_id == "local"


def test_a_new_handle_is_not_done():
    assert TaskHandle("task-1").done() is False


def test_a_resolved_handle_gives_its_value():
    handle = TaskHandle("task-1")
    handle._resolve("the value")

    assert handle.done()
    assert asyncio.run(_await(handle)) == "the value"


def test_a_failed_handle_raises_its_error_when_awaited():
    handle = TaskHandle("task-1")
    handle._fail(ValueError("boom"))

    with pytest.raises(ValueError, match="boom"):
        asyncio.run(_await(handle))


def test_a_handle_can_be_awaited_more_than_once():
    handle = TaskHandle("task-1")
    handle._resolve(41)

    async def main():
        return await handle, await handle

    assert asyncio.run(main()) == (41, 41)


def test_a_handle_settled_after_it_is_awaited_wakes_the_waiter():
    handle = TaskHandle("task-1")

    async def main():
        waiting = asyncio.ensure_future(_await(handle))
        await asyncio.sleep(0.02)
        assert not waiting.done()
        handle._resolve("later")
        return await asyncio.wait_for(waiting, 5)

    assert asyncio.run(main()) == "later"


def test_a_handle_settled_from_another_thread_wakes_the_waiter():
    handle = TaskHandle("task-1")

    async def main():
        waiting = asyncio.ensure_future(_await(handle))
        await asyncio.sleep(0.02)
        threading.Thread(target=lambda: handle._resolve("from a thread")).start()
        return await asyncio.wait_for(waiting, 5)

    assert asyncio.run(main()) == "from a thread"


def test_only_the_first_settlement_counts():
    handle = TaskHandle("task-1")

    handle._resolve("first")
    handle._resolve("second")
    handle._fail(RuntimeError("late"))

    assert asyncio.run(_await(handle)) == "first"


def test_a_failure_first_is_not_replaced_by_a_value():
    handle = TaskHandle("task-1")

    handle._fail(RuntimeError("first"))
    handle._resolve("late")

    with pytest.raises(RuntimeError, match="first"):
        asyncio.run(_await(handle))


def test_a_handle_that_is_never_awaited_leaves_no_warning(recwarn):
    handle = TaskHandle("task-1")
    handle._fail(RuntimeError("nobody looked"))
    del handle

    assert [w for w in recwarn if "never retrieved" in str(w.message)] == []


async def _await(handle):
    return await handle


def test_one_awaiter_timing_out_does_not_settle_the_handle_for_anyone():
    handle = TaskHandle("task-1")

    async def main():
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(handle, 0.05)
        handle._resolve("certified value")
        return handle.done(), await handle

    assert asyncio.run(main()) == (True, "certified value")


def test_one_awaiter_being_cancelled_leaves_the_others_waiting():
    handle = TaskHandle("task-1")

    async def main():
        first = asyncio.ensure_future(_await(handle))
        second = asyncio.ensure_future(_await(handle))
        await asyncio.sleep(0.02)
        first.cancel()
        await asyncio.sleep(0.02)
        assert not handle.done()
        assert not second.done()
        handle._resolve("value")
        return await asyncio.wait_for(second, 5)

    assert asyncio.run(main()) == "value"


def test_a_cancelled_await_does_not_lose_the_result_for_a_later_await():
    handle = TaskHandle("task-1")

    async def main():
        waiting = asyncio.ensure_future(_await(handle))
        await asyncio.sleep(0.02)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        handle._resolve("kept")
        return await handle

    assert asyncio.run(main()) == "kept"


def test_a_body_that_waits_on_a_handle_is_told_when_it_starts_and_stops_waiting():
    events = []

    class Observer:
        def waiting_started(self):
            events.append("started")

        def waiting_finished(self):
            events.append("finished")

    handle = TaskHandle("task-1")

    async def main():
        current_body.set(Observer())
        waiting = asyncio.ensure_future(_await(handle))
        await asyncio.sleep(0.02)
        events.append("settling")
        handle._resolve(1)
        await waiting

    asyncio.run(main())

    assert events == ["started", "settling", "finished"]


def test_waiting_on_a_handle_that_is_already_settled_is_not_a_wait():
    events = []

    class Observer:
        def waiting_started(self):
            events.append("started")

        def waiting_finished(self):
            events.append("finished")

    handle = TaskHandle("task-1")
    handle._resolve(1)

    async def main():
        current_body.set(Observer())
        await handle

    asyncio.run(main())

    assert events == []


def test_a_handle_that_can_be_cancelled_asks_for_it_and_says_whether_it_took_effect():
    asked = []

    def canceller(task_id):
        asked.append(task_id)
        return True

    handle = TaskHandle("task-1", canceller)

    assert handle.cancel() is True
    assert asked == ["task-1"]


def test_cancelling_a_settled_handle_does_nothing_and_says_so():
    asked = []
    handle = TaskHandle("task-1", lambda task_id: asked.append(task_id) or True)
    handle._resolve("done")

    assert handle.cancel() is False
    assert asked == []


def test_a_handle_with_no_way_to_cancel_says_it_could_not():
    assert TaskHandle("task-1").cancel() is False


def test_a_callback_is_called_with_the_result_once_the_handle_resolves():
    seen = []
    handle = TaskHandle("task-1")
    handle.callback(seen.append)
    assert seen == []

    handle._resolve("the value")

    assert seen == ["the value"]


def test_a_callback_added_to_a_resolved_handle_is_called_at_once():
    seen = []
    handle = TaskHandle("task-1")
    handle._resolve("the value")

    handle.callback(seen.append)

    assert seen == ["the value"]


def test_callbacks_are_called_in_the_order_they_were_added_and_the_handle_is_returned():
    seen = []
    handle = TaskHandle("task-1")

    assert handle.callback(lambda v: seen.append(("a", v))).callback(
        lambda v: seen.append(("b", v))
    ) is handle
    handle._resolve(1)

    assert seen == [("a", 1), ("b", 1)]


def test_a_callback_is_not_called_for_a_task_that_failed():
    seen = []
    handle = TaskHandle("task-1")
    handle.callback(seen.append)

    handle._fail(ValueError("no result"))

    assert seen == []


def test_a_callback_that_raises_changes_nothing_and_does_not_stop_the_others(caplog):
    seen = []
    handle = TaskHandle("task-1")

    def broken(value):
        raise RuntimeError(f"secret {value}")

    handle.callback(broken).callback(seen.append)
    handle._resolve("fine")

    assert seen == ["fine"]
    assert asyncio.run(_await(handle)) == "fine"
    assert "secret" not in caplog.text
    assert "RuntimeError" in caplog.text


def test_a_callback_must_be_callable():
    with pytest.raises(TypeError):
        TaskHandle("task-1").callback("not callable")


def test_an_async_callback_added_to_a_bare_handle_runs_on_the_running_loop():
    seen = []

    async def callback(value):
        seen.append(value)

    async def scenario():
        handle = TaskHandle("task-1")
        handle.callback(callback)
        handle._resolve("the value")
        await asyncio.sleep(0.01)

    asyncio.run(scenario())

    assert seen == ["the value"]


def test_an_async_callback_on_a_bare_handle_settled_outside_a_loop_runs_to_completion(recwarn):
    seen = []

    async def callback(value):
        await asyncio.sleep(0)
        seen.append(value)

    handle = TaskHandle("task-1")
    handle.callback(callback)
    handle._resolve("the value")

    assert seen == ["the value"]
    assert not [w for w in recwarn if "never awaited" in str(w.message)]


def test_an_async_callback_that_raises_is_logged_by_type_and_changes_nothing(caplog):
    async def callback(value):
        raise KeyError(f"leaks {value}")

    handle = TaskHandle("task-1")
    handle.callback(callback)
    handle._resolve("secret")

    assert "secret" not in caplog.text
    assert "KeyError" in caplog.text
    assert handle.done()


def test_cancelling_a_flow_after_a_stage_that_failed_does_not_claim_to_have_cancelled_it():
    from kabudachi.composites import FlowHandle

    failed, succeeded = TaskHandle("stage-1"), TaskHandle("stage-2")
    failed._fail(ValueError("stage failed"))
    succeeded._resolve("done")

    flow_after_failure, flow_after_success = FlowHandle("flow-1"), FlowHandle("flow-2")
    flow_after_failure._current = (failed, False)
    flow_after_success._current = (succeeded, False)

    # The first flow is about to fail with the stage's own error; the second
    # has a stage still to start, which the cancel stops.
    assert flow_after_failure.cancel() is False
    assert flow_after_failure._cancel_requested is False
    assert flow_after_success.cancel() is True


def test_the_three_handles_are_exported_from_the_package_wherever_they_live():
    import kabudachi
    from kabudachi.composites import FlowHandle, GroupHandle

    assert (kabudachi.FlowHandle, kabudachi.GroupHandle) == (FlowHandle, GroupHandle)
    assert kabudachi.TaskHandle is TaskHandle
    assert {"FlowHandle", "GroupHandle", "TaskHandle"} <= set(kabudachi.__all__)
