"""What one task is going through in this process, as a plain state holder:
the transition rules, with no event loop and no runtime."""

import pytest

from kabudachi.lifecycle import RetryOutcome, TaskLifecycle


def new(**fields):
    return TaskLifecycle(object(), "billing.charge", **fields)


def test_a_new_task_is_unstarted_and_open():
    lifecycle = new()

    assert not lifecycle.started
    assert not lifecycle.settled
    assert not lifecycle.active
    assert lifecycle.outcome_counts
    assert not lifecycle.retirable


def test_claiming_an_open_task_marks_it_started():
    lifecycle = new()

    assert lifecycle.claim() is True
    assert lifecycle.started


def test_a_settled_task_cannot_be_claimed_to_run():
    lifecycle = new()
    lifecycle.settle()

    assert lifecycle.claim() is False
    assert not lifecycle.started


def test_a_task_settles_once():
    lifecycle = new()

    assert lifecycle.settle() is True
    assert lifecycle.settle() is False


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({}, True),
        ({"started": True}, False),
        ({"settled": True}, False),
    ],
)
def test_stopping_fails_only_the_open_task_that_has_not_started(fields, expected):
    lifecycle = new(**fields)

    assert lifecycle.stop_unstarted() is expected
    assert lifecycle.settled is (expected or fields.get("settled", False))


def test_a_task_waiting_for_its_retry_is_stopped_like_an_unstarted_one():
    lifecycle = new(started=True, provisional_result=b"result")
    assert lifecycle.retry_queued(stopping=False) is RetryOutcome.WAIT

    assert lifecycle.stop_unstarted() is True


def test_a_queued_retry_forgets_the_last_attempts_result_and_is_unstarted_again():
    lifecycle = new(started=True, provisional_result=b"result")

    assert lifecycle.retry_queued(stopping=False) is RetryOutcome.WAIT
    assert not lifecycle.started
    assert lifecycle.provisional_result is None
    assert not lifecycle.settled


def test_a_retry_queued_while_the_run_is_stopping_settles_the_task():
    lifecycle = new(started=True)

    assert lifecycle.retry_queued(stopping=True) is RetryOutcome.STOPPED
    assert lifecycle.settled


def test_a_retry_queued_for_a_settled_task_changes_nothing():
    lifecycle = new(started=True, settled=True, provisional_result=b"result")

    assert lifecycle.retry_queued(stopping=False) is RetryOutcome.SETTLED
    assert lifecycle.started
    assert lifecycle.provisional_result == b"result"


def test_a_result_is_held_only_while_the_task_is_open():
    open_task = new()
    open_task.hold(b"result")
    settled_task = new(settled=True)
    settled_task.hold(b"result")

    assert open_task.provisional_result == b"result"
    assert settled_task.provisional_result is None


def test_the_leaders_cancel_notice_suppresses_a_run_in_progress():
    lifecycle = new()
    lifecycle.begin_run()

    lifecycle.cancelled_by_leader()

    assert lifecycle.cancelled
    assert not lifecycle.outcome_counts


def test_the_leaders_cancel_notice_does_nothing_when_no_run_is_in_progress():
    lifecycle = new()

    lifecycle.cancelled_by_leader()

    assert not lifecycle.cancelled
    assert lifecycle.outcome_counts


def test_a_cancel_this_process_asked_for_suppresses_the_outcome_before_the_notice_arrives():
    lifecycle = new()
    lifecycle.begin_run()

    lifecycle.request_cancel()

    assert lifecycle.cancel_requested
    assert not lifecycle.cancelled  # only the leader's own notice says the body was cancelled
    assert not lifecycle.outcome_counts


def test_ending_a_run_clears_what_belongs_to_the_run_only():
    body = object()
    abandoned = object()
    lifecycle = new(
        body=body,
        abandoned=abandoned,
        settled=True,
        started=True,
        provisional_result=b"held",
    )
    generation = lifecycle.begin_run()
    lifecycle.request_cancel()
    lifecycle.cancelled_by_leader()

    lifecycle.end_run(generation)

    assert not lifecycle.active
    assert not lifecycle.cancelled
    assert lifecycle.body is None
    # What outlives a run stays.
    assert lifecycle.settled
    assert lifecycle.started
    assert lifecycle.cancel_requested
    assert lifecycle.provisional_result == b"held"
    assert lifecycle.abandoned is abandoned
    # So a cancel this process asked for still suppresses what a later run ends with.
    assert not lifecycle.outcome_counts


def test_an_abandoned_runs_end_run_cannot_undo_a_newer_run_that_already_began():
    # The abandoned-body retry path: an old run's body outlives its hard
    # limit, a retry begins a new run while the old one is still finishing,
    # and both end up awaiting the same future. The old run's own `end_run`
    # must not clear state the new run already set.
    lifecycle = new()
    stale = lifecycle.begin_run()
    new_body = object()

    current = lifecycle.begin_run()  # the retry's own run begins first
    lifecycle.body = new_body

    lifecycle.end_run(stale)  # the abandoned run's `end_run` arrives late

    assert lifecycle.active
    assert lifecycle.body is new_body
    # The current run's own end_run still works normally.
    lifecycle.end_run(current)
    assert not lifecycle.active
    assert lifecycle.body is None


@pytest.mark.parametrize(
    ("fields", "retirable"),
    [
        ({}, False),
        ({"settled": True}, True),
        ({"settled": True, "active": True}, False),
        ({"settled": True, "continuation": object()}, False),
        ({"active": True}, False),
    ],
)
def test_a_task_can_be_forgotten_once_it_is_settled_and_nothing_of_it_is_still_going(
    fields, retirable
):
    assert new(**fields).retirable is retirable


def test_a_detached_lifecycle_is_settled_and_has_no_handle():
    lifecycle = TaskLifecycle.detached("billing.charge")

    assert lifecycle.settled
    assert lifecycle.handle is None
    assert lifecycle.definition_name == "billing.charge"
    assert lifecycle.claim() is False
