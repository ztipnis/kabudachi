//! Cancelling a task in whatever state its current run is in. The leader
//! decides: the run becomes `Cancelled` at once, so nothing the worker still
//! reports about it can count, and the worker is told to stop the body.

use crate::support::scheduler::{ticks, Fixture};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    CancelRejection, Cancellation, ClaimRejection, Completion, Event, ReportRejection, Submission,
};

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn plain() -> Submission {
    Submission::new(
        TaskDefinitionId::new("billing.charge"),
        0,
        b"in".to_vec(),
        "default",
    )
}

fn cancelled_events(fixture: &mut Fixture) -> Vec<(TaskId, bool)> {
    fixture
        .scheduler
        .take_events()
        .into_iter()
        .filter_map(|event| match event {
            Event::Cancelled {
                task_id,
                was_running,
                ..
            } => Some((task_id, was_running)),
            _ => None,
        })
        .collect()
}

#[test]
fn a_queued_task_is_cancelled_and_never_handed_out() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain()).unwrap();

    let outcome = fixture.scheduler.cancel(&task).unwrap();

    assert_eq!(outcome, Cancellation::Cancelled { was_running: false });
    assert_eq!(fixture.state(&task), TaskRunState::Cancelled);
    assert!(fixture.spy.pending() == 0);
    assert_eq!(cancelled_events(&mut fixture), vec![(task.clone(), false)]);
    assert_eq!(
        fixture
            .scheduler
            .request_claim(&worker(), &task)
            .unwrap_err(),
        ClaimRejection::Finished
    );
    assert!(fixture
        .scheduler
        .claim_oldest(&worker(), 10)
        .unwrap()
        .is_empty());
}

#[test]
fn a_scheduled_task_is_cancelled_and_never_becomes_queued() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)).with_expiry(ticks(50)))
        .unwrap();

    fixture.scheduler.cancel(&task).unwrap();
    // Nothing waits on time any more, before the clock could clear a leftover.
    assert_eq!(fixture.scheduler.next_deadline(), None);
    fixture.clock.advance(ticks(500));
    let advanced = fixture.scheduler.catch_up();

    assert_eq!(advanced.queued, 0);
    assert_eq!(fixture.state(&task), TaskRunState::Cancelled);
    assert_eq!(fixture.scheduler.next_deadline(), None);
}

#[test]
fn a_claimed_task_is_cancelled_and_its_worker_can_no_longer_start_it() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain()).unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();

    let outcome = fixture.scheduler.cancel(&task).unwrap();

    assert_eq!(outcome, Cancellation::Cancelled { was_running: true });
    assert_eq!(fixture.state(&task), TaskRunState::Cancelled);
    assert_eq!(
        fixture
            .scheduler
            .report_started(&worker(), &claim.task_run_id)
            .unwrap_err(),
        ReportRejection::NotAuthoritative
    );
}

#[test]
fn a_running_task_is_cancelled_and_nothing_it_reports_afterwards_counts() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain().with_retries(3)).unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();

    let outcome = fixture.scheduler.cancel(&task).unwrap();

    assert_eq!(outcome, Cancellation::Cancelled { was_running: true });
    assert_eq!(cancelled_events(&mut fixture), vec![(task.clone(), true)]);
    let mark = fixture.spy.mark();
    let completed = fixture.scheduler.complete(
        &worker(),
        &claim.task_run_id,
        Digest::blake3(b"late"),
        Completion::Final,
    );
    let failed = fixture
        .scheduler
        .fail(&worker(), &claim.task_run_id, "ValueError");
    assert_eq!(completed.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(failed.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(fixture.state(&task), TaskRunState::Cancelled);
    assert!(
        fixture.spy.since(mark).is_empty(),
        "refused reports change nothing"
    );
    assert_eq!(
        fixture
            .scheduler
            .task_run(&claim.task_run_id)
            .unwrap()
            .result_digest,
        None
    );
    // A cancelled task is not retried.
    assert!(fixture.spy.pending() == 0);
    assert_eq!(fixture.scheduler.runs_of(&task).len(), 1);
}

#[test]
fn a_task_waiting_for_its_retry_can_be_cancelled() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain().with_retries(1)).unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .fail(&worker(), &claim.task_run_id, "ValueError")
        .unwrap();

    let outcome = fixture.scheduler.cancel(&task).unwrap();

    assert_eq!(outcome, Cancellation::Cancelled { was_running: false });
    assert!(fixture.spy.pending() == 0);
}

#[test]
fn a_finished_task_cannot_be_cancelled() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain()).unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .complete(
            &worker(),
            &claim.task_run_id,
            Digest::blake3(b"d"),
            Completion::Final,
        )
        .unwrap();

    let outcome = fixture.scheduler.cancel(&task).unwrap();

    assert_eq!(outcome, Cancellation::AlreadyFinished);
    assert_eq!(fixture.state(&task), TaskRunState::Succeeded);
    assert!(!fixture.scheduler.has_events());
}

#[test]
fn an_unknown_task_cannot_be_cancelled() {
    let mut fixture = Fixture::leading();

    let outcome = fixture
        .scheduler
        .cancel(&TaskId::new("no-such-task"))
        .unwrap();

    assert_eq!(outcome, Cancellation::UnknownTask);
}

#[test]
fn only_a_leader_decides_a_cancellation() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain()).unwrap();
    fixture.scheduler.set_leadership_grant(None);

    let result = fixture.scheduler.cancel(&task);

    assert_eq!(result.unwrap_err(), CancelRejection::NotLeader);
    assert_eq!(fixture.state(&task), TaskRunState::Queued);
}
