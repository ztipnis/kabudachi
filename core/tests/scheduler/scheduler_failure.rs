//! A run that fails, and how long finished tasks are kept: the leader accepts
//! a failure only from the worker running the run, records what kind it was,
//! and forgets finished tasks once `result_ttl` has passed.


use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{Completion, ReportRejection, Submission};
use kabudachi_core::time::Duration;
use crate::support::scheduler::Fixture;

const TTL: u64 = 100;

fn worker(name: &str) -> WorkerId {
    WorkerId::new(name)
}

fn submit(fixture: &mut Fixture) -> TaskId {
    fixture
        .scheduler
        .submit(Submission::new(
            TaskDefinitionId::new("billing.charge"),
            0,
            b"input".to_vec(),
            "default",
        ))
        .unwrap()
}

fn running_task(fixture: &mut Fixture) -> (TaskId, TaskRunId) {
    let task_id = submit(fixture);
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();
    fixture
        .scheduler
        .report_started(&worker("w1"), &claim.task_run_id)
        .unwrap();
    (task_id, claim.task_run_id)
}

#[test]
fn a_running_run_can_fail_and_records_what_kind_of_failure_it_was() {
    let mut fixture = Fixture::leading();
    let (task_id, run_id) = running_task(&mut fixture);

    let failure = fixture
        .scheduler
        .fail(&worker("w1"), &run_id, "ValueError")
        .unwrap();

    assert_eq!(failure.task_id, task_id);
    assert_eq!(failure.task_run_id, run_id);
    assert_eq!(fixture.run_state(&run_id), TaskRunState::Failed);
    let run = fixture.scheduler.task_run(&run_id).unwrap();
    assert_eq!(run.failure_kind, "ValueError");
    assert!(run.result_digest.is_empty());
}

#[test]
fn a_run_that_never_started_cannot_fail() {
    let mut fixture = Fixture::leading();
    let task_id = submit(&mut fixture);
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();

    let result = fixture
        .scheduler
        .fail(&worker("w1"), &claim.task_run_id, "ValueError");

    assert_eq!(result.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(
        fixture.run_state(&claim.task_run_id),
        TaskRunState::Claimed
    );
}

#[test]
fn a_failed_run_cannot_then_complete_or_fail_again() {
    let mut fixture = Fixture::leading();
    let (_, run_id) = running_task(&mut fixture);
    fixture
        .scheduler
        .fail(&worker("w1"), &run_id, "ValueError")
        .unwrap();

    let completed = fixture
        .scheduler
        .complete(&worker("w1"), &run_id, b"digest".to_vec(), Completion::Final);
    let failed_again = fixture.scheduler.fail(&worker("w1"), &run_id, "KeyError");

    assert_eq!(completed.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(failed_again.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(
        fixture.scheduler.task_run(&run_id).unwrap().failure_kind,
        "ValueError"
    );
}

#[test]
fn nothing_is_forgotten_until_a_result_ttl_is_set() {
    let mut fixture = Fixture::leading();
    let (task_id, run_id) = running_task(&mut fixture);
    fixture
        .scheduler
        .complete(&worker("w1"), &run_id, b"digest".to_vec(), Completion::Final)
        .unwrap();
    fixture.clock.advance(Duration::from_ticks(1_000_000));

    assert_eq!(fixture.scheduler.catch_up().forgotten, 0);

    assert!(!fixture.spy.forgotten(&task_id));
}

#[test]
fn a_finished_task_is_kept_for_the_result_ttl_and_then_forgotten() {
    let mut fixture = Fixture::leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(TTL)));
    let (task_id, run_id) = running_task(&mut fixture);
    fixture
        .scheduler
        .complete(&worker("w1"), &run_id, b"digest".to_vec(), Completion::Final)
        .unwrap();

    fixture.clock.advance(Duration::from_ticks(TTL - 1));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 0);
    assert!(fixture.scheduler.task_run(&run_id).is_some());

    fixture.clock.advance(Duration::from_ticks(1));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 1);
    assert!(fixture.spy.forgotten(&task_id));
    assert!(fixture.scheduler.task_run(&run_id).is_none());
}

#[test]
fn tasks_that_have_not_finished_are_never_forgotten() {
    let mut fixture = Fixture::leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(TTL)));
    let queued = submit(&mut fixture);
    let (running, _) = running_task(&mut fixture);

    fixture.clock.advance(Duration::from_ticks(TTL * 10));

    assert_eq!(fixture.scheduler.catch_up().forgotten, 0);
    assert!(!fixture.spy.forgotten(&queued));
    assert!(!fixture.spy.forgotten(&running));
    assert_eq!(fixture.spy.pending(), 1);
    assert_eq!(fixture.state(&queued), TaskRunState::Queued);
}

#[test]
fn each_finished_task_is_forgotten_at_its_own_time() {
    let mut fixture = Fixture::leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(TTL)));
    let (early, early_run) = running_task(&mut fixture);
    fixture
        .scheduler
        .complete(&worker("w1"), &early_run, b"a".to_vec(), Completion::Final)
        .unwrap();
    fixture.clock.advance(Duration::from_ticks(60));
    let (late, late_run) = running_task(&mut fixture);
    fixture
        .scheduler
        .complete(&worker("w1"), &late_run, b"b".to_vec(), Completion::Final)
        .unwrap();

    fixture.clock.advance(Duration::from_ticks(40));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 1);
    assert!(fixture.spy.forgotten(&early));
    assert!(!fixture.spy.forgotten(&late));

    fixture.clock.advance(Duration::from_ticks(60));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 1);
    assert!(fixture.spy.forgotten(&late));
}
