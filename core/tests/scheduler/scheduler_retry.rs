//! Retries: a failed run with retries left is replaced by a new run of the
//! same task, one at a time, and only the newest run can be reported on.


use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{Completion, Submission};
use kabudachi_core::time::Duration;
use crate::support::scheduler::Fixture;

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn submit_with_retries(fixture: &mut Fixture, retries: u32) -> TaskId {
    fixture
        .scheduler
        .submit(
            Submission::new(
                TaskDefinitionId::new("billing.charge"),
                0,
                b"input".to_vec(),
                "default",
            )
            .with_retries(retries),
        )
        .unwrap()
}

/// Claims and starts the task's current run.
fn start_attempt(fixture: &mut Fixture, task_id: &TaskId) -> TaskRunId {
    let claim = fixture.scheduler.request_claim(&worker(), task_id).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    claim.task_run_id
}

#[test]
fn a_failed_run_with_retries_left_is_replaced_by_a_queued_child_run() {
    let mut fixture = Fixture::leading();
    let task_id = submit_with_retries(&mut fixture, 2);
    let first = start_attempt(&mut fixture, &task_id);

    let failure = fixture
        .scheduler
        .fail(&worker(), &first, "ValueError")
        .unwrap();

    let retry = failure.retry.expect("a retry was due");
    assert_eq!(fixture.run_state(&first), TaskRunState::Failed);
    let child = fixture.scheduler.task_run(&retry).unwrap();
    assert_eq!(child.current_state(), TaskRunState::Queued);
    assert_eq!(child.attempt_number(), 2);
    assert_eq!(child.parent_task_run_id(), Some(first.clone()));
    assert_eq!(child.task_id(), task_id);
    assert_eq!(fixture.spy.run_of(&task_id).task_run_id(), retry);
    assert_eq!(fixture.spy.pending(), 1);
    assert_eq!(fixture.state(&task_id), TaskRunState::Queued);
}

#[test]
fn a_retry_does_not_change_the_task() {
    let mut fixture = Fixture::leading();
    let task_id = submit_with_retries(&mut fixture, 1);
    let before = fixture.spy.task(&task_id);
    let first = start_attempt(&mut fixture, &task_id);

    fixture
        .scheduler
        .fail(&worker(), &first, "ValueError")
        .unwrap();
    let claim = fixture
        .scheduler
        .request_claim(&worker(), &task_id)
        .unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .complete(
            &worker(),
            &claim.task_run_id,
            b"digest".to_vec(),
            Completion::Final,
        )
        .unwrap();

    assert_eq!(claim.task, before, "a worker is handed the task as submitted");
}

#[test]
fn a_task_gets_its_retries_plus_the_first_attempt_and_no_more() {
    let mut fixture = Fixture::leading();
    let task_id = submit_with_retries(&mut fixture, 2);

    let mut attempts = Vec::new();
    loop {
        let run = start_attempt(&mut fixture, &task_id);
        attempts.push(fixture.scheduler.task_run(&run).unwrap().attempt_number());
        if fixture
            .scheduler
            .fail(&worker(), &run, "ValueError")
            .unwrap()
            .retry
            .is_none()
        {
            break;
        }
    }

    assert_eq!(attempts, vec![1, 2, 3]);
    assert_eq!(fixture.spy.pending(), 0);
    assert_eq!(fixture.state(&task_id), TaskRunState::Failed);
}

#[test]
fn a_retry_that_succeeds_certifies_its_own_result() {
    let mut fixture = Fixture::leading();
    let task_id = submit_with_retries(&mut fixture, 1);
    let first = start_attempt(&mut fixture, &task_id);
    let retry = fixture
        .scheduler
        .fail(&worker(), &first, "ValueError")
        .unwrap()
        .retry
        .unwrap();

    let second = start_attempt(&mut fixture, &task_id);
    let certification = fixture
        .scheduler
        .complete(&worker(), &second, b"digest".to_vec(), Completion::Final)
        .unwrap();

    assert_eq!(second, retry);
    assert_eq!(certification.task_run_id, retry);
    assert_eq!(fixture.run_state(&retry), TaskRunState::Succeeded);
    assert!(
        fixture
            .scheduler
            .task_run(&first)
            .unwrap()
            .result_digest
            .is_empty()
    );
}

#[test]
fn a_finished_task_is_forgotten_with_every_run_counted_from_its_last() {
    let mut fixture = Fixture::leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(100)));
    let task_id = submit_with_retries(&mut fixture, 1);
    let first = start_attempt(&mut fixture, &task_id);
    fixture
        .scheduler
        .fail(&worker(), &first, "ValueError")
        .unwrap();
    fixture.clock.advance(Duration::from_ticks(80));
    let second = start_attempt(&mut fixture, &task_id);
    fixture
        .scheduler
        .fail(&worker(), &second, "ValueError")
        .unwrap();

    // The first failure is 120 ticks old, but the task only finished now.
    fixture.clock.advance(Duration::from_ticks(40));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 0);

    fixture.clock.advance(Duration::from_ticks(60));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 1);
    assert!(fixture.scheduler.task_run(&first).is_none());
    assert!(fixture.scheduler.task_run(&second).is_none());
    assert!(fixture.spy.forgotten(&task_id));
}

#[test]
fn a_task_that_is_waiting_for_its_retry_is_not_forgotten() {
    let mut fixture = Fixture::leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(10)));
    let task_id = submit_with_retries(&mut fixture, 1);
    let first = start_attempt(&mut fixture, &task_id);
    fixture
        .scheduler
        .fail(&worker(), &first, "ValueError")
        .unwrap();

    fixture.clock.advance(Duration::from_ticks(1_000));

    assert_eq!(fixture.scheduler.catch_up().forgotten, 0);
    assert_eq!(fixture.spy.pending(), 1);
    assert_eq!(fixture.state(&task_id), TaskRunState::Queued);
}

#[test]
fn every_run_of_a_task_can_be_listed_oldest_attempt_first() {
    let mut fixture = Fixture::leading();
    let task_id = submit_with_retries(&mut fixture, 1);
    assert_eq!(fixture.scheduler.runs_of(&task_id).len(), 1);
    let first = start_attempt(&mut fixture, &task_id);
    let retry = fixture
        .scheduler
        .fail(&worker(), &first, "ValueError")
        .unwrap()
        .retry
        .unwrap();

    assert_eq!(fixture.scheduler.runs_of(&task_id), vec![first, retry]);
    assert!(
        fixture
            .scheduler
            .runs_of(&TaskId::new("unknown"))
            .is_empty()
    );
}
