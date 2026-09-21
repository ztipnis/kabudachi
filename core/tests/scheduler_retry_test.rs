//! Retries: a failed run with retries left is replaced by a new run of the
//! same task, one at a time, and only the newest run can be reported on
//! (README §25.1.2, §25.1.4).

mod support;

use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{ReportRejection, Scheduler, Submission};
use kabudachi_core::time::Duration;
use support::clock::FakeClock;
use support::ids::SequentialIds;

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

struct Fixture {
    clock: FakeClock,
    scheduler: Scheduler<FakeClock, SequentialIds>,
}

fn leading() -> Fixture {
    let clock = FakeClock::new();
    let mut scheduler = Scheduler::new(clock.clone(), SequentialIds::new());
    scheduler.set_worker_state(WorkerState::Leader);
    Fixture { clock, scheduler }
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

fn state_of(fixture: &Fixture, run: &TaskRunId) -> TaskRunState {
    fixture.scheduler.task_run(run).unwrap().current_state()
}

#[test]
fn a_failed_run_with_retries_left_is_replaced_by_a_queued_child_run() {
    let mut fixture = leading();
    let task_id = submit_with_retries(&mut fixture, 2);
    let first = start_attempt(&mut fixture, &task_id);

    let failure = fixture
        .scheduler
        .fail(&worker(), &first, "ValueError")
        .unwrap();

    let retry = failure.retry.expect("a retry was due");
    assert_eq!(state_of(&fixture, &first), TaskRunState::Failed);
    let child = fixture.scheduler.task_run(&retry).unwrap();
    assert_eq!(child.current_state(), TaskRunState::Queued);
    assert_eq!(child.attempt_number(), 2);
    assert_eq!(child.parent_task_run_id(), Some(first.clone()));
    assert_eq!(child.task_id(), task_id);
    assert_eq!(
        fixture.scheduler.run_of(&task_id).unwrap().task_run_id(),
        retry
    );
    assert_eq!(fixture.scheduler.pending_tasks(), vec![task_id]);
}

#[test]
fn a_retry_does_not_change_the_task() {
    let mut fixture = leading();
    let task_id = submit_with_retries(&mut fixture, 1);
    let before = fixture.scheduler.task(&task_id).unwrap().clone();
    let first = start_attempt(&mut fixture, &task_id);

    fixture
        .scheduler
        .fail(&worker(), &first, "ValueError")
        .unwrap();

    assert_eq!(*fixture.scheduler.task(&task_id).unwrap(), before);
}

#[test]
fn a_task_gets_its_retries_plus_the_first_attempt_and_no_more() {
    let mut fixture = leading();
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
    assert!(fixture.scheduler.pending_tasks().is_empty());
    let last = fixture.scheduler.run_of(&task_id).unwrap();
    assert_eq!(last.current_state(), TaskRunState::Failed);
}

#[test]
fn a_task_without_retries_fails_once_and_for_all() {
    let mut fixture = leading();
    let task_id = submit_with_retries(&mut fixture, 0);
    let run = start_attempt(&mut fixture, &task_id);

    let failure = fixture
        .scheduler
        .fail(&worker(), &run, "ValueError")
        .unwrap();

    assert_eq!(failure.retry, None);
    assert!(fixture.scheduler.pending_tasks().is_empty());
}

#[test]
fn a_retry_that_succeeds_certifies_its_own_result() {
    let mut fixture = leading();
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
        .complete(&worker(), &second, b"digest".to_vec())
        .unwrap();

    assert_eq!(second, retry);
    assert_eq!(certification.task_run_id, retry);
    assert_eq!(state_of(&fixture, &retry), TaskRunState::Succeeded);
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
fn a_claim_says_which_attempt_it_is() {
    let mut fixture = leading();
    let task_id = submit_with_retries(&mut fixture, 1);
    let first = fixture
        .scheduler
        .request_claim(&worker(), &task_id)
        .unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &first.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .fail(&worker(), &first.task_run_id, "ValueError")
        .unwrap();

    let second = fixture
        .scheduler
        .claim_oldest(&worker(), 1)
        .unwrap()
        .remove(0);

    assert_eq!(first.attempt_number, 1);
    assert_eq!(second.attempt_number, 2);
}

#[test]
fn only_the_newest_run_can_be_reported_on() {
    let mut fixture = leading();
    let task_id = submit_with_retries(&mut fixture, 1);
    let first = start_attempt(&mut fixture, &task_id);
    fixture
        .scheduler
        .fail(&worker(), &first, "ValueError")
        .unwrap();
    start_attempt(&mut fixture, &task_id);

    let started = fixture.scheduler.report_started(&worker(), &first);
    let completed = fixture
        .scheduler
        .complete(&worker(), &first, b"late".to_vec());
    let failed = fixture.scheduler.fail(&worker(), &first, "KeyError");

    assert_eq!(started.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(completed.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(failed.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(state_of(&fixture, &first), TaskRunState::Failed);
}

#[test]
fn a_failure_reported_by_a_non_leader_starts_no_retry() {
    let mut fixture = leading();
    let task_id = submit_with_retries(&mut fixture, 3);
    let first = start_attempt(&mut fixture, &task_id);
    fixture.scheduler.set_worker_state(WorkerState::Active);

    let result = fixture.scheduler.fail(&worker(), &first, "ValueError");

    assert_eq!(result.unwrap_err(), ReportRejection::NotLeader);
    assert!(fixture.scheduler.pending_tasks().is_empty());
    assert_eq!(state_of(&fixture, &first), TaskRunState::Running);
}

#[test]
fn a_finished_task_is_forgotten_with_every_run_counted_from_its_last() {
    let mut fixture = leading();
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
    assert_eq!(fixture.scheduler.sweep(), 0);

    fixture.clock.advance(Duration::from_ticks(60));
    assert_eq!(fixture.scheduler.sweep(), 1);
    assert!(fixture.scheduler.task_run(&first).is_none());
    assert!(fixture.scheduler.task_run(&second).is_none());
    assert!(fixture.scheduler.task(&task_id).is_none());
}

#[test]
fn a_task_that_is_waiting_for_its_retry_is_not_forgotten() {
    let mut fixture = leading();
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

    assert_eq!(fixture.scheduler.sweep(), 0);
    assert_eq!(fixture.scheduler.pending_tasks(), vec![task_id]);
}

#[test]
fn every_run_of_a_task_can_be_listed_oldest_attempt_first() {
    let mut fixture = leading();
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
