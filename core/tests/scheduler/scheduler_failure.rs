//! A run that fails, and how long finished tasks are kept: the leader accepts
//! a failure only from the worker running the run, records what kind it was,
//! and forgets finished tasks once `result_ttl` has passed.


use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{ReportRejection, Scheduler, Submission};
use kabudachi_core::time::Duration;
use crate::support::clock::FakeClock;
use crate::support::grant::unbounded_grant;
use crate::support::ids::SequentialIds;

const TTL: u64 = 100;

fn worker(name: &str) -> WorkerId {
    WorkerId::new(name)
}

struct Fixture {
    clock: FakeClock,
    scheduler: Scheduler<FakeClock, SequentialIds>,
}

fn leading() -> Fixture {
    let clock = FakeClock::new();
    let mut scheduler = Scheduler::new(clock.clone(), SequentialIds::new());
    scheduler.set_leadership_grant(Some(unbounded_grant()));
    Fixture { clock, scheduler }
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

fn state_of(fixture: &Fixture, run: &TaskRunId) -> TaskRunState {
    fixture.scheduler.task_run(run).unwrap().current_state()
}

#[test]
fn a_running_run_can_fail_and_records_what_kind_of_failure_it_was() {
    let mut fixture = leading();
    let (task_id, run_id) = running_task(&mut fixture);

    let failure = fixture
        .scheduler
        .fail(&worker("w1"), &run_id, "ValueError")
        .unwrap();

    assert_eq!(failure.task_id, task_id);
    assert_eq!(failure.task_run_id, run_id);
    assert_eq!(state_of(&fixture, &run_id), TaskRunState::Failed);
    let run = fixture.scheduler.task_run(&run_id).unwrap();
    assert_eq!(run.failure_kind, "ValueError");
    assert!(run.result_digest.is_empty());
}

#[test]
fn a_run_that_never_started_cannot_fail() {
    let mut fixture = leading();
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
        state_of(&fixture, &claim.task_run_id),
        TaskRunState::Claimed
    );
}

#[test]
fn a_failed_run_cannot_then_complete_or_fail_again() {
    let mut fixture = leading();
    let (_, run_id) = running_task(&mut fixture);
    fixture
        .scheduler
        .fail(&worker("w1"), &run_id, "ValueError")
        .unwrap();

    let completed = fixture
        .scheduler
        .complete(&worker("w1"), &run_id, b"digest".to_vec());
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
    let mut fixture = leading();
    let (task_id, run_id) = running_task(&mut fixture);
    fixture
        .scheduler
        .complete(&worker("w1"), &run_id, b"digest".to_vec())
        .unwrap();
    fixture.clock.advance(Duration::from_ticks(1_000_000));

    assert_eq!(fixture.scheduler.sweep(), 0);

    assert!(fixture.scheduler.task(&task_id).is_some());
}

#[test]
fn a_finished_task_is_kept_for_the_result_ttl_and_then_forgotten() {
    let mut fixture = leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(TTL)));
    let (task_id, run_id) = running_task(&mut fixture);
    fixture
        .scheduler
        .complete(&worker("w1"), &run_id, b"digest".to_vec())
        .unwrap();

    fixture.clock.advance(Duration::from_ticks(TTL - 1));
    assert_eq!(fixture.scheduler.sweep(), 0);
    assert!(fixture.scheduler.task_run(&run_id).is_some());

    fixture.clock.advance(Duration::from_ticks(1));
    assert_eq!(fixture.scheduler.sweep(), 1);
    assert!(fixture.scheduler.task(&task_id).is_none());
    assert!(fixture.scheduler.task_run(&run_id).is_none());
    assert!(fixture.scheduler.run_of(&task_id).is_none());
}

#[test]
fn tasks_that_have_not_finished_are_never_forgotten() {
    let mut fixture = leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(TTL)));
    let queued = submit(&mut fixture);
    let (running, _) = running_task(&mut fixture);

    fixture.clock.advance(Duration::from_ticks(TTL * 10));

    assert_eq!(fixture.scheduler.sweep(), 0);
    assert!(fixture.scheduler.task(&queued).is_some());
    assert!(fixture.scheduler.task(&running).is_some());
    assert_eq!(fixture.scheduler.pending_tasks(), vec![queued]);
}

#[test]
fn each_finished_task_is_forgotten_at_its_own_time() {
    let mut fixture = leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(TTL)));
    let (early, early_run) = running_task(&mut fixture);
    fixture
        .scheduler
        .complete(&worker("w1"), &early_run, b"a".to_vec())
        .unwrap();
    fixture.clock.advance(Duration::from_ticks(60));
    let (late, late_run) = running_task(&mut fixture);
    fixture
        .scheduler
        .complete(&worker("w1"), &late_run, b"b".to_vec())
        .unwrap();

    fixture.clock.advance(Duration::from_ticks(40));
    assert_eq!(fixture.scheduler.sweep(), 1);
    assert!(fixture.scheduler.task(&early).is_none());
    assert!(fixture.scheduler.task(&late).is_some());

    fixture.clock.advance(Duration::from_ticks(60));
    assert_eq!(fixture.scheduler.sweep(), 1);
    assert!(fixture.scheduler.task(&late).is_none());
}
