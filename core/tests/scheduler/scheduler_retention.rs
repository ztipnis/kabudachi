//! How long finished tasks are kept: nothing is forgotten until a result TTL
//! is set, a task is forgotten once its own last run has aged past it, and a
//! task that has not finished (still queued, running, waiting for a retry or
//! continuing) is never forgotten.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::scheduler::{Completion, Submission};
use kabudachi_core::time::Duration;

use crate::support::scheduler::Fixture;

const TTL: u64 = 100;

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

fn running_task(fixture: &mut Fixture) -> (TaskId, TaskRunId) {
    let task_id = submit_with_retries(fixture, 0);
    let run = start_attempt(fixture, &task_id);
    (task_id, run)
}

fn complete(fixture: &mut Fixture, run: &TaskRunId, completion: Completion) {
    fixture
        .scheduler
        .complete(&worker(), run, Digest::blake3(b"digest"), completion)
        .unwrap();
}

#[test]
fn nothing_is_forgotten_until_a_result_ttl_is_set() {
    let mut fixture = Fixture::leading();
    let (task_id, run_id) = running_task(&mut fixture);
    complete(&mut fixture, &run_id, Completion::Final);
    fixture.clock.advance(Duration::from_ticks(1_000_000));

    assert_eq!(fixture.scheduler.catch_up().forgotten, 0);

    assert!(!fixture.spy.forgotten(&task_id));
}

#[test]
fn tasks_that_have_not_finished_are_never_forgotten() {
    let mut fixture = Fixture::leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(TTL)));
    let queued = submit_with_retries(&mut fixture, 0);
    let (running, _) = running_task(&mut fixture);

    fixture.clock.advance(Duration::from_ticks(TTL * 10));

    assert_eq!(fixture.scheduler.catch_up().forgotten, 0);
    assert!(!fixture.spy.forgotten(&queued));
    assert!(!fixture.spy.forgotten(&running));
}

#[test]
fn each_finished_task_is_forgotten_at_its_own_time() {
    let mut fixture = Fixture::leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(TTL)));
    let (early, early_run) = running_task(&mut fixture);
    complete(&mut fixture, &early_run, Completion::Final);
    fixture.clock.advance(Duration::from_ticks(60));
    let (late, late_run) = running_task(&mut fixture);
    complete(&mut fixture, &late_run, Completion::Final);

    fixture.clock.advance(Duration::from_ticks(40));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 1);
    assert!(fixture.spy.forgotten(&early));
    assert!(!fixture.spy.forgotten(&late));

    fixture.clock.advance(Duration::from_ticks(60));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 1);
    assert!(fixture.spy.forgotten(&late));
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
    assert!(!fixture.spy.forgotten(&task_id));
}

#[test]
fn a_task_with_a_continuation_is_not_forgotten_until_it_ends() {
    let mut fixture = Fixture::leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(100)));
    let (task, run) = running_task(&mut fixture);
    complete(&mut fixture, &run, Completion::Continues);

    fixture.clock.advance(Duration::from_ticks(10_000));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 0);

    fixture.scheduler.end_continuation(&task).unwrap();
    fixture.clock.advance(Duration::from_ticks(100));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 1);
    assert!(fixture.spy.forgotten(&task));
}
