//! A task that returns a continuation (an implicit flow): its run is certified
//! at once, but the task is not over until its continuation is, so a
//! coalescing key stays held and its memory stays counted until the client
//! ends the continuation.


use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{Completion, ClaimRejection, Submission};
use kabudachi_core::time::Duration;
use crate::support::scheduler::Fixture;

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn plain(size: usize) -> Submission {
    Submission::new(
        TaskDefinitionId::new("billing.plan"),
        0,
        vec![b'x'; size],
        "default",
    )
}

fn generation() -> Submission {
    Submission::new(
        TaskDefinitionId::new("index.refresh"),
        0,
        b"g".to_vec(),
        "default",
    )
    .with_coalescing_key("k")
}

fn running(fixture: &mut Fixture, task: &TaskId) -> TaskRunId {
    let claim = fixture.scheduler.request_claim(&worker(), task).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    claim.task_run_id
}

#[test]
fn completing_with_a_continuation_certifies_the_run_like_any_other() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain(1)).unwrap();
    let run = running(&mut fixture, &task);

    let certification = fixture
        .scheduler
        .complete(&worker(), &run, b"plan".to_vec(), Completion::Continues)
        .unwrap();

    assert_eq!(certification.task_run_id, run);
    assert_eq!(certification.result_digest, b"plan".to_vec());
    assert_eq!(
        fixture.scheduler.task_run(&run).unwrap().current_state(),
        TaskRunState::Succeeded
    );
}

#[test]
fn a_task_with_a_continuation_still_counts_against_memory_until_it_ends() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain(40)).unwrap();
    let run = running(&mut fixture, &task);

    fixture
        .scheduler
        .complete(&worker(), &run, b"d".to_vec(), Completion::Continues)
        .unwrap();
    assert_eq!(fixture.spy.memory_in_use(), 40);

    assert!(fixture.scheduler.end_continuation(&task));
    assert_eq!(fixture.spy.memory_in_use(), 0);
}

#[test]
fn a_task_with_a_continuation_is_not_forgotten_until_it_ends() {
    let mut fixture = Fixture::leading();
    fixture
        .scheduler
        .set_result_ttl(Some(Duration::from_ticks(100)));
    let task = fixture.scheduler.submit(plain(1)).unwrap();
    let run = running(&mut fixture, &task);
    fixture
        .scheduler
        .complete(&worker(), &run, b"d".to_vec(), Completion::Continues)
        .unwrap();

    fixture.clock.advance(Duration::from_ticks(10_000));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 0);

    fixture.scheduler.end_continuation(&task);
    fixture.clock.advance(Duration::from_ticks(100));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 1);
    assert!(fixture.spy.forgotten(&task));
}

#[test]
fn a_coalescing_key_stays_held_for_the_life_of_the_continuation() {
    let mut fixture = Fixture::leading();
    let first = fixture.scheduler.submit(generation()).unwrap();
    let run = running(&mut fixture, &first);
    let newer = fixture.scheduler.submit(generation()).unwrap();

    fixture
        .scheduler
        .complete(&worker(), &run, b"d".to_vec(), Completion::Continues)
        .unwrap();

    assert!(
        fixture
            .scheduler
            .claim_oldest(&worker(), 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture
            .scheduler
            .request_claim(&worker(), &newer)
            .unwrap_err(),
        ClaimRejection::KeyBusy
    );

    fixture.scheduler.end_continuation(&first);

    let claims = fixture.scheduler.claim_oldest(&worker(), 10).unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].task.task_id(), newer);
}

#[test]
fn ending_a_continuation_is_once_only_and_ignores_tasks_that_have_none() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain(1)).unwrap();
    let other = fixture.scheduler.submit(plain(1)).unwrap();
    let run = running(&mut fixture, &task);
    fixture
        .scheduler
        .complete(&worker(), &run, b"d".to_vec(), Completion::Continues)
        .unwrap();

    assert!(!fixture.scheduler.end_continuation(&other));
    assert!(!fixture.scheduler.end_continuation(&TaskId::new("unknown")));
    assert!(fixture.scheduler.end_continuation(&task));
    assert!(!fixture.scheduler.end_continuation(&task));
    // Nothing was double-released.
    assert_eq!(fixture.spy.memory_in_use(), 1);
}
