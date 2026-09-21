//! A worker that is lost while it holds runs (README §3.2.1, §3.2.2, §25.4.5):
//! its runs become `Lost`, and are replayed by a new run of the same task
//! (at-least-once), except that a coalescing generation is only replayed if it
//! is the newest for its key.

mod support;

use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{
    ClaimRejection, LoseRejection, ReportRejection, Scheduler, Submission,
};
use support::clock::FakeClock;
use support::ids::SequentialIds;

fn worker(name: &str) -> WorkerId {
    WorkerId::new(name)
}

fn leading() -> Scheduler<FakeClock, SequentialIds> {
    let mut scheduler = Scheduler::new(FakeClock::new(), SequentialIds::new());
    scheduler.set_worker_state(WorkerState::Leader);
    scheduler
}

fn plain(payload: &str) -> Submission {
    Submission::new(
        TaskDefinitionId::new("billing.charge"),
        0,
        payload.as_bytes().to_vec(),
        "default",
    )
}

fn generation(payload: &str) -> Submission {
    Submission::new(
        TaskDefinitionId::new("index.refresh"),
        0,
        payload.as_bytes().to_vec(),
        "default",
    )
    .with_coalescing_key("k")
}

fn state(scheduler: &Scheduler<FakeClock, SequentialIds>, run: &TaskRunId) -> TaskRunState {
    scheduler.task_run(run).unwrap().current_state()
}

fn running(
    scheduler: &mut Scheduler<FakeClock, SequentialIds>,
    who: &WorkerId,
    task: &TaskId,
) -> TaskRunId {
    let claim = scheduler.request_claim(who, task).unwrap();
    scheduler.report_started(who, &claim.task_run_id).unwrap();
    claim.task_run_id
}

#[test]
fn a_lost_running_run_is_replaced_by_a_queued_next_attempt() {
    let mut scheduler = leading();
    let task = scheduler.submit(plain("p")).unwrap();
    let run = running(&mut scheduler, &worker("w1"), &task);

    let lost = scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(lost.len(), 1);
    assert_eq!(lost[0].task_run_id, run);
    assert_eq!(state(&scheduler, &run), TaskRunState::Lost);
    let replay = lost[0].replayed.clone().expect("replayed");
    let next = scheduler.task_run(&replay).unwrap();
    assert_eq!(next.current_state(), TaskRunState::Queued);
    assert_eq!(next.attempt_number(), 2);
    assert_eq!(next.parent_task_run_id(), Some(run));
    assert_eq!(scheduler.pending_tasks(), vec![task]);
}

#[test]
fn a_lost_claimed_run_is_replayed_too() {
    let mut scheduler = leading();
    let task = scheduler.submit(plain("p")).unwrap();
    let claim = scheduler.request_claim(&worker("w1"), &task).unwrap();

    let lost = scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(state(&scheduler, &claim.task_run_id), TaskRunState::Lost);
    assert!(lost[0].replayed.is_some(), "no retries were configured");
}

#[test]
fn a_replay_after_a_loss_does_not_use_up_a_retry() {
    let mut scheduler = leading();
    let task = scheduler.submit(plain("p").with_retries(1)).unwrap();
    let first = running(&mut scheduler, &worker("w1"), &task);
    scheduler.lose_worker(&worker("w1")).unwrap();

    // The replay is attempt 2, but only failures count against retries: one
    // failure of it is still retried, and a second is not.
    let replay = running(&mut scheduler, &worker("w2"), &task);
    let after_first_failure = scheduler.fail(&worker("w2"), &replay, "E").unwrap();
    let retry = after_first_failure
        .retry
        .expect("the one retry is still available");
    scheduler.request_claim(&worker("w2"), &task).unwrap();
    scheduler.report_started(&worker("w2"), &retry).unwrap();
    let after_second_failure = scheduler.fail(&worker("w2"), &retry, "E").unwrap();

    assert_ne!(first, replay);
    assert_eq!(after_second_failure.retry, None);
}

#[test]
fn only_the_lost_workers_runs_are_lost() {
    let mut scheduler = leading();
    let mine = scheduler.submit(plain("a")).unwrap();
    let theirs = scheduler.submit(plain("b")).unwrap();
    running(&mut scheduler, &worker("w1"), &mine);
    let other = running(&mut scheduler, &worker("w2"), &theirs);

    let lost = scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(lost.len(), 1);
    assert_eq!(state(&scheduler, &other), TaskRunState::Running);
}

#[test]
fn a_finished_or_pending_run_is_not_touched() {
    let mut scheduler = leading();
    let done = scheduler.submit(plain("a")).unwrap();
    let waiting = scheduler.submit(plain("b")).unwrap();
    let run = running(&mut scheduler, &worker("w1"), &done);
    scheduler
        .complete(&worker("w1"), &run, b"d".to_vec())
        .unwrap();

    let lost = scheduler.lose_worker(&worker("w1")).unwrap();

    assert!(lost.is_empty());
    assert_eq!(state(&scheduler, &run), TaskRunState::Succeeded);
    assert_eq!(scheduler.pending_tasks(), vec![waiting]);
}

#[test]
fn what_the_lost_worker_reports_afterwards_is_refused() {
    let mut scheduler = leading();
    let task = scheduler.submit(plain("p")).unwrap();
    let run = running(&mut scheduler, &worker("w1"), &task);
    scheduler.lose_worker(&worker("w1")).unwrap();

    let completed = scheduler.complete(&worker("w1"), &run, b"late".to_vec());
    let failed = scheduler.fail(&worker("w1"), &run, "ValueError");

    assert_eq!(completed.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(failed.unwrap_err(), ReportRejection::NotAuthoritative);
}

#[test]
fn only_a_leader_decides_that_a_worker_is_lost() {
    let mut scheduler = leading();
    let task = scheduler.submit(plain("p")).unwrap();
    let run = running(&mut scheduler, &worker("w1"), &task);
    scheduler.set_worker_state(WorkerState::Active);

    assert_eq!(
        scheduler.lose_worker(&worker("w1")).unwrap_err(),
        LoseRejection::NotLeader
    );
    assert_eq!(state(&scheduler, &run), TaskRunState::Running);
}

#[test]
fn a_lost_coalescing_generation_that_is_the_newest_is_replayed() {
    let mut scheduler = leading();
    let task = scheduler.submit(generation("a")).unwrap();
    let run = running(&mut scheduler, &worker("w1"), &task);

    let lost = scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(state(&scheduler, &run), TaskRunState::Lost);
    let replay = lost[0].replayed.clone().expect("replayed");
    assert_eq!(state(&scheduler, &replay), TaskRunState::Queued);
    // It still holds the key, so it is the one that runs again.
    let claims = scheduler.claim_oldest(&worker("w2"), 10).unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].task.task_id(), task);
    assert_eq!(claims[0].attempt_number, 2);
}

#[test]
fn a_lost_coalescing_generation_with_a_newer_one_waiting_is_not_replayed() {
    let mut scheduler = leading();
    let task = scheduler.submit(generation("a")).unwrap();
    let run = running(&mut scheduler, &worker("w1"), &task);
    let newer = scheduler.submit(generation("b")).unwrap();

    let lost = scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(state(&scheduler, &run), TaskRunState::Lost);
    assert_eq!(lost[0].replayed, None);
    // The newer generation runs; the lost one stays Lost and its payload is
    // not folded into it.
    let claims = scheduler.claim_oldest(&worker("w2"), 10).unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].task.task_id(), newer);
    assert!(claims[0].chain.is_empty());
    assert_eq!(
        scheduler.request_claim(&worker("w2"), &task).unwrap_err(),
        ClaimRejection::Finished
    );
}

#[test]
fn a_generation_lost_and_not_replayed_stops_counting_against_memory() {
    let mut scheduler = leading();
    let task = scheduler.submit(generation("aaaa")).unwrap();
    running(&mut scheduler, &worker("w1"), &task);
    scheduler.submit(generation("bb")).unwrap();
    assert_eq!(scheduler.memory_in_use(), 6);

    scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(scheduler.memory_in_use(), 2);
}

#[test]
fn a_task_whose_continuation_is_running_is_not_lost_with_its_worker() {
    let mut scheduler = leading();
    let task = scheduler.submit(generation("a")).unwrap();
    let run = running(&mut scheduler, &worker("w1"), &task);
    scheduler
        .complete_and_continue(&worker("w1"), &run, b"d".to_vec())
        .unwrap();

    let lost = scheduler.lose_worker(&worker("w1")).unwrap();

    assert!(lost.is_empty());
    assert_eq!(state(&scheduler, &run), TaskRunState::Succeeded);
}
