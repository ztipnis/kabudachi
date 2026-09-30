//! A worker that is lost while it holds runs (README §3.2.1, §3.2.2, §25.4.5):
//! its runs become `Lost`, and are replayed by a new run of the same task
//! (at-least-once), except that a coalescing generation is only replayed if it
//! is the newest for its key. An ephemeral task's lost run is not replayed, and a
//! non-retriable task's running run is orphaned instead (README §3.2.2).

use crate::support::scheduler::Fixture;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    ClaimRejection, Completion, LoseRejection, ReportRejection, Submission,
};

fn worker(name: &str) -> WorkerId {
    WorkerId::new(name)
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

fn running(fixture: &mut Fixture, who: &WorkerId, task: &TaskId) -> TaskRunId {
    let claim = fixture.scheduler.request_claim(who, task).unwrap();
    fixture
        .scheduler
        .report_started(who, &claim.task_run_id)
        .unwrap();
    claim.task_run_id
}

#[test]
fn a_lost_running_run_is_replaced_by_a_queued_next_attempt() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain("p")).unwrap();
    let run = running(&mut fixture, &worker("w1"), &task);

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(lost.len(), 1);
    assert_eq!(lost[0].task_run_id, run);
    assert_eq!(fixture.run_state(&run), TaskRunState::Lost);
    let replay = lost[0].replayed.clone().expect("replayed");
    let next = fixture.scheduler.task_run(&replay).unwrap();
    assert_eq!(next.current_state(), TaskRunState::Queued);
    assert_eq!(next.attempt_number(), 2);
    assert_eq!(next.parent_task_run_id(), Some(run));
    assert_eq!(fixture.spy.pending(), 1);
    assert_eq!(fixture.state(&task), TaskRunState::Queued);
}

#[test]
fn a_lost_claimed_run_is_replayed_too() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain("p")).unwrap();
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task)
        .unwrap();

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(fixture.run_state(&claim.task_run_id), TaskRunState::Lost);
    assert!(lost[0].replayed.is_some(), "no retries were configured");
}

#[test]
fn a_replay_after_a_loss_does_not_use_up_a_retry() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(plain("p").with_retries(1))
        .unwrap();
    let first = running(&mut fixture, &worker("w1"), &task);
    fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    // The replay is attempt 2, but only failures count against retries: one
    // failure of it is still retried, and a second is not.
    let replay = running(&mut fixture, &worker("w2"), &task);
    let after_first_failure = fixture.scheduler.fail(&worker("w2"), &replay, "E").unwrap();
    let retry = after_first_failure
        .retry
        .expect("the one retry is still available");
    fixture
        .scheduler
        .request_claim(&worker("w2"), &task)
        .unwrap();
    fixture
        .scheduler
        .report_started(&worker("w2"), &retry)
        .unwrap();
    let after_second_failure = fixture.scheduler.fail(&worker("w2"), &retry, "E").unwrap();

    assert_ne!(first, replay);
    assert_eq!(after_second_failure.retry, None);
}

#[test]
fn only_the_lost_workers_runs_are_lost() {
    let mut fixture = Fixture::leading();
    let mine = fixture.scheduler.submit(plain("a")).unwrap();
    let theirs = fixture.scheduler.submit(plain("b")).unwrap();
    running(&mut fixture, &worker("w1"), &mine);
    let other = running(&mut fixture, &worker("w2"), &theirs);

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(lost.len(), 1);
    assert_eq!(fixture.run_state(&other), TaskRunState::Running);
}

#[test]
fn a_finished_or_pending_run_is_not_touched() {
    let mut fixture = Fixture::leading();
    let done = fixture.scheduler.submit(plain("a")).unwrap();
    let waiting = fixture.scheduler.submit(plain("b")).unwrap();
    let run = running(&mut fixture, &worker("w1"), &done);
    fixture
        .scheduler
        .complete(&worker("w1"), &run, b"d".to_vec(), Completion::Final)
        .unwrap();

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert!(lost.is_empty());
    assert_eq!(fixture.run_state(&run), TaskRunState::Succeeded);
    assert_eq!(fixture.spy.pending(), 1);
    assert_eq!(fixture.state(&waiting), TaskRunState::Queued);
}

#[test]
fn only_a_leader_decides_that_a_worker_is_lost() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain("p")).unwrap();
    let run = running(&mut fixture, &worker("w1"), &task);
    fixture.scheduler.set_leadership_grant(None);

    assert_eq!(
        fixture.scheduler.lose_worker(&worker("w1")).unwrap_err(),
        LoseRejection::NotLeader
    );
    assert_eq!(fixture.run_state(&run), TaskRunState::Running);
}

#[test]
fn a_lost_coalescing_generation_that_is_the_newest_is_replayed() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(generation("a")).unwrap();
    let run = running(&mut fixture, &worker("w1"), &task);

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(fixture.run_state(&run), TaskRunState::Lost);
    let replay = lost[0].replayed.clone().expect("replayed");
    assert_eq!(fixture.run_state(&replay), TaskRunState::Queued);
    // It still holds the key, so it is the one that runs again.
    let claims = fixture.scheduler.claim_oldest(&worker("w2"), 10).unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].task.task_id(), task);
    assert_eq!(claims[0].attempt_number, 2);
}

#[test]
fn a_lost_coalescing_generation_with_a_newer_one_waiting_is_not_replayed() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(generation("a")).unwrap();
    let run = running(&mut fixture, &worker("w1"), &task);
    let newer = fixture.scheduler.submit(generation("b")).unwrap();

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(fixture.run_state(&run), TaskRunState::Lost);
    assert_eq!(lost[0].replayed, None);
    // The newer generation runs; the lost one stays Lost and its payload is
    // not folded into it.
    let claims = fixture.scheduler.claim_oldest(&worker("w2"), 10).unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].task.task_id(), newer);
    assert!(claims[0].chain.is_empty());
    assert_eq!(
        fixture
            .scheduler
            .request_claim(&worker("w2"), &task)
            .unwrap_err(),
        ClaimRejection::Finished
    );
}

#[test]
fn a_generation_lost_and_not_replayed_stops_counting_against_memory() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(generation("aaaa")).unwrap();
    running(&mut fixture, &worker("w1"), &task);
    fixture.scheduler.submit(generation("bb")).unwrap();
    assert_eq!(fixture.spy.memory_in_use(), 6);

    fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(fixture.spy.memory_in_use(), 2);
}

#[test]
fn a_task_whose_continuation_is_running_is_not_lost_with_its_worker() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(generation("a")).unwrap();
    let run = running(&mut fixture, &worker("w1"), &task);
    fixture
        .scheduler
        .complete(&worker("w1"), &run, b"d".to_vec(), Completion::Continues)
        .unwrap();

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert!(lost.is_empty());
    assert_eq!(fixture.run_state(&run), TaskRunState::Succeeded);
}

#[test]
fn a_lost_workers_report_on_its_lost_run_is_refused() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain("p")).unwrap();
    let run = running(&mut fixture, &worker("w1"), &task);
    fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    let completed = fixture
        .scheduler
        .complete(&worker("w1"), &run, b"d".to_vec(), Completion::Final);
    let failed = fixture.scheduler.fail(&worker("w1"), &run, "ValueError");

    assert_eq!(completed.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(failed.unwrap_err(), ReportRejection::NotAuthoritative);
}

#[test]
fn an_ephemeral_tasks_lost_run_is_not_replayed_and_the_task_is_over() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain("aaaa").ephemeral()).unwrap();
    let run = running(&mut fixture, &worker("w1"), &task);

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(lost[0].state, TaskRunState::Lost);
    assert_eq!(lost[0].replayed, None);
    assert_eq!(fixture.run_state(&run), TaskRunState::Lost);
    assert_eq!(fixture.scheduler.runs_of(&task), vec![run]);
    assert_eq!(fixture.spy.memory_in_use(), 0);
    assert_eq!(
        fixture
            .scheduler
            .request_claim(&worker("w2"), &task)
            .unwrap_err(),
        ClaimRejection::Finished
    );
}

#[test]
fn an_ephemeral_tasks_claimed_run_is_lost_and_not_replayed_either() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain("p").ephemeral()).unwrap();
    fixture
        .scheduler
        .request_claim(&worker("w1"), &task)
        .unwrap();

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(lost[0].state, TaskRunState::Lost);
    assert_eq!(lost[0].replayed, None);
}

#[test]
fn a_non_retriable_tasks_running_run_is_orphaned_and_not_replayed() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(plain("aaaa").non_retriable())
        .unwrap();
    let run = running(&mut fixture, &worker("w1"), &task);

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(lost[0].state, TaskRunState::Orphaned);
    assert_eq!(lost[0].replayed, None);
    assert_eq!(fixture.run_state(&run), TaskRunState::Orphaned);
    assert_eq!(fixture.scheduler.runs_of(&task), vec![run.clone()]);
    assert_eq!(fixture.spy.memory_in_use(), 0);
    // What the lost worker reports afterwards is not authoritative.
    assert_eq!(
        fixture
            .scheduler
            .complete(&worker("w1"), &run, b"d".to_vec(), Completion::Final)
            .unwrap_err(),
        ReportRejection::NotAuthoritative
    );
}

#[test]
fn a_non_retriable_tasks_claimed_run_never_started_so_it_is_replayed() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain("p").non_retriable()).unwrap();
    fixture
        .scheduler
        .request_claim(&worker("w1"), &task)
        .unwrap();

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(lost[0].state, TaskRunState::Lost);
    assert!(lost[0].replayed.is_some());
}

#[test]
fn a_task_kind_and_retriable_flag_are_part_of_the_submitted_task() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(plain("p").ephemeral().non_retriable())
        .unwrap();

    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task)
        .unwrap();

    assert!(claim.task.ephemeral);
    assert!(claim.task.non_retriable);
}
