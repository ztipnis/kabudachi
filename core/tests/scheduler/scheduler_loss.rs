//! A worker that is lost while it holds runs:
//! its runs become `Lost`, and are replayed by a new run of the same task
//! (at-least-once), except that a coalescing generation is only replayed if it
//! is the newest for its key. An ephemeral task's lost run is not replayed, and a
//! non-retriable task's running run is orphaned instead.

use crate::support::scheduler::Fixture;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    ClaimRejection, Completion, ReportRejection, Submission,
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
fn what_a_lost_worker_leaves_behind() {
    let mut fixture = Fixture::leading();
    let held = |fixture: &mut Fixture, submission: Submission, started: bool| {
        let task = fixture.scheduler.submit(submission).unwrap();
        let run = if started {
            running(fixture, &worker("w1"), &task)
        } else {
            let claim = fixture
                .scheduler
                .request_claim(&worker("w1"), &task)
                .unwrap();
            claim.task_run_id
        };
        (task, run)
    };
    // (task, run, state the run ends in, whether a new attempt replays it)
    let rows = vec![
        (held(&mut fixture, plain("p"), false), TaskRunState::Lost, true),
        (held(&mut fixture, plain("p"), true), TaskRunState::Lost, true),
        (
            held(&mut fixture, plain("p").ephemeral(), false),
            TaskRunState::Lost,
            false,
        ),
        (
            held(&mut fixture, plain("p").ephemeral(), true),
            TaskRunState::Lost,
            false,
        ),
        (
            held(&mut fixture, plain("p").non_retriable(), false),
            TaskRunState::Lost,
            true,
        ),
        (
            held(&mut fixture, plain("p").non_retriable(), true),
            TaskRunState::Orphaned,
            false,
        ),
    ];
    // Runs the loss must not touch: another worker's, a finished one, and one
    // nobody has claimed.
    let theirs = fixture.scheduler.submit(plain("b")).unwrap();
    let their_run = running(&mut fixture, &worker("w2"), &theirs);
    let (done, done_run) = held(&mut fixture, plain("c"), true);
    fixture
        .scheduler
        .complete(&worker("w1"), &done_run, Digest::blake3(b"d"), Completion::Final)
        .unwrap();
    let waiting = fixture.scheduler.submit(plain("e")).unwrap();
    assert_eq!(fixture.spy.memory_in_use(), 8);

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert_eq!(lost.len(), rows.len(), "only the lost worker's held runs");
    for ((task, run), state, replayed) in rows {
        let outcome = lost
            .iter()
            .find(|outcome| outcome.task_run_id == run)
            .unwrap_or_else(|| panic!("{run:?} was not reported lost"));
        assert_eq!(outcome.state, state, "{run:?}");
        assert_eq!(fixture.run_state(&run), state, "{run:?}");
        assert_eq!(outcome.replayed.is_some(), replayed, "{run:?}");
        if let Some(replay) = &outcome.replayed {
            let next = fixture.scheduler.task_run(replay).unwrap();
            assert_eq!(next.current_state(), TaskRunState::Queued);
            assert_eq!(next.attempt_number(), 2);
            assert_eq!(next.parent_task_run_id(), Some(run.clone()));
            assert_eq!(fixture.state(&task), TaskRunState::Queued);
        } else {
            assert_eq!(fixture.scheduler.runs_of(&task), vec![run.clone()]);
        }
        // What the lost worker reports afterwards is not authoritative.
        assert_eq!(
            fixture
                .scheduler
                .complete(&worker("w1"), &run, Digest::blake3(b"d"), Completion::Final)
                .unwrap_err(),
            ReportRejection::NotAuthoritative
        );
    }
    // Memory counts the three replays, the other worker's run and the waiting
    // task; an orphaned or ephemeral run's task is over.
    assert_eq!(fixture.spy.memory_in_use(), 5);
    assert_eq!(fixture.run_state(&their_run), TaskRunState::Running);
    assert_eq!(fixture.run_state(&done_run), TaskRunState::Succeeded);
    assert_eq!(fixture.state(&done), TaskRunState::Succeeded);
    assert_eq!(fixture.state(&waiting), TaskRunState::Queued);
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
fn a_task_whose_continuation_is_running_is_not_lost_with_its_worker() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(generation("a")).unwrap();
    let run = running(&mut fixture, &worker("w1"), &task);
    fixture
        .scheduler
        .complete(&worker("w1"), &run, Digest::blake3(b"d"), Completion::Continues)
        .unwrap();

    let lost = fixture.scheduler.lose_worker(&worker("w1")).unwrap();

    assert!(lost.is_empty());
    assert_eq!(fixture.run_state(&run), TaskRunState::Succeeded);
}

