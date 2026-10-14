//! A task submitted with a client: the scheduler records whether each
//! run's result reached that client, answers how the task stands, and, when
//! the task ends, leaves exactly one conclusion for the client, whatever
//! ended it. A retry, a replay or a task nobody awaits concludes nothing.

use std::collections::{BTreeMap, BTreeSet};

use crate::support::scheduler::{Fixture, newest_records, ticks};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::reconcile::Rebuild;
use kabudachi_core::scheduler::{Completion, OutcomeRejection, Submission};

fn client() -> WorkerId {
    WorkerId::new("client")
}

fn runner() -> WorkerId {
    WorkerId::new("runner")
}

fn awaited(input: &str) -> Submission {
    Submission::new(
        TaskDefinitionId::new("billing.charge"),
        0,
        input.as_bytes().to_vec(),
        "default",
    )
    .with_client(client())
}

fn running(fixture: &mut Fixture, task: &TaskId) -> TaskRunId {
    let claim = fixture.scheduler.request_claim(&runner(), task).unwrap();
    fixture
        .scheduler
        .report_started(&runner(), &claim.task_run_id, None)
        .unwrap();
    claim.task_run_id
}

fn concluded(fixture: &mut Fixture) -> Vec<(TaskId, TaskRunState)> {
    let mut concluded: Vec<_> = fixture
        .scheduler
        .take_conclusions()
        .into_iter()
        .map(|conclusion| {
            assert_eq!(conclusion.client, client());
            assert!(conclusion.outcome.finished, "{conclusion:?}");
            (conclusion.outcome.task_id, conclusion.outcome.state)
        })
        .collect();
    concluded.sort_by(|(left, _), (right, _)| left.cmp(right));
    concluded
}

#[test]
fn a_task_with_a_client_concludes_to_it_once_however_it_ends() {
    let mut fixture = Fixture::leading();
    a_certified_task_concludes_with_its_delivered_result(&mut fixture);
    every_other_ending_concludes_once_in_its_runs_state(&mut fixture);
    a_retry_and_an_unawaited_task_conclude_nothing(&mut fixture);
}

/// Certified, its result delivered first: the run, its newest revision and
/// the conclusion all say so.
fn a_certified_task_concludes_with_its_delivered_result(fixture: &mut Fixture) {
    let certified = fixture.scheduler.submit(awaited("certified")).unwrap();
    let run = running(fixture, &certified);
    let digest = Digest::blake3(b"result");
    fixture
        .scheduler
        .complete_delivered(&runner(), &run, digest.clone(), Completion::Final, true)
        .unwrap();
    let [conclusion]: [_; 1] = fixture
        .scheduler
        .take_conclusions()
        .try_into()
        .expect("one conclusion");
    assert_eq!(conclusion.client, client());
    assert_eq!(
        (conclusion.outcome.state, conclusion.outcome.result_digest, conclusion.outcome.delivered),
        (TaskRunState::Succeeded, Some(digest), true)
    );
    let record = newest_records(fixture)
        .into_iter()
        .find(|record| record.task.as_ref().unwrap().task_id() == certified)
        .unwrap();
    assert_eq!(record.task.as_ref().unwrap().client(), Some(client()));
    assert!(record.runs.last().unwrap().delivered);
}

/// Every other way a task ends: each concludes once, in its run's state.
fn every_other_ending_concludes_once_in_its_runs_state(fixture: &mut Fixture) {
    let failed = fixture.scheduler.submit(awaited("failed")).unwrap();
    let run = running(fixture, &failed);
    fixture.scheduler.fail(&runner(), &run, "ValueError").unwrap();
    let expired = fixture
        .scheduler
        .submit(awaited("expired").with_delay(ticks(10)).with_expiry(ticks(5)))
        .unwrap();
    fixture.clock.advance(ticks(20));
    fixture.scheduler.catch_up();
    let cancelled = fixture.scheduler.submit(awaited("cancelled")).unwrap();
    fixture.scheduler.cancel(&cancelled).unwrap();
    let older = fixture
        .scheduler
        .submit(awaited("older").with_coalescing_key("k"))
        .unwrap();
    fixture
        .scheduler
        .submit(awaited("newer").with_coalescing_key("k"))
        .unwrap();
    let lost = fixture.scheduler.submit(awaited("lost").ephemeral()).unwrap();
    running(fixture, &lost);
    let orphaned = fixture
        .scheduler
        .submit(awaited("orphaned").non_retriable())
        .unwrap();
    running(fixture, &orphaned);
    fixture.lose_silent(&runner());

    let mut expected = vec![
        (failed, TaskRunState::Failed),
        (expired, TaskRunState::Expired),
        (cancelled, TaskRunState::Cancelled),
        (older, TaskRunState::Superseded),
        (lost, TaskRunState::Lost),
        (orphaned, TaskRunState::Orphaned),
    ];
    expected.sort_by(|(left, _), (right, _)| left.cmp(right));
    assert_eq!(concluded(fixture), expected);
}

/// A failure with a retry left, and a task nobody awaits, conclude nothing.
fn a_retry_and_an_unawaited_task_conclude_nothing(fixture: &mut Fixture) {
    let retried = fixture
        .scheduler
        .submit(awaited("retried").with_retries(1))
        .unwrap();
    let run = running(fixture, &retried);
    fixture.scheduler.fail(&runner(), &run, "ValueError").unwrap();
    let unawaited = fixture
        .scheduler
        .submit(Submission::new(TaskDefinitionId::new("billing.charge"), 0, b"x".to_vec(), "default"))
        .unwrap();
    let run = running(fixture, &unawaited);
    fixture
        .scheduler
        .complete(&runner(), &run, Digest::blake3(b"x"), Completion::Final)
        .unwrap();
    assert_eq!(concluded(fixture), vec![]);
}

#[test]
fn the_outcome_of_a_task_is_its_current_run_and_only_a_leader_answers() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(awaited("retried").with_retries(1))
        .unwrap();
    let first = running(&mut fixture, &task);
    let retry = fixture
        .scheduler
        .fail(&runner(), &first, "ValueError")
        .unwrap()
        .retry
        .expect("one retry left");

    let outcome = fixture.scheduler.outcome(&task).unwrap().expect("the leader holds it");
    assert_eq!(
        (outcome.task_run_id, outcome.state, outcome.finished),
        (retry.clone(), TaskRunState::Queued, false)
    );
    let run = running(&mut fixture, &task);
    fixture
        .scheduler
        .complete_delivered(&runner(), &run, Digest::blake3(b"done"), Completion::Final, false)
        .unwrap();
    let outcome = fixture.scheduler.outcome(&task).unwrap().unwrap();
    assert_eq!(
        (outcome.state, outcome.finished, outcome.delivered),
        (TaskRunState::Succeeded, true, false)
    );
    assert_eq!(fixture.scheduler.outcome(&TaskId::new("never-submitted")), Ok(None));
    // A task whose newest record this leader does not know yet is held back,
    // and asked about again later.
    let uncertain = TaskId::new("uncertain");
    fixture
        .scheduler
        .adopt(Rebuild {
            uncertain: BTreeMap::from([(uncertain.clone(), BTreeSet::new())]),
            ..Rebuild::default()
        })
        .unwrap();
    assert_eq!(
        fixture.scheduler.outcome(&uncertain),
        Err(OutcomeRejection::NotReady)
    );
    assert_eq!(
        Fixture::not_leading().scheduler.outcome(&task),
        Err(OutcomeRejection::NotLeader)
    );
}
