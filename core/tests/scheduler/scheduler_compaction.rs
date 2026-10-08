//! A key whose waiting chain grows past its threshold gets a compaction run
//! that a worker folds; the fold replaces the chain's front only if the chain
//! still starts with what was folded, so no payload is lost or folded twice,
//! and the newest generation folds what is left.

use std::collections::BTreeSet;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::chain_entry;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    COMPACTION_SOFT_BYTES, Claim, ClaimRejection, Completion, Event, MAX_CLAIM_FRAME_BYTES,
    MAX_SUBMISSION_BYTES, MemoryLimits, ReportRejection, Submission, SubmitRejection,
};

use crate::support::scheduler::Fixture;

fn runner() -> WorkerId {
    WorkerId::new("runner")
}

fn holder_worker() -> WorkerId {
    WorkerId::new("w1")
}

fn generation(payload: &[u8]) -> Submission {
    Submission::new(
        TaskDefinitionId::new("index.refresh"),
        0,
        payload.to_vec(),
        "default",
    )
    .with_coalescing_key("k")
}

/// Not associative, so a fold in the wrong order or grouping shows.
fn merge(older: &[u8], newer: &[u8]) -> Vec<u8> {
    [b"(".as_slice(), older, b">", newer, b")"].concat()
}

fn fold_all(payloads: &[Vec<u8>]) -> Vec<u8> {
    payloads[1..]
        .iter()
        .fold(payloads[0].clone(), |folded, next| merge(&folded, next))
}

fn payloads_of(letters: std::ops::Range<u8>) -> Vec<Vec<u8>> {
    letters.map(|n| vec![b'a' + n; 80]).collect()
}

/// A leader with a compaction runner and a running generation holding the
/// key, so later generations wait and build a chain.
struct Held {
    fixture: Fixture,
    holder_run: TaskRunId,
}

fn held_with(limits: Option<MemoryLimits>, runners: bool) -> Held {
    let mut fixture = Fixture::leading();
    fixture.scheduler.set_memory_limits(limits);
    if runners {
        fixture
            .scheduler
            .set_compaction_runners(BTreeSet::from([runner()]));
    }
    let holder = fixture.scheduler.submit(generation(b"h")).unwrap();
    let claim = fixture
        .scheduler
        .request_claim(&holder_worker(), &holder)
        .unwrap();
    fixture
        .scheduler
        .report_started(&holder_worker(), &claim.task_run_id)
        .unwrap();
    Held {
        fixture,
        holder_run: claim.task_run_id,
    }
}

fn held_key() -> Held {
    held_with(
        Some(MemoryLimits {
            soft: 300,
            hard: 10_000,
        }),
        true,
    )
}

impl Held {
    fn submit(&mut self, payload: &[u8]) -> TaskId {
        self.fixture.scheduler.submit(generation(payload)).unwrap()
    }

    fn submit_all(&mut self, payloads: &[Vec<u8>]) -> TaskId {
        payloads
            .iter()
            .map(|payload| self.submit(payload))
            .last()
            .unwrap()
    }

    fn compaction_claim(&mut self) -> Option<Claim> {
        self.fixture
            .scheduler
            .claim_oldest(&runner(), 10)
            .unwrap()
            .into_iter()
            .find(|claim| claim.task.compacts.is_some())
    }

    fn finish_holder(&mut self) {
        self.fixture
            .scheduler
            .complete(
                &holder_worker(),
                &self.holder_run,
                Digest::blake3(b"d"),
                Completion::Final,
            )
            .unwrap();
    }

    /// What the key's newest generation is handed when it is claimed, the
    /// holder being done: its chain, then its own payload.
    fn newest_inputs(&mut self) -> Vec<Vec<u8>> {
        self.finish_holder();
        let claim = self
            .fixture
            .scheduler
            .claim_oldest(&WorkerId::new("w2"), 1)
            .unwrap()
            .remove(0);
        let mut inputs = claim.chain.clone();
        inputs.push(claim.task.serialized_input.clone());
        inputs
    }
}

#[test]
fn a_chain_past_the_soft_limit_is_folded_by_a_compaction_and_the_newest_folds_the_rest_in_order() {
    let mut held = held_key();
    let payloads = payloads_of(0..6);
    let newest = held.submit_all(&payloads);
    let compaction = held
        .compaction_claim()
        .expect("past the soft limit a compaction is queued");
    assert!(compaction.chain.len() >= 2, "a compaction folds at least two entries");
    assert!(compaction.task.serialized_input.is_empty());

    let folded = fold_all(&compaction.chain);
    let done = held
        .fixture
        .scheduler
        .complete_compaction(&runner(), &compaction.task_run_id, folded)
        .unwrap();

    assert!(done.applied);
    assert_eq!(held.fixture.state(&compaction.task.task_id()), TaskRunState::Succeeded);
    let published = held.fixture.spy.newest_revision_of(&newest).unwrap();
    assert!(
        matches!(
            published.retained_chain.first().and_then(|entry| entry.entry.as_ref()),
            Some(chain_entry::Entry::Folded(_))
        ),
        "the waiting generation's record carries the fold"
    );
    assert_eq!(
        fold_all(&held.newest_inputs()),
        fold_all(&payloads),
        "the fold of a folded prefix and the rest is the fold of all"
    );
}

#[test]
fn a_chain_past_the_per_key_threshold_is_compacted_without_any_memory_limit() {
    let mut held = held_with(None, true);
    let size = (COMPACTION_SOFT_BYTES / 3 + 1) as usize;
    for letter in b'a'..b'e' {
        held.submit(&vec![letter; size]);
    }

    assert!(held.compaction_claim().is_some());
}

#[test]
fn only_a_claimed_compaction_holds_the_newest_generation_back() {
    let mut queued = held_key();
    let newest = queued.submit_all(&payloads_of(0..6));
    queued.finish_holder();

    assert!(
        queued
            .fixture
            .scheduler
            .request_claim(&WorkerId::new("w2"), &newest)
            .is_ok(),
        "a queued compaction does not hold it"
    );
    assert!(
        queued.compaction_claim().is_none(),
        "the queued compaction has nothing left to fold: the newest folds the whole chain"
    );

    let mut claimed = held_key();
    let newest = claimed.submit_all(&payloads_of(0..6));
    claimed.compaction_claim().unwrap();
    claimed.finish_holder();
    assert_eq!(
        claimed
            .fixture
            .scheduler
            .request_claim(&WorkerId::new("w2"), &newest),
        Err(ClaimRejection::KeyBusy),
        "a claimed one does"
    );
}

#[test]
fn a_supersession_during_a_compaction_keeps_every_payload_folded_once() {
    let mut held = held_key();
    let mut payloads = payloads_of(0..6);
    held.submit_all(&payloads);
    let compaction = held.compaction_claim().unwrap();
    let late = vec![b'z'; 80];
    held.submit(&late);
    payloads.push(late);

    let done = held
        .fixture
        .scheduler
        .complete_compaction(&runner(), &compaction.task_run_id, fold_all(&compaction.chain))
        .unwrap();

    assert!(done.applied, "the folded entries are still the front of the chain");
    assert_eq!(fold_all(&held.newest_inputs()), fold_all(&payloads), "nothing lost, nothing folded twice");
}

#[test]
fn a_fold_whose_prefix_was_dropped_meanwhile_is_discarded() {
    let mut held = held_with(
        Some(MemoryLimits {
            soft: 300,
            hard: 600,
        }),
        true,
    );
    let dropping = |payload: &[u8]| generation(payload).with_drop_oldest();
    for payload in payloads_of(0..5) {
        held.fixture.scheduler.submit(dropping(&payload)).unwrap();
    }
    let compaction = held.compaction_claim().unwrap();
    // Past the hard limit: the chain's oldest payloads are dropped.
    held.fixture
        .scheduler
        .submit(dropping(&[b'y'; 300]))
        .unwrap();

    let done = held
        .fixture
        .scheduler
        .complete_compaction(&runner(), &compaction.task_run_id, fold_all(&compaction.chain))
        .unwrap();

    assert!(!done.applied, "the chain no longer starts with what was folded");
    let inputs = held.newest_inputs();
    assert!(
        inputs.iter().all(|input| input.len() == 80 || input.len() == 300),
        "the discarded fold is nowhere in what the newest generation folds: {inputs:?}"
    );
}

#[test]
fn a_fold_whose_waiting_generation_was_cancelled_meanwhile_is_discarded() {
    let mut held = held_key();
    let newest = held.submit_all(&payloads_of(0..6));
    let compaction = held.compaction_claim().unwrap();
    held.fixture.scheduler.cancel(&newest).unwrap();

    let done = held
        .fixture
        .scheduler
        .complete_compaction(&runner(), &compaction.task_run_id, fold_all(&compaction.chain))
        .unwrap();

    assert!(!done.applied);
    assert_eq!(held.fixture.state(&compaction.task.task_id()), TaskRunState::Succeeded);
}

#[test]
fn with_no_runner_no_compaction_is_made() {
    let mut held = held_with(
        Some(MemoryLimits {
            soft: 300,
            hard: 10_000,
        }),
        false,
    );
    held.submit_all(&payloads_of(0..6));

    assert!(
        held.fixture
            .spy
            .revisions()
            .iter()
            .all(|record| record.task.as_ref().is_some_and(|task| task.compacts.is_none()))
    );
}

#[test]
fn a_lost_compaction_is_never_replayed_and_does_not_count_as_a_loss() {
    let mut held = held_key();
    held.submit_all(&payloads_of(0..6));
    let compaction = held.compaction_claim().unwrap();

    let lost = held.fixture.lose_silent(&runner());

    let run = lost
        .iter()
        .find(|run| run.task_run_id == compaction.task_run_id)
        .expect("the compaction run was lost");
    assert_eq!((run.state, run.replayed.clone()), (TaskRunState::Lost, None));
    assert_eq!(
        held.fixture
            .scheduler
            .request_claim(&runner(), &compaction.task.task_id()),
        Err(ClaimRejection::Finished)
    );
    assert_eq!(held.fixture.scheduler.runs_of(&compaction.task.task_id()).len(), 1);
}

#[test]
fn a_failed_compaction_is_not_made_again_until_the_waiting_generation_changes() {
    let mut held = held_key();
    held.submit_all(&payloads_of(0..6));
    let compaction = held.compaction_claim().unwrap();

    held.fixture
        .scheduler
        .fail(&runner(), &compaction.task_run_id, "ValueError")
        .unwrap();
    assert!(held.compaction_claim().is_none(), "a merge that failed would fail again");
    // Asking again for the same waiting generation (a runner coming and going)
    // does not make another.
    held.fixture.scheduler.set_compaction_runners(BTreeSet::new());
    held.fixture.scheduler.set_compaction_runners(BTreeSet::from([runner()]));
    assert!(held.compaction_claim().is_none(), "a merge that failed would fail again");

    held.submit(&[b'q'; 80]);
    assert!(held.compaction_claim().is_some(), "a new generation makes a new attempt");
}

#[test]
fn only_a_runner_may_claim_a_compaction() {
    let mut held = held_key();
    held.submit_all(&payloads_of(0..6));
    let queued = held
        .fixture
        .spy
        .revisions()
        .into_iter()
        .find_map(|record| record.task.filter(|task| task.compacts.is_some()))
        .expect("a compaction was made")
        .task_id();

    assert!(
        held.fixture
            .scheduler
            .claim_oldest(&WorkerId::new("other"), 10)
            .unwrap()
            .is_empty(),
        "a batch claim skips it for a worker that does not run compaction"
    );
    assert_eq!(
        held.fixture
            .scheduler
            .request_claim(&WorkerId::new("other"), &queued),
        Err(ClaimRejection::CannotRun)
    );
}

#[test]
fn a_submission_that_would_make_its_keys_claim_too_large_to_carry_is_refused() {
    // No runner, so nothing compacts the chain.
    let mut held = held_with(None, false);
    let chunk = vec![7; (MAX_SUBMISSION_BYTES / 3) as usize];
    let mut accepted = 0;
    let mut refused = None;
    for _ in 0..4 {
        match held.fixture.scheduler.submit(generation(&chunk)) {
            Ok(_) => accepted += 1,
            Err(rejection) => {
                refused = Some(rejection);
                break;
            }
        }
    }

    assert!(
        matches!(refused, Some(SubmitRejection::KeyBackpressure { .. })),
        "{refused:?}"
    );
    assert_eq!(accepted, 2);
    let inputs = held.newest_inputs();
    assert!(
        inputs.iter().map(Vec::len).sum::<usize>() as u64 <= MAX_SUBMISSION_BYTES,
        "the newest generation still fits one claim and is handed out"
    );
}

#[test]
fn a_fold_too_large_to_carry_fails_the_newest_generation_and_frees_its_key() {
    let mut held = held_key();
    let newest = held.submit_all(&payloads_of(0..6));
    let compaction = held.compaction_claim().unwrap();

    let huge = vec![0; MAX_CLAIM_FRAME_BYTES as usize];
    held.fixture
        .scheduler
        .complete_compaction(&runner(), &compaction.task_run_id, huge)
        .unwrap();

    let run = held.fixture.scheduler.runs_of(&newest)[0].clone();
    assert!(held.fixture.scheduler.take_events().contains(&Event::CoalescedPayloadTooLarge {
        task_id: newest.clone(),
        task_run_id: run.clone(),
    }));
    assert_eq!(held.fixture.state(&newest), TaskRunState::Failed);
    assert_eq!(
        held.fixture.scheduler.task_run(&run).unwrap().failure_kind,
        kabudachi_core::scheduler::COALESCED_PAYLOAD_TOO_LARGE
    );
    assert!(
        held.fixture.scheduler.submit(generation(b"again")).is_ok(),
        "the key takes submissions again"
    );
}

#[test]
fn an_ordinary_completion_of_a_compaction_run_is_refused() {
    let mut held = held_key();
    held.submit_all(&payloads_of(0..6));
    let compaction = held.compaction_claim().unwrap();
    held.fixture.scheduler.report_started(&runner(), &compaction.task_run_id).unwrap();

    let refused = held.fixture.scheduler.complete(
        &runner(),
        &compaction.task_run_id,
        Digest::blake3(b"d"),
        Completion::Final,
    );

    assert_eq!(refused, Err(ReportRejection::NotAuthoritative));
    assert_eq!(held.fixture.run_state(&compaction.task_run_id), TaskRunState::Running);
}
