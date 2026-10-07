//! A leader's answers in the simulator: released once a quorum of each
//! record's placement has stored it within the lease, `NotLeader` when the
//! lease ends between the write and its acknowledgement.

use std::collections::BTreeSet;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::Submission;
use kabudachi_core::time::Duration;

use crate::support::harness::{Answer, Cluster};

const SUSPECT: Duration = Duration::from_millis(1_000);
const STEP: Duration = Duration::from_millis(50);

fn plain() -> Submission {
    Submission::new(
        TaskDefinitionId::new("billing.charge"),
        0,
        b"in".to_vec(),
        "default",
    )
}

fn elected() -> (Cluster, WorkerId) {
    let mut cluster = Cluster::bootstrap(3, SUSPECT);
    // A fresh cluster is a fixed point until its first election starts,
    // which it does once its voters suspect there is no leader.
    for _ in 0..(SUSPECT.as_ticks() * 2 / STEP.as_ticks()) {
        cluster.advance(STEP);
    }
    cluster.run_until_quiescent(STEP, 200);
    let leader = cluster.leader().expect("three voters elect a leader");
    (cluster, leader)
}

fn submitted(cluster: &mut Cluster, leader: &WorkerId) -> TaskId {
    let ticket = cluster.submit(leader, plain());
    cluster.advance(STEP);
    match cluster.answer(ticket) {
        Some(Answer::Submitted(task)) => task.clone(),
        other => panic!("expected the submission to be acknowledged, got {other:?}"),
    }
}

/// Claims and starts `task` on `claimant` with acknowledgements flowing at
/// once, and returns the run.
fn running(
    cluster: &mut Cluster,
    leader: &WorkerId,
    claimant: &WorkerId,
    task: &TaskId,
) -> TaskRunId {
    let claim = cluster.claim(leader, claimant, task);
    cluster.advance(STEP);
    let Some(Answer::Claimed(claim)) = cluster.answer(claim).cloned() else {
        panic!("the claim was not answered");
    };
    let started = cluster.start(leader, claimant, &claim.task_run_id);
    cluster.advance(STEP);
    assert_eq!(cluster.answer(started), Some(&Answer::Started));
    claim.task_run_id
}

/// Cuts `leader` off from the rest and holds every acknowledgement of a write
/// made from now on back longer than its lease can last.
fn depose_before_the_ack(cluster: &mut Cluster, leader: &WorkerId) {
    cluster
        .records()
        .set_ack_delay(Duration::from_ticks(SUSPECT.as_ticks() * 4));
    let rest: BTreeSet<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| id != leader)
        .collect();
    cluster.partition(BTreeSet::from([leader.clone()]), rest);
}

fn run_past_the_lease(cluster: &mut Cluster) {
    for _ in 0..(SUSPECT.as_ticks() * 5 / STEP.as_ticks()) {
        cluster.advance(STEP);
    }
}

#[test]
fn a_claim_whose_acknowledgement_arrives_after_the_lease_ended_is_answered_not_leader() {
    let (mut cluster, leader) = elected();
    let task = submitted(&mut cluster, &leader);
    // The write lands on every holder at once, but its acknowledgement
    // takes longer than the leader's lease can outlive losing its quorum.
    cluster
        .records()
        .set_ack_delay(Duration::from_ticks(SUSPECT.as_ticks() * 4));
    let claimant = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader)
        .unwrap();

    let ticket = cluster.claim(&leader, &claimant, &task);
    let rest: BTreeSet<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    cluster.partition(BTreeSet::from([leader.clone()]), rest);
    for _ in 0..(SUSPECT.as_ticks() * 5 / STEP.as_ticks()) {
        cluster.advance(STEP);
    }

    assert_eq!(cluster.answer(ticket), Some(&Answer::NotLeader));
    assert!(
        !cluster.holds_valid_grant(&leader),
        "its lease had ended when the acknowledgement came"
    );
}

#[test]
fn a_claim_acknowledged_at_the_very_instant_its_lease_ends_is_answered_not_leader() {
    let (mut cluster, leader) = elected();
    let task = submitted(&mut cluster, &leader);
    let claimant = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader)
        .unwrap();
    // Cut the leader off so its lease is not renewed, then make the write's
    // acknowledgement fall due exactly when the lease ends: no earlier step,
    // catch-up or wake has found the lease over for the held call.
    let rest: BTreeSet<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    cluster.partition(BTreeSet::from([leader.clone()]), rest);
    // The write itself still reaches a quorum: only the lease is lost.
    cluster.records().heal();
    let lease_end = cluster
        .scheduler_mut(&leader)
        .lease_end()
        .expect("the elected leader holds a bounded lease");
    cluster.records().set_ack_delay(lease_end - cluster.now());

    let ticket = cluster.claim(&leader, &claimant, &task);
    assert_eq!(cluster.answer(ticket), None, "held until the write is acknowledged");
    cluster.advance(lease_end - cluster.now());

    assert_eq!(cluster.now(), lease_end);
    assert!(
        cluster.records().held_by(&claimant, &task).is_some_and(|r| {
            r.runs[0].current_state() == TaskRunState::Claimed
        }),
        "the write was stored at a quorum, so only the lease check can refuse the answer"
    );
    assert_eq!(cluster.answer(ticket), Some(&Answer::NotLeader));
}

#[test]
fn a_submission_whose_write_missed_its_quorum_is_answered_not_leader_while_the_leader_leads() {
    let (mut cluster, leader) = elected();
    for holder in cluster.node_ids().into_iter().filter(|id| *id != leader) {
        cluster.records().set_up(&holder, false);
    }

    let ticket = cluster.submit(&leader, plain());
    cluster.advance(STEP);

    assert_eq!(cluster.answer(ticket), Some(&Answer::NotLeader));
    assert!(cluster.holds_valid_grant(&leader), "the lease did not end");
}

#[test]
fn a_certification_whose_acknowledgement_arrives_after_the_lease_ended_is_answered_not_leader() {
    let (mut cluster, leader) = elected();
    let task = submitted(&mut cluster, &leader);
    let claimant = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader)
        .unwrap();
    let run = running(&mut cluster, &leader, &claimant, &task);

    depose_before_the_ack(&mut cluster, &leader);
    let ticket = cluster.complete(&leader, &claimant, &run, Digest::blake3(b"result"));
    run_past_the_lease(&mut cluster);

    assert_eq!(cluster.answer(ticket), Some(&Answer::NotLeader));
}
