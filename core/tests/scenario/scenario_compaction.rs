//! A key's chain, partly folded by a compaction, lives in the shard's
//! records: a new leader rebuilds it, and its newest generation's fold is the
//! fold of every payload in submission order.

use std::collections::BTreeSet;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::scheduler::{Claim, MemoryLimits, Submission};

use super::scenario_reconcile::{leader_loss, some_other};
use super::scenario_records::{STEP, elected_among, running, submitted_with};
use crate::support::harness::{Answer, Cluster};

/// Not associative, so a fold in the wrong order or grouping shows.
fn merge(older: &[u8], newer: &[u8]) -> Vec<u8> {
    [b"(".as_slice(), older, b">", newer, b")"].concat()
}

fn fold_all(payloads: &[Vec<u8>]) -> Vec<u8> {
    payloads[1..]
        .iter()
        .fold(payloads[0].clone(), |folded, next| merge(&folded, next))
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

/// The compaction run some node's records hold that is not finished.
fn compaction_task(cluster: &Cluster) -> TaskId {
    cluster
        .node_ids()
        .iter()
        .flat_map(|holder| cluster.records().held_records(holder))
        .filter(|record| !record.finished)
        .filter_map(|record| record.task)
        .find(|task| task.compacts.is_some())
        .expect("a compaction run was recorded")
        .task_id()
}

fn claimed(cluster: &mut Cluster, leader: &WorkerId, worker: &WorkerId, task: &TaskId) -> Claim {
    let ticket = cluster.claim(leader, worker, task);
    cluster.advance(STEP);
    match cluster.answer(ticket).cloned() {
        Some(Answer::Claimed(claim)) => claim,
        other => panic!("the claim was not granted: {other:?}"),
    }
}

#[test]
fn a_compacted_chain_folds_in_order_after_the_leader_that_compacted_it_is_lost() {
    let (mut cluster, leader) = elected_among(5);
    let runner = some_other(&cluster, &[&leader]);
    for id in cluster.node_ids() {
        let scheduler = cluster.scheduler_mut(&id);
        scheduler.set_compaction_runners(BTreeSet::from([runner.clone()]));
        scheduler.set_memory_limits(Some(MemoryLimits {
            soft: 300,
            hard: 10_000,
        }));
    }
    let holder = submitted_with(&mut cluster, &leader, generation(b"h"));
    let holder_run = running(&mut cluster, &leader, &runner, &holder);
    let payloads: Vec<Vec<u8>> = (0..6).map(|n| vec![b'a' + n; 80]).collect();
    let newest = payloads
        .iter()
        .map(|payload| submitted_with(&mut cluster, &leader, generation(payload)))
        .last()
        .unwrap();
    let compaction = compaction_task(&cluster);
    let claim = claimed(&mut cluster, &leader, &runner, &compaction);
    let folded = fold_all(&claim.chain);
    let ticket = cluster.complete_compaction(&leader, &runner, &claim.task_run_id, folded);
    cluster.advance(STEP);
    assert!(matches!(cluster.answer(ticket), Some(Answer::Compacted(done)) if done.applied));

    let held_bytes = cluster.scheduler_mut(&leader).memory_in_use();
    let next = leader_loss(&mut cluster, &leader);
    assert_eq!(
        cluster.scheduler_mut(&next).memory_in_use(),
        held_bytes,
        "the folded payload is counted when the new leader installs it"
    );

    let finished = cluster.complete(&next, &runner, &holder_run, Digest::blake3(b"out"));
    cluster.advance(STEP);
    assert!(matches!(cluster.answer(finished), Some(Answer::Certified(_))));
    let claimant = some_other(&cluster, &[&leader, &next, &runner]);
    let last = claimed(&mut cluster, &next, &claimant, &newest);
    let mut inputs = last.chain.clone();
    inputs.push(last.task.serialized_input.clone());
    assert_eq!(fold_all(&inputs), fold_all(&payloads));
}
