//! Property tests for the election state machine (README §25.2): random
//! sequences of advance, partition, heal and drain events against a 3-node
//! `Cluster`, checking invariants L1-L5 after every event.
//!
//! The cluster is always exactly 3 nodes: with 4 or more, simultaneously
//! suspecting nodes can elect two leaders in different terms (see
//! `scenario_partition_test.rs`), which would produce failures unrelated to
//! the invariants checked here.
//!
//! - L1: at most one node is `Leader`.
//! - L2: a node's `term()` never decreases.
//! - L3: a node's `recovery_epoch()` never decreases.
//! - L4: a node that has drained (reached `Stopped`) is never `Leader` later.
//!   `begin_drain()` is a no-op outside `Active`/`Leader`, so only genuine
//!   drains count.
//! - L5: a `Heal` alone changes no node's `term()` or `recovery_epoch()`.
//!
//! Not covered: leadership only with quorum (unit-tested against exact quorum
//! arithmetic instead) and lease-expiry fencing (not implemented).
//!
//! Events: `Advance` (1..=8 ticks), `Partition` (a random 2-way split of the
//! 3 nodes), `Heal` and `Drain` (a random node). Sequences are 1..100 events.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use proptest::prelude::*;
use support::harness::Cluster;

/// The 3 node IDs `Cluster::bootstrap(3, ..)` creates. Hardcoded because
/// events are generated before any `Cluster` exists.
fn known_worker_ids() -> [WorkerId; 3] {
    [
        WorkerId::new("worker-0"),
        WorkerId::new("worker-1"),
        WorkerId::new("worker-2"),
    ]
}

#[derive(Debug, Clone)]
enum ScenarioEvent {
    Advance(Duration),
    Partition(BTreeSet<WorkerId>, BTreeSet<WorkerId>),
    Heal,
    Drain(WorkerId),
}

fn worker_id_strategy() -> impl Strategy<Value = WorkerId> {
    let [w0, w1, w2] = known_worker_ids();
    prop_oneof![Just(w0), Just(w1), Just(w2)]
}

/// A random 2-way split of the 3 node IDs. It may be trivial (one side empty),
/// which blocks nothing and is harmless.
fn partition_strategy() -> impl Strategy<Value = (BTreeSet<WorkerId>, BTreeSet<WorkerId>)> {
    let ids = known_worker_ids();
    proptest::collection::vec(any::<bool>(), ids.len()).prop_map(move |assignment| {
        let mut group_a = BTreeSet::new();
        let mut group_b = BTreeSet::new();
        for (id, in_a) in ids.iter().zip(assignment.iter()) {
            if *in_a {
                group_a.insert(id.clone());
            } else {
                group_b.insert(id.clone());
            }
        }
        (group_a, group_b)
    })
}

fn scenario_event_strategy() -> impl Strategy<Value = ScenarioEvent> {
    prop_oneof![
        (1u64..=8).prop_map(|ticks| ScenarioEvent::Advance(Duration::from_ticks(ticks))),
        partition_strategy().prop_map(|(a, b)| ScenarioEvent::Partition(a, b)),
        Just(ScenarioEvent::Heal),
        worker_id_strategy().prop_map(ScenarioEvent::Drain),
    ]
}

proptest! {
    /// Runs a random event sequence on a fresh 3-node cluster, checking L1-L5 after every event.
    #[test]
    fn leadership_invariants_hold_after_every_event(
        events in proptest::collection::vec(scenario_event_strategy(), 1..100)
    ) {
        let suspect_timeout = Duration::from_ticks(10);
        let mut cluster = Cluster::bootstrap(3, suspect_timeout);
        let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
        prop_assert_eq!(
            ids.clone(),
            known_worker_ids().to_vec(),
            "sanity check: Cluster::bootstrap(3, ..)'s actual node IDs must match this file's \
             hardcoded known_worker_ids() (see harness.rs's naming-scheme doc comment) — if this \
             ever fails, the naming scheme changed and every strategy in this file needs updating"
        );

        let mut last_term: BTreeMap<WorkerId, u64> = ids.iter().cloned().map(|id| (id, 0)).collect();
        let mut last_epoch: BTreeMap<WorkerId, u64> = ids.iter().cloned().map(|id| (id, 0)).collect();
        let mut ever_drained: BTreeSet<WorkerId> = BTreeSet::new();

        for event in events {
            // L5: snapshot term and recovery_epoch before a Heal.
            let is_heal = matches!(event, ScenarioEvent::Heal);
            let before_heal: Option<BTreeMap<WorkerId, (u64, u64)>> = if is_heal {
                Some(
                    ids.iter()
                        .map(|id| {
                            let node = cluster.node(id);
                            (id.clone(), (node.term(), node.recovery_epoch()))
                        })
                        .collect(),
                )
            } else {
                None
            };

            match event {
                ScenarioEvent::Advance(dt) => cluster.advance(dt),
                ScenarioEvent::Partition(group_a, group_b) => cluster.partition(group_a, group_b),
                ScenarioEvent::Heal => cluster.heal(),
                ScenarioEvent::Drain(id) => {
                    cluster.drain(&id);
                    // Only a node that reached `Stopped` counts as drained for L4.
                    if cluster.states()[&id] == WorkerState::Stopped {
                        ever_drained.insert(id.clone());
                    }
                }
            }

            cluster.assert_at_most_one_leader();

            for id in &ids {
                let node = cluster.node(id);
                let term = node.term();
                let epoch = node.recovery_epoch();

                prop_assert!(
                    term >= last_term[id],
                    "L2 violated: node {id:?}'s term() decreased from {} to {term}",
                    last_term[id]
                );
                prop_assert!(
                    epoch >= last_epoch[id],
                    "L3 violated: node {id:?}'s recovery_epoch() decreased from {} to {epoch}",
                    last_epoch[id]
                );

                last_term.insert(id.clone(), term);
                last_epoch.insert(id.clone(), epoch);
            }

            let states = cluster.states();
            for drained_id in &ever_drained {
                prop_assert_ne!(
                    states[drained_id],
                    WorkerState::Leader,
                    "L4 violated: previously-drained node {:?} is now Leader",
                    drained_id
                );
            }

            if let Some(before) = before_heal {
                for id in &ids {
                    let node = cluster.node(id);
                    let (before_term, before_epoch) = before[id];
                    let new_term = node.term();
                    let new_epoch = node.recovery_epoch();
                    prop_assert_eq!(
                        new_term,
                        before_term,
                        "L5 violated: node {:?}'s term() changed from {} to {} across a bare Heal event",
                        id,
                        before_term,
                        new_term
                    );
                    prop_assert_eq!(
                        new_epoch,
                        before_epoch,
                        "L5 violated: node {:?}'s recovery_epoch() changed from {} to {} across a bare \
                         Heal event",
                        id,
                        before_epoch,
                        new_epoch
                    );
                }
            }
        }
    }
}
