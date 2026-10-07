//! Scenario tests for leaving `NoQuorum` with no coordination authority, built
//! on the `Cluster` harness: the node waits for its peers to return, then
//! takes part in the election they hold or holds one itself.


use std::collections::BTreeSet;

use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use crate::support::harness::Cluster;
use crate::support::scenarios::run_out_cut_off_leaders_lease;

fn suspect_timeout() -> Duration {
    Duration::from_ticks(10)
}

fn tick_size() -> Duration {
    Duration::from_ticks(5)
}

/// Bootstraps `n` nodes and lets them elect a leader and settle.
fn settled_cluster(n: usize) -> (Cluster, WorkerId) {
    let mut cluster = Cluster::bootstrap(n, suspect_timeout());
    // Past every node's suspicion timeout, which no quiescence check
    // counts as activity before it fires.
    for _ in 0..3 {
        cluster.advance(tick_size());
    }
    cluster.run_until_quiescent(tick_size(), 60);
    let leader = cluster.leader().expect("the cluster must elect a leader");
    (cluster, leader)
}

/// Panics unless exactly one node leads and every other is `Active`.
fn assert_settled(cluster: &Cluster) {
    let states = cluster.states();
    let leaders = states
        .values()
        .filter(|state| **state == WorkerState::Leader)
        .count();
    assert_eq!(leaders, 1, "{states:?}");
    assert!(
        states
            .values()
            .all(|state| matches!(state, WorkerState::Leader | WorkerState::Active)),
        "{states:?}"
    );
}

#[test]
fn a_leader_stranded_by_a_stall_in_its_acks_leaves_no_quorum_once_they_flow_again() {
    let (mut cluster, leader) = settled_cluster(3);

    // Every message is lost for as long as it takes the leader's lease to
    // run out, but not so long that a follower suspects it.
    cluster.network().set_drop_rate(1.0);
    run_out_cut_off_leaders_lease(&mut cluster);
    assert_eq!(
        cluster.states()[&leader],
        WorkerState::NoQuorum,
        "setup invariant"
    );
    cluster.network().set_drop_rate(0.0);

    cluster.run_until_quiescent(tick_size(), 60);

    assert_settled(&cluster);
}

#[test]
fn a_shard_whose_every_node_lost_its_quorum_elects_a_leader_once_its_peers_return() {
    let (mut cluster, _) = settled_cluster(4);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();

    // Two against two: no side holds a quorum of 3. The leader's lease runs
    // out, and every roll call on either side closes short of a quorum.
    cluster.partition(
        ids[..2].iter().cloned().collect(),
        ids[2..].iter().cloned().collect(),
    );
    let mut lost_quorum = BTreeSet::new();
    for _ in 0..40 {
        cluster.advance(tick_size());
        cluster.assert_at_most_one_in_leader_state();
        for (id, state) in cluster.states() {
            if state == WorkerState::NoQuorum {
                lost_quorum.insert(id);
            }
        }
    }
    assert_eq!(cluster.leader(), None, "{:?}", cluster.states());
    assert_eq!(
        lost_quorum,
        cluster.node_ids(),
        "every node must have gone NoQuorum"
    );

    cluster.heal();
    for _ in 0..3 {
        cluster.advance(tick_size());
    }
    cluster.run_until_quiescent(tick_size(), 60);

    assert_settled(&cluster);
}
