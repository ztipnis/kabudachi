//! Scenario test for an election after a refused false suspicion, built on
//! the `Cluster` harness.

use crate::support::harness::Cluster;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;

/// Bootstraps 3 nodes and lets them elect a leader: the first to suspect
/// after its jittered suspicion timeout wins. Returns `(cluster, leader,
/// followers)` fully settled.
///
/// Not inside a partition: a node cut off during the election misses the
/// winning roll call, so it is not in the leader's roster, and its
/// confirmations never count toward the leader's lease until an election
/// founds a configuration that admits it. The scenarios below then cut one
/// follower off, and the leader of 3 must keep its quorum with the other.
fn bootstrap_and_elect_leader_n3(
    suspect_timeout: Duration,
    tick_size: Duration,
) -> (Cluster, WorkerId, Vec<WorkerId>) {
    let mut cluster = Cluster::bootstrap(3, suspect_timeout);

    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    let iterations = cluster.run_until_quiescent(tick_size, 60);
    assert!(
        iterations < 60,
        "expected quiescence well before max_ticks, ran all {iterations}"
    );

    let leader = cluster.leader().expect("the three must elect a leader");
    let followers: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    assert_eq!(followers.len(), 2);
    for follower in &followers {
        assert_eq!(
            cluster.states()[follower],
            WorkerState::Active,
            "setup invariant"
        );
    }

    (cluster, leader, followers)
}

// A false suspicion refused, then a real leader loss: f1's refused roll call
// took a term, but no node keeps contesting it. f1 is the lower `WorkerId`,
// so its call ranks better than any f2 makes for the same term, and f1
// passes over f2's; both must still move on to a later term and elect.
#[test]
fn a_refused_false_suspicion_then_a_real_leader_loss_still_elects() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let (mut cluster, leader, followers) =
        bootstrap_and_elect_leader_n3(suspect_timeout, tick_size);
    let (f1, f2) = (followers[0].clone(), followers[1].clone());

    // f1 alone loses its leader for a while: f2 refuses its roll call.
    cluster.partition(
        [f1.clone()].into_iter().collect(),
        [leader.clone()].into_iter().collect(),
    );
    for _ in 0..4 {
        cluster.advance(tick_size);
    }
    assert_eq!(
        cluster.states()[&leader],
        WorkerState::Leader,
        "setup invariant: the leader is never challenged"
    );
    assert_eq!(
        cluster.states()[&f2],
        WorkerState::Active,
        "setup invariant: f2 kept receiving real heartbeats throughout"
    );
    assert!(
        matches!(
            cluster.states()[&f1],
            WorkerState::RollCall | WorkerState::NoQuorum
        ),
        "f1 must have timed out and begun its own roll call: {:?}",
        cluster.states()
    );
    cluster.heal();
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    assert_eq!(cluster.leader(), Some(leader.clone()), "setup invariant");
    assert_eq!(
        cluster.states()[&f1],
        WorkerState::Active,
        "setup invariant"
    );

    // Now the leader is lost for real.
    cluster.partition(
        [leader.clone()].into_iter().collect(),
        [f1.clone(), f2.clone()].into_iter().collect(),
    );
    for _ in 0..20 {
        cluster.advance(tick_size);
    }

    let new_leader = cluster
        .leader()
        .filter(|id| *id != leader)
        .unwrap_or_else(|| panic!("f1 and f2 must elect a leader: {:?}", cluster.states()));
    let follower = if new_leader == f1 { &f2 } else { &f1 };
    assert_eq!(cluster.states()[follower], WorkerState::Active);
}
