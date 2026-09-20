//! Tests of the `Cluster` simulation harness itself, including that a full
//! election cycle can be driven through the harness alone (test 3 uses
//! `Cluster::network()` to check message-level delivery and drops directly).

mod support;

use support::builders::worker;

use std::collections::BTreeSet;

use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, LeaderHeartbeatAck, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::harness::Cluster;

/// A `LeaderHeartbeatAck` message from `leader`, at epoch and term 0 so any node accepts it.
fn heartbeat_ack(shard_id: &str, leader: &WorkerId) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::HeartbeatAck(
            LeaderHeartbeatAck {
                shard_id: Some(ShardId::new(shard_id).into()),
                leader_id: Some(leader.clone().into()),
                recovery_epoch: 0,
                term: 0,
                membership_generation: 0,
            },
        )),
    }
}

#[test]
fn bootstrap_produces_exactly_n_distinct_active_registered_nodes() {
    let cluster = Cluster::bootstrap(3, Duration::from_ticks(50));

    let ids = cluster.node_ids();
    let expected: BTreeSet<WorkerId> = ["worker-0", "worker-1", "worker-2"]
        .iter()
        .map(|s| worker(s))
        .collect();
    assert_eq!(
        ids, expected,
        "bootstrap(3, ..) must produce exactly these 3 IDs"
    );

    let states = cluster.states();
    assert_eq!(states.len(), 3);
    for (id, state) in &states {
        assert_eq!(
            *state,
            WorkerState::Active,
            "node {id:?} must start Active — Phase 0 has no BOOTSTRAPPING/JOINING modeling"
        );
    }
}

// Tuning: `suspect_timeout` is 10 ticks and `tick_size` 5. The test first
// advances 15 ticks by hand because a freshly bootstrapped cluster is already a
// fixed point of `run_until_quiescent`, which cannot see a timer that hasn't
// fired; crossing the timeout (15 > 10) starts the election. The cluster has 3
// nodes because with 4 or more, simultaneous suspicion can elect two leaders in
// different terms (see `scenario_partition_test.rs`).
#[test]
fn full_election_cycle_converges_through_the_harness_alone() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let max_ticks = 60;

    let mut cluster = Cluster::bootstrap(3, suspect_timeout);

    // No leader exists, so no heartbeats arrive: that absence alone must
    // trigger suspicion, roll call, voting and a leader.
    for _ in 0..3 {
        cluster.advance(tick_size);
    }

    let iterations = cluster.run_until_quiescent(tick_size, max_ticks);
    assert!(
        iterations < max_ticks,
        "expected the cluster to reach a fixed point well before max_ticks, ran all {iterations}"
    );

    assert!(
        cluster.leader().is_some(),
        "expected a leader to have been elected after the cluster settled; states: {:?}",
        cluster.states()
    );
    cluster.assert_at_most_one_leader();
}

const SHARD: &str = "shard-1"; // Matches Cluster::bootstrap's own documented shard-1 scheme.

#[test]
fn partition_and_heal_are_correctly_wired_through() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let mut cluster = Cluster::bootstrap(4, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let group_a: BTreeSet<WorkerId> = ids[..1].iter().cloned().collect(); // 1 node: minority.
    let group_b: BTreeSet<WorkerId> = ids[1..].iter().cloned().collect(); // 3 nodes: majority.
    let minority_id = ids[0].clone();

    cluster.partition(group_a.clone(), group_b.clone());

    // Cross the suspicion boundary by hand (see the tuning note on test 2).
    for _ in 0..3 {
        cluster.advance(tick_size);
    }

    // Each side runs its own election. The minority (1 node, against its own
    // 4-member electorate view) can never reach quorum 3; the majority can.
    cluster.run_until_quiescent(tick_size, 60);

    // While still partitioned, the majority must have elected its own leader.
    let leader_while_partitioned = cluster
        .leader()
        .expect("the 3-node majority side must elect a leader while still partitioned");
    assert!(
        group_b.contains(&leader_while_partitioned),
        "the elected leader while partitioned must be a majority-side node, got {leader_while_partitioned:?}"
    );

    let minority_state = cluster.states()[&minority_id];
    assert_ne!(
        minority_state,
        WorkerState::Leader,
        "the 1-node minority side can never reach quorum (needs 3 of 4) and so must never elect \
         a leader while partitioned"
    );

    // A synthetic ack sent across the active partition must be dropped: the
    // minority node is stuck in `RollCall`, so delivery would flip it to Active.
    let state_before_send = cluster.states()[&minority_id];
    cluster.network().send(
        leader_while_partitioned.clone(),
        minority_id.clone(),
        heartbeat_ack(SHARD, &leader_while_partitioned),
    );
    cluster.advance(tick_size);
    assert_eq!(
        cluster.states()[&minority_id],
        state_before_send,
        "a message sent across an ACTIVE partition must be dropped, not delivered — the \
         minority node's state must be unaffected"
    );

    // After heal, the identical send must be delivered on the next advance(),
    // flipping the minority from `RollCall` to `Active`.
    cluster.heal();
    cluster.network().send(
        leader_while_partitioned.clone(),
        minority_id.clone(),
        heartbeat_ack(SHARD, &leader_while_partitioned),
    );
    cluster.advance(tick_size);
    assert_eq!(
        cluster.states()[&minority_id],
        WorkerState::Active,
        "a message sent after heal() must be delivered on the next advance() — the previously- \
         isolated minority node must transition RollCall -> Active via on_leader_ack, proving \
         connectivity actually resumed"
    );

    // Let everything settle; the cluster-wide invariant must hold.
    cluster.run_until_quiescent(tick_size, 60);
    cluster.assert_at_most_one_leader();
}

#[test]
fn drain_works_through_the_harness() {
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(50));
    let target = worker("worker-0");

    // Every node is Active, which allows `Active -> Draining`.
    assert_eq!(cluster.states()[&target], WorkerState::Active);

    cluster.drain(&target);

    assert_eq!(
        cluster.states()[&target],
        WorkerState::Stopped,
        "begin_drain() synchronously reaches Stopped with no wire round-trip (Draining is \
         momentary and unobservable), per WorkerNode::begin_drain's own doc comment"
    );
}

#[test]
#[should_panic(expected = "is not a known node ID")]
fn drain_panics_on_an_unknown_worker_id() {
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(50));
    cluster.drain(&worker("does-not-exist"));
}

#[test]
fn run_until_quiescent_waits_for_delayed_messages() {
    let tick_size = Duration::from_ticks(5);
    // A suspect timeout far beyond this test, so no election interferes.
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(1000));
    cluster.network().set_delay(Duration::from_ticks(30));

    let drainer = cluster.node_ids().into_iter().next().unwrap();
    cluster.drain(&drainer);
    assert_eq!(cluster.network().pending().len(), 2);

    let iterations = cluster.run_until_quiescent(tick_size, 60);

    assert_eq!(
        cluster.network().pending().len(),
        0,
        "quiescence must not be reported while a delayed SelfRemove is still in flight"
    );
    assert!(
        iterations > 1 && iterations < 60,
        "ran {iterations} iterations"
    );
}

#[test]
fn run_until_quiescent_settles_a_healed_partition_without_extra_advances() {
    let tick_size = Duration::from_ticks(5);
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(10));
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    cluster.partition(
        ids[..2].iter().cloned().collect(),
        ids[2..].iter().cloned().collect(),
    );
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    cluster.run_until_quiescent(tick_size, 60);
    assert_eq!(cluster.states()[&ids[2]], WorkerState::RollCall);

    cluster.heal();
    cluster.run_until_quiescent(tick_size, 60);

    assert_eq!(
        cluster.states()[&ids[2]],
        WorkerState::Active,
        "the isolated node must have received the leader's heartbeat before quiescence"
    );
}

#[test]
fn run_until_quiescent_terminates_quickly_once_converged() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let max_ticks = 60;

    let mut cluster = Cluster::bootstrap(3, suspect_timeout);

    // Cross the suspicion boundary by hand: otherwise the first call would converge after 1 iteration on an all-Active cluster.
    for _ in 0..3 {
        cluster.advance(tick_size);
    }

    let first_run = cluster.run_until_quiescent(tick_size, max_ticks);
    assert!(
        first_run < max_ticks,
        "expected the initial election to settle well before max_ticks, ran all {first_run}"
    );
    assert!(
        cluster.leader().is_some(),
        "expected a leader to have been elected before checking re-quiescence; states: {:?}",
        cluster.states()
    );

    // The cluster is settled (a Leader that still sees full quorum does
    // nothing on tick), so a second call must find the fixed point at once.
    let second_run = cluster.run_until_quiescent(tick_size, max_ticks);
    assert_eq!(
        second_run, 1,
        "an already-settled cluster must be detected as quiescent on the very first check"
    );
}
