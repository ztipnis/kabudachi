//! Catastrophic-authority scenarios (README §26.3) at the full `Cluster`
//! level: a multi-node cluster loses quorum, calls forced recovery under
//! various authority conditions, and reacts afterwards.
//!
//! `no_quorum_redis_slow` is scoped narrowly: authority calls are synchronous
//! and take no simulated time, so `FakeCoordinationAuthority::set_latency`
//! cannot change any outcome and slowness itself is not exercised.
//!
//! Known gap: groups of more than 3 simultaneously suspecting nodes can elect
//! two leaders (see `scenario_partition_test.rs`), so larger groups are
//! elected by manually starting exactly one pre-computed winner
//! (`elect_new_leader_among`).

mod support;

use support::scenarios::{bootstrap_5_and_elect_leader, elect_new_leader_among};

use support::builders::{
    SharedMembership, observation, roll_call, roll_call_message, shard, worker,
};

use support::candidate::predict_winner;

use std::collections::BTreeSet;
use std::rc::Rc;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::MembershipView;
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::election_message;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::clock::FakeClock;
use support::coordination_authority::FakeCoordinationAuthority;
use support::harness::Cluster;
use support::network::FakeNetwork;

const SHARD: &str = "shard-1"; // Matches Cluster::bootstrap's own documented shard-1 scheme.

/// Bootstraps `n` nodes and elects a leader among all of them with
/// `elect_new_leader_among`, which is safe for any `n`. Returns `(cluster,
/// leader)` settled.
fn bootstrap_and_elect_leader_general(
    n: usize,
    suspect_timeout: Duration,
    tick_size: Duration,
) -> (Cluster, WorkerId) {
    let mut cluster = Cluster::bootstrap(n, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();

    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    for id in &ids {
        assert_eq!(
            cluster.states()[id],
            WorkerState::LeaderSuspect,
            "setup invariant"
        );
    }

    let leader = elect_new_leader_among(&mut cluster, &ids, 0);
    cluster.run_until_quiescent(tick_size, 60);
    for id in &ids {
        let expected = if *id == leader {
            WorkerState::Leader
        } else {
            WorkerState::Active
        };
        assert_eq!(
            cluster.states()[id],
            expected,
            "expected the whole cluster to settle after the initial election"
        );
    }

    (cluster, leader)
}

/// Shared setup for scenarios 1 and 2: 5 nodes with a leader, then the leader
/// and one follower (`helper`) are partitioned from the other 3 (`majority`).
/// The leader sees 2 of 5, below quorum, so it goes `NoQuorum` while `helper`
/// is still reachable to recover through. Returns `(cluster, leader, helper,
/// majority)`.
fn bootstrap_isolated_leader_with_one_reachable_peer(
    suspect_timeout: Duration,
    tick_size: Duration,
) -> (Cluster, WorkerId, WorkerId, BTreeSet<WorkerId>) {
    let (mut cluster, leader) = bootstrap_5_and_elect_leader(suspect_timeout, tick_size);
    let followers: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    assert_eq!(followers.len(), 4);
    let helper = followers[0].clone();
    let majority: BTreeSet<WorkerId> = followers[1..].iter().cloned().collect();

    let isolated_group: BTreeSet<WorkerId> = [leader.clone(), helper.clone()].into_iter().collect();
    cluster.partition(isolated_group, majority.clone());
    cluster.advance(tick_size); // Flush an in-flight heartbeat + let leader's live tick() re-evaluate quorum.
    assert_eq!(
        cluster.states()[&leader],
        WorkerState::NoQuorum,
        "setup invariant: leader must detect genuine peer loss (visible 2 < quorum 3) via a real tick()"
    );

    (cluster, leader, helper, majority)
}

#[test]
fn no_quorum_redis_healthy() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let (mut cluster, leader, _helper, majority) =
        bootstrap_isolated_leader_with_one_reachable_peer(suspect_timeout, tick_size);

    // The authority reports the leader's real 5-member world; a partitioned authority view is scenario 4.
    let all_ids = cluster.node_ids();
    cluster
        .authority()
        .force_reconfigure(&shard(SHARD), 0, all_ids)
        .expect("seed a healthy authority view");

    cluster.node(&leader).attempt_forced_recovery();

    assert_eq!(
        cluster.node(&leader).state(),
        WorkerState::RollCall,
        "a healthy authority + real local reachability (to `helper`) must let forced recovery succeed"
    );
    assert_eq!(
        cluster.node(&leader).recovery_epoch(),
        2,
        "recovery_epoch must have genuinely advanced past this test's own seed bump (0 -> 1 -> 2)"
    );

    // The other 3 followers, cut off from the leader and helper, notice the
    // lost leader and elect a new one among themselves.
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    cluster.run_until_quiescent(tick_size, 60);

    let new_leader = cluster
        .leader()
        .expect("the reachable majority-of-3 group must elect a new leader");
    assert!(
        majority.contains(&new_leader),
        "the new leader must come from the still-mutually-reachable majority group"
    );
    assert_ne!(new_leader, leader);
    cluster.assert_at_most_one_leader();
}

// Scenario 2 (no_quorum_redis_slow): authority calls are synchronous, so
// latency cannot be observed. This only checks that a large `set_latency`
// value does not interfere with an ordinary successful forced recovery.
#[test]
fn no_quorum_redis_slow() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let (mut cluster, leader, _helper, _majority) =
        bootstrap_isolated_leader_with_one_reachable_peer(suspect_timeout, tick_size);

    let all_ids = cluster.node_ids();
    cluster
        .authority()
        .force_reconfigure(&shard(SHARD), 0, all_ids)
        .expect("seed a healthy authority view");
    cluster
        .authority()
        .set_latency(Duration::from_ticks(1_000_000));

    cluster.node(&leader).attempt_forced_recovery();

    assert_eq!(
        cluster.node(&leader).state(),
        WorkerState::RollCall,
        "a configured (but, per this design, inert) large latency must not spuriously block forced \
         recovery — this is the full extent of what 'slow' can mean in this synchronous implementation"
    );
    assert_eq!(
        cluster.node(&leader).recovery_epoch(),
        2,
        "recovery_epoch must have advanced identically to the no_quorum_redis_healthy scenario, \
         proving set_latency changed nothing about the outcome"
    );
}

// Scenario 3 (no_quorum_redis_empty): built on a bare `WorkerNode` because it
// must read the node's own `MembershipView` (through `SharedMembership`) to
// prove no membership was fabricated, and `Cluster` cannot substitute that
// wrapper.

fn make_network_direct(clock: &FakeClock, members: &[WorkerId]) -> FakeNetwork {
    let network = FakeNetwork::new(Rc::new(clock.clone()));
    for member in members {
        network.register(member.clone());
    }
    network
}

#[allow(clippy::type_complexity)]
fn make_node_with_ring<A: CoordinationAuthority + Clone>(
    clock: &FakeClock,
    network: &FakeNetwork,
    authority: &A,
    my_id: WorkerId,
    electorate: &[WorkerId],
    suspect_timeout: Duration,
) -> (
    WorkerNode<FakeClock, FakeNetwork, SharedMembership, A>,
    SharedMembership,
) {
    let membership = SharedMembership::new(electorate.iter().cloned().collect());
    let node = WorkerNode::new(
        my_id,
        IncarnationId::new("incarnation-1"),
        shard(SHARD),
        clock.clone(),
        network.clone(),
        membership.clone(),
        authority.clone(),
        suspect_timeout,
    );
    (node, membership)
}

/// Drives a fresh node to a real `Leader` with a 3-member electorate.
#[allow(clippy::type_complexity)]
fn leader_with_electorate<A: CoordinationAuthority + Clone>(
    clock: &FakeClock,
    authority: &A,
) -> (
    WorkerNode<FakeClock, FakeNetwork, SharedMembership, A>,
    WorkerId,
    WorkerId,
    WorkerId,
    WorkerId,
    FakeNetwork,
    SharedMembership,
) {
    let suspect_timeout = Duration::from_ticks(10);

    let candidate_x = worker("candidate-x");
    let candidate_y = worker("candidate-y");
    let next_term = 1;
    let winner = predict_winner(
        &shard(SHARD),
        0,
        next_term,
        &[candidate_x.clone(), candidate_y.clone()],
    );
    let (self_id, peer_a) = if winner == candidate_x {
        (candidate_x, candidate_y)
    } else {
        (candidate_y, candidate_x)
    };
    let peer_b = worker("peer-b");
    let helper_peer = worker("helper-peer");

    let network = make_network_direct(
        clock,
        &[
            self_id.clone(),
            peer_a.clone(),
            peer_b.clone(),
            helper_peer.clone(),
        ],
    );
    let (mut node, membership) = make_node_with_ring(
        clock,
        &network,
        authority,
        self_id.clone(),
        &[self_id.clone(), peer_a.clone(), peer_b.clone()],
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));
    node.tick(); // Active -> LeaderSuspect
    node.tick(); // LeaderSuspect -> RollCall (self-only response: 1 < quorum-of-2, forwards).
    assert_eq!(node.state(), WorkerState::RollCall, "test setup invariant");

    let call = roll_call(
        "external-call-1",
        peer_a.clone(),
        vec![observation(peer_a.clone(), 0)],
    );
    node.on_message(peer_a.clone(), roll_call_message(call));
    assert_eq!(node.state(), WorkerState::Candidate, "test setup invariant");

    let grant = kabudachi_core::protocol::messages::VoteGrant {
        shard_id: Some(shard(SHARD).into()),
        recovery_epoch: 0,
        term: next_term,
        candidate_id: Some(self_id.clone().into()),
        voter_id: Some(peer_a.clone().into()),
    };
    node.on_vote_grant(&grant);
    assert_eq!(node.state(), WorkerState::Leader, "test setup invariant");

    (
        node,
        self_id,
        peer_a,
        peer_b,
        helper_peer,
        network,
        membership,
    )
}

/// Drives a fresh node to `NoQuorum` through the leader's peer-loss edge.
#[allow(clippy::type_complexity)]
fn leader_in_no_quorum<A: CoordinationAuthority + Clone>(
    clock: &FakeClock,
    authority: &A,
) -> (
    WorkerNode<FakeClock, FakeNetwork, SharedMembership, A>,
    WorkerId,
    WorkerId,
    WorkerId,
    WorkerId,
    FakeNetwork,
    SharedMembership,
) {
    let (mut node, self_id, peer_a, peer_b, helper_peer, network, membership) =
        leader_with_electorate(clock, authority);

    network.partition(
        [self_id.clone()].into_iter().collect(),
        [peer_a.clone(), peer_b.clone()].into_iter().collect(),
    );
    node.tick(); // Leader -> NoQuorum.
    assert_eq!(node.state(), WorkerState::NoQuorum, "test setup invariant");

    (
        node,
        self_id,
        peer_a,
        peer_b,
        helper_peer,
        network,
        membership,
    )
}

#[test]
fn no_quorum_redis_empty() {
    let clock = FakeClock::new();
    let authority = FakeCoordinationAuthority::new(); // Deliberately never seeded: a shard the authority has never seen.
    let (mut node, _self_id, _peer_a, _peer_b, _helper_peer, _network, membership) =
        leader_in_no_quorum(&clock, &authority);

    let electorate_before = membership.effective_electorate();
    let generation_before = membership.membership_generation();
    let recovery_epoch_before = node.recovery_epoch();

    assert_eq!(
        authority.discover_workers(&shard(SHARD)).unwrap(),
        BTreeSet::new(),
        "setup invariant: an unseeded shard's authority view must be empty, with no overlap against anyone"
    );

    node.attempt_forced_recovery();

    assert_eq!(
        node.state(),
        WorkerState::NoQuorum,
        "an empty authority view must leave the node stuck in NoQuorum (fail_recovery's implicit no-op)"
    );
    assert_eq!(
        node.recovery_epoch(),
        recovery_epoch_before,
        "recovery_epoch must be completely untouched by a failed attempt"
    );
    assert_eq!(
        authority.read_recovery_epoch(&shard(SHARD)).unwrap(),
        0,
        "the authority itself must never have been mutated by a failed attempt"
    );
    // No membership fabrication: an empty authority view must leave the
    // electorate unchanged, not emptied or shrunk to {self}.
    assert_eq!(
        membership.effective_electorate(),
        electorate_before,
        "CRITICAL: no membership fabrication — the electorate must be unchanged, not reconstructed \
         from an empty/fabricated set"
    );
    assert_eq!(
        membership.membership_generation(),
        generation_before,
        "membership_generation must not have bumped either — rebuild() must never have been called"
    );
}

#[test]
fn no_quorum_redis_partitioned_differently_from_workers() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    // 6 nodes: an intersection smaller than either input needs the leader to
    // keep 2 reachable peers while dropping below quorum. A 5-member electorate
    // (quorum 3) cannot do that, a 6-member one (quorum 4) can.
    let (mut cluster, leader) = bootstrap_and_elect_leader_general(6, suspect_timeout, tick_size);
    let others: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    assert_eq!(others.len(), 5);
    let f1 = others[0].clone();
    let f2 = others[1].clone();
    let q = others[2].clone();
    let r = others[3].clone();
    let s = others[4].clone();

    // Partition leader+f1+f2 from {q,r,s}: the leader sees 3 of 6 (quorum 4),
    // so it goes NoQuorum while keeping two reachable peers.
    cluster.partition(
        [leader.clone(), f1.clone(), f2.clone()]
            .into_iter()
            .collect(),
        [q.clone(), r.clone(), s.clone()].into_iter().collect(),
    );
    cluster.advance(tick_size);
    assert_eq!(
        cluster.states()[&leader],
        WorkerState::NoQuorum,
        "setup invariant: leader must detect genuine peer loss (visible 3 < quorum 4)"
    );
    // Drain the heartbeat the leader broadcast while demoting, so the RollCall
    // inbox check below sees only forced recovery's own message.
    cluster.network().pump();
    let _ = cluster.network().poll_inbox(f1.clone());
    let _ = cluster.network().poll_inbox(f2.clone());

    // The authority's view differs from the network: it sees {leader, f2, q},
    // wrongly excluding f1 (reachable) and including q (unreachable).
    let all_ids = cluster.node_ids();
    cluster
        .authority()
        .force_reconfigure(&shard(SHARD), 0, all_ids)
        .expect("seed the authority's stored roster");
    cluster
        .authority()
        .partition_from([f1.clone(), r.clone(), s.clone()].into_iter().collect());
    assert_eq!(
        cluster.authority().discover_workers(&shard(SHARD)).unwrap(),
        [leader.clone(), f2.clone(), q.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>(),
        "setup invariant: authority view must be exactly {{leader, f2, q}}"
    );

    // The network reaches {f1, f2}; the intersection is exactly {f2}, smaller
    // than both inputs.
    cluster.node(&leader).attempt_forced_recovery();

    assert_eq!(
        cluster.node(&leader).state(),
        WorkerState::RollCall,
        "the non-empty intersection ({{f2}}) must let forced recovery succeed"
    );
    assert_eq!(
        cluster.node(&leader).recovery_epoch(),
        2,
        "recovery_epoch must have advanced past this test's own seed bump (0 -> 1 -> 2)"
    );

    // The rebuilt electorate is exactly {leader, f2}: check it by seeing where
    // the new RollCall is forwarded.
    cluster.network().pump();
    let f2_inbox = cluster.network().poll_inbox(f2.clone());
    assert_eq!(
        f2_inbox.len(),
        1,
        "expected exactly one forwarded RollCall, addressed to f2 specifically"
    );
    match &f2_inbox[0].1.payload {
        Some(election_message::Payload::RollCall(call)) => {
            assert_eq!(
                call.recovery_epoch, 2,
                "the RollCall must carry the freshly-bumped recovery_epoch"
            );
        }
        other => panic!("expected a RollCall payload, got {other:?}"),
    }
    assert!(
        cluster.network().poll_inbox(f1.clone()).is_empty(),
        "f1 (network-reachable but authority-excluded) must NOT be part of the rebuilt electorate"
    );
    assert!(
        cluster.network().poll_inbox(q.clone()).is_empty(),
        "q (authority-confirmed but not locally reachable) must never even receive a real message"
    );

    cluster.assert_at_most_one_leader();
}

#[test]
fn flushall_with_healthy_shard() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let (mut cluster, leader) = bootstrap_5_and_elect_leader(suspect_timeout, tick_size);

    // Seed authority state so FLUSHALL has something to wipe.
    let all_ids = cluster.node_ids();
    cluster
        .authority()
        .force_reconfigure(&shard(SHARD), 0, all_ids)
        .expect("seed prior authority state");
    assert_eq!(
        cluster
            .authority()
            .read_recovery_epoch(&shard(SHARD))
            .unwrap(),
        1,
        "setup invariant"
    );

    cluster.authority().flush_all();
    assert_eq!(
        cluster
            .authority()
            .read_recovery_epoch(&shard(SHARD))
            .unwrap(),
        0,
        "setup invariant: FLUSHALL must have reset the shard to never-seen"
    );

    // A live shard never calls attempt_forced_recovery (README §15.3):
    // heartbeats keep flowing and the leader is unaffected.
    for _ in 0..6 {
        cluster.advance(tick_size);
    }

    assert_eq!(
        cluster.states()[&leader],
        WorkerState::Leader,
        "a FLUSHALL against the authority must never disrupt an already-healthy, authority-independent shard"
    );
    for id in cluster.node_ids() {
        if id != leader {
            assert_eq!(
                cluster.states()[&id],
                WorkerState::Active,
                "every follower must remain Active throughout"
            );
        }
    }
    cluster.assert_at_most_one_leader();
}

// Scenario 6: an isolated old leader cannot recover while cut off, even
// against a healthy authority. Once healed, its forced recovery never lets it
// reclaim `Leader`, and the new leader stays undisturbed.
//
// It cannot settle back to `Active`: its bumped `recovery_epoch` makes
// `on_leader_ack` treat the current leader's heartbeats as stale. The test
// asserts what is achievable, not a full rejoin.
#[test]
fn old_leader_reconnects_after_forced_recovery() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let (mut cluster, old_leader) = bootstrap_5_and_elect_leader(suspect_timeout, tick_size);
    let others: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != old_leader)
        .collect();
    assert_eq!(others.len(), 4);

    cluster.partition(
        [old_leader.clone()].into_iter().collect(),
        others.iter().cloned().collect(),
    );
    cluster.advance(tick_size); // Flush an in-flight heartbeat + let the leader's live tick() re-evaluate.
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    assert_eq!(
        cluster.states()[&old_leader],
        WorkerState::NoQuorum,
        "setup invariant: the isolated leader must detect its own peer loss"
    );
    for id in &others {
        assert_eq!(
            cluster.states()[id],
            WorkerState::LeaderSuspect,
            "setup invariant"
        );
    }

    // A healthy authority is not enough: forced recovery also needs local
    // reachability, and there is none.
    let all_ids = cluster.node_ids();
    cluster
        .authority()
        .force_reconfigure(&shard(SHARD), 0, all_ids)
        .expect("seed a healthy authority view");
    cluster.node(&old_leader).attempt_forced_recovery();
    assert_eq!(
        cluster.states()[&old_leader],
        WorkerState::NoQuorum,
        "forced recovery must still fail while genuinely isolated, even against a healthy authority"
    );
    assert_eq!(
        cluster.node(&old_leader).recovery_epoch(),
        0,
        "a failed attempt must never bump recovery_epoch"
    );
    assert_eq!(
        cluster
            .authority()
            .read_recovery_epoch(&shard(SHARD))
            .unwrap(),
        1,
        "only this test's own seed bump must have happened so far"
    );

    // The 4-member majority elects a new leader (every node's highest_term_seen is already 1).
    let new_leader = elect_new_leader_among(&mut cluster, &others, 1);
    cluster.run_until_quiescent(tick_size, 60);
    for id in &others {
        let expected = if *id == new_leader {
            WorkerState::Leader
        } else {
            WorkerState::Active
        };
        assert_eq!(
            cluster.states()[id],
            expected,
            "setup invariant: the majority must fully settle"
        );
    }

    // Heal, then the old leader's own forced recovery succeeds.
    cluster.heal();
    cluster.node(&old_leader).attempt_forced_recovery();

    assert_eq!(
        cluster.states()[&old_leader],
        WorkerState::RollCall,
        "a healed network + already-healthy authority must let forced recovery succeed this time"
    );
    assert_eq!(
        cluster.node(&old_leader).recovery_epoch(),
        2,
        "recovery_epoch must have genuinely advanced past the earlier seed bump"
    );
    assert_ne!(
        cluster.states()[&old_leader],
        WorkerState::Leader,
        "the old leader must NEVER fall back into believing it's still the authoritative leader"
    );

    // The old leader's stray roll call cannot disturb the new leader: only a
    // `RollCall` node can accept a win, and every other node is Active or Leader.
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    cluster.run_until_quiescent(tick_size, 60);

    assert_eq!(
        cluster.leader(),
        Some(new_leader),
        "the legitimately-elected new leader must remain the cluster's sole leader, undisturbed"
    );
    assert_ne!(
        cluster.states()[&old_leader],
        WorkerState::Leader,
        "old_leader must still never have reclaimed Leader status"
    );
    cluster.assert_at_most_one_leader();
}
