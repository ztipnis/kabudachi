//! Tests that `tick()`'s `Leader` branch sends `LeaderHeartbeatAck`s
//! (README §12.1), the sending half of what `on_leader_ack` receives.
//!
//! Kept apart from `election_heartbeat_test.rs` because these tests need a real
//! multi-member electorate driven all the way to `Leader`, the same technique
//! `election_forced_recovery_test.rs` uses. Test 3 runs end to end on the
//! `Cluster` harness.

mod support;

use support::builders::{
    make_network, observation, roll_call, roll_call_message, shard, vote_grant, worker,
};

use support::candidate::predict_winner;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{ElectionMessage, LeaderHeartbeatAck, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::clock::FakeClock;
use support::coordination_authority::FakeCoordinationAuthority;
use support::harness::Cluster;
use support::network::FakeNetwork;

const SHARD: &str = "shard-1";

/// Drives a fresh node to a real `Leader` with a 3-member electorate through
/// roll call and voting. Returns `(node, self_id, peer_a, peer_b, network)`:
/// `peer_a` casts the deciding grant, `peer_b` only makes the electorate 3
/// (quorum 2), all three are mutually reachable, and every message the setup
/// generated is already drained.
fn leader_with_electorate(
    clock: &FakeClock,
) -> (
    WorkerNode<FakeClock, FakeNetwork, RingMembership, FakeCoordinationAuthority>,
    WorkerId,
    WorkerId,
    WorkerId,
    FakeNetwork,
) {
    let suspect_timeout = Duration::from_ticks(10);

    let candidate_x = worker("candidate-x");
    let candidate_y = worker("candidate-y");
    let next_term = 1; // both observations below carry highest_term_seen: 0.
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

    let network = make_network(clock, &[self_id.clone(), peer_a.clone(), peer_b.clone()]);
    let membership = RingMembership::new(
        [self_id.clone(), peer_a.clone(), peer_b.clone()]
            .into_iter()
            .collect(),
    );
    let authority = FakeCoordinationAuthority::new();
    let mut node = WorkerNode::new(
        self_id.clone(),
        IncarnationId::new("incarnation-1"),
        shard(SHARD),
        clock.clone(),
        network.clone(),
        membership,
        authority,
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));
    node.tick(); // Active -> LeaderSuspect
    node.tick(); // LeaderSuspect -> RollCall (self-only response: 1 < quorum-of-2, forwards).
    assert_eq!(node.state(), WorkerState::RollCall, "test setup invariant");

    // A synthetic roll call carrying peer_a's observation reaches quorum 2;
    // self_id was chosen to win.
    let call = roll_call(
        "external-call-1",
        peer_a.clone(),
        vec![observation(peer_a.clone(), 0)],
    );
    node.on_message(peer_a.clone(), roll_call_message(call));
    assert_eq!(node.state(), WorkerState::Candidate, "test setup invariant");

    // The self-vote plus this grant reaches quorum 2.
    node.on_vote_grant(&vote_grant(self_id.clone(), peer_a.clone(), next_term));
    assert_eq!(node.state(), WorkerState::Leader, "test setup invariant");

    // Drain everything the setup generated so callers start from empty inboxes.
    network.pump();
    for id in [self_id.clone(), peer_a.clone(), peer_b.clone()] {
        let _ = network.poll_inbox(id);
    }

    (node, self_id, peer_a, peer_b, network)
}

fn expect_heartbeat_ack(msg: ElectionMessage) -> LeaderHeartbeatAck {
    match msg.payload {
        Some(election_message::Payload::HeartbeatAck(ack)) => ack,
        other => panic!("expected a LeaderHeartbeatAck payload, got {other:?}"),
    }
}

#[test]
fn leader_broadcasts_heartbeat_to_reachable_electorate_on_tick() {
    let clock = FakeClock::new();
    let (mut node, self_id, peer_a, peer_b, network) = leader_with_electorate(&clock);
    assert_eq!(node.state(), WorkerState::Leader);

    node.tick();
    network.pump();

    // Both peers are reachable and must each get one ack carrying this
    // leader's identity, term and recovery_epoch.
    for peer in [peer_a, peer_b] {
        let mut inbox = network.poll_inbox(peer.clone());
        assert_eq!(
            inbox.len(),
            1,
            "expected exactly one message delivered to {peer:?}, got {inbox:?}"
        );
        let (from, msg) = inbox.remove(0);
        assert_eq!(
            from, self_id,
            "the heartbeat must be sent from the leader itself"
        );
        let ack = expect_heartbeat_ack(msg);
        assert_eq!(
            ack.leader_id(),
            self_id,
            "leader_id must name the real leader"
        );
        assert_eq!(ack.term, 1, "term must be the leader's real current term");
        assert_eq!(
            ack.recovery_epoch, 0,
            "recovery_epoch must be the leader's real current epoch"
        );
    }

    // The leader never sends itself a heartbeat.
    assert!(
        network.poll_inbox(self_id).is_empty(),
        "a leader must never send itself a heartbeat"
    );
}

#[test]
fn leader_does_not_broadcast_heartbeat_to_a_partitioned_away_electorate_member() {
    let clock = FakeClock::new();
    let (mut node, self_id, peer_a, peer_b, network) = leader_with_electorate(&clock);
    assert_eq!(node.state(), WorkerState::Leader);

    // Partition self from peer_b only: the visible count stays 2, still a
    // quorum, so this isolates the broadcast's recipient filtering from the
    // peer-loss transition.
    network.partition(
        [self_id.clone()].into_iter().collect(),
        [peer_b.clone()].into_iter().collect(),
    );

    node.tick();
    network.pump();

    assert_eq!(
        node.state(),
        WorkerState::Leader,
        "test setup invariant: partitioning away only one of two peers must not cost quorum"
    );

    // Reachable peer_a: gets the real heartbeat.
    let mut inbox_a = network.poll_inbox(peer_a);
    assert_eq!(
        inbox_a.len(),
        1,
        "expected exactly one message delivered to peer_a"
    );
    expect_heartbeat_ack(inbox_a.remove(0).1);

    // peer_b, partitioned away, receives nothing.
    assert!(
        network.poll_inbox(peer_b).is_empty(),
        "a partitioned-away electorate member must receive no heartbeat"
    );
}

// A follower receiving a real leader's heartbeat resets its suspicion timer,
// end to end through the `Cluster` harness (send and receive together).
#[test]
fn follower_never_suspects_a_leader_that_keeps_broadcasting_real_heartbeats() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let mut cluster = Cluster::bootstrap(2, suspect_timeout);

    // Converge to one Leader and one Active follower with Cluster::advance(),
    // bounded by a cap because run_until_quiescent's fixed point ignores
    // repeating heartbeats.
    let mut converged = false;
    for _ in 0..30 {
        cluster.advance(tick_size);
        if cluster.leader().is_some() {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "expected a leader to emerge in a 2-node cluster within 30 advances"
    );

    let leader_id = cluster.leader().expect("checked above");
    let follower_id = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader_id)
        .expect("a 2-node cluster must have exactly one non-leader node");

    // Delivery is one advance behind the send, so give the follower one more step to settle into Active.
    cluster.advance(tick_size);
    assert_eq!(
        cluster.states()[&follower_id],
        WorkerState::Active,
        "the follower must have returned to Active via a real leader-originated heartbeat"
    );

    // Advance repeatedly, well within suspect_timeout; the follower must never
    // become suspicious because it gets a fresh heartbeat every advance.
    for _ in 0..20 {
        cluster.advance(tick_size);
        assert_eq!(
            cluster.states()[&follower_id],
            WorkerState::Active,
            "a follower receiving a real heartbeat every tick must never become suspicious"
        );
        assert_eq!(
            cluster.leader(),
            Some(leader_id.clone()),
            "the leader must remain stable throughout"
        );
    }
}
