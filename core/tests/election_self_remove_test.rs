mod support;

use support::builders::{
    SharedMembership, make_network, observation, roll_call, roll_call_message, shard, worker,
};

use support::candidate::predict_winner;

use std::collections::BTreeSet;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::MembershipView;
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{ElectionMessage, SelfRemove, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::clock::FakeClock;
use support::coordination_authority::FakeCoordinationAuthority;
use support::network::FakeNetwork;

const SHARD: &str = "shard-1";

fn self_remove_for(worker_id: &WorkerId) -> SelfRemove {
    SelfRemove {
        worker_id: Some(worker_id.clone().into()),
        incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
        shard_id: Some(shard(SHARD).into()),
        membership_generation: 0,
    }
}

fn self_remove_for_shard(worker_id: &WorkerId, shard_id: &str) -> SelfRemove {
    let mut msg = self_remove_for(worker_id);
    msg.shard_id = Some(shard(shard_id).into());
    msg
}

fn self_remove_message(msg: SelfRemove) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::SelfRemove(msg)),
    }
}

#[allow(clippy::type_complexity)]
fn make_node_with_ring(
    clock: &FakeClock,
    network: &FakeNetwork,
    my_id: WorkerId,
    electorate: &[WorkerId],
    suspect_timeout: Duration,
) -> (
    WorkerNode<FakeClock, FakeNetwork, SharedMembership, FakeCoordinationAuthority>,
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
        FakeCoordinationAuthority::new(),
        suspect_timeout,
    );
    (node, membership)
}

#[test]
fn begin_drain_from_active_transitions_to_stopped_and_emits_self_remove() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let peer = worker("w2");
    let network = make_network(&clock, &[self_id.clone(), peer.clone()]);
    let (mut node, _membership) = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), peer.clone()],
        suspect_timeout,
    );
    assert_eq!(node.state(), WorkerState::Active);

    node.begin_drain();

    assert_eq!(
        node.state(),
        WorkerState::Stopped,
        "begin_drain must synchronously continue through Draining all the way to Stopped"
    );

    network.pump();
    let mut inbox = network.poll_inbox(peer);
    assert_eq!(inbox.len(), 1, "expected exactly one SelfRemove broadcast");
    match inbox.remove(0).1.payload {
        Some(election_message::Payload::SelfRemove(msg)) => {
            assert_eq!(msg.worker_id(), self_id);
            assert_eq!(msg.incarnation_id(), IncarnationId::new("incarnation-1"));
            assert_eq!(msg.shard_id(), shard(SHARD));
        }
        other => panic!("expected SelfRemove payload, got {other:?}"),
    }
}

#[test]
fn begin_drain_from_leader_transitions_to_stopped_and_emits_self_remove() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let solo = worker("solo");
    let peer = worker("peer");
    let network = make_network(&clock, &[solo.clone(), peer.clone()]);
    // A single-member electorate reaches Leader within two tick() calls on its
    // own self-vote. `peer` is on the network but not in the electorate, so it
    // receives only the SELF_REMOVE.
    let (mut node, _membership) = make_node_with_ring(
        &clock,
        &network,
        solo.clone(),
        std::slice::from_ref(&solo),
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));
    node.tick(); // Active -> LeaderSuspect
    node.tick(); // ... -> Leader
    assert_eq!(node.state(), WorkerState::Leader);

    node.begin_drain();

    assert_eq!(node.state(), WorkerState::Stopped);

    network.pump();
    let mut inbox = network.poll_inbox(peer);
    assert_eq!(inbox.len(), 1, "expected exactly one SelfRemove broadcast");
    match inbox.remove(0).1.payload {
        Some(election_message::Payload::SelfRemove(msg)) => {
            assert_eq!(msg.worker_id(), solo);
        }
        other => panic!("expected SelfRemove payload, got {other:?}"),
    }
}

#[test]
fn begin_drain_from_illegal_state_is_a_no_op() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let candidate_a = worker("candidate-a");
    let candidate_b = worker("candidate-b");
    let next_term = 1; // both observations below carry highest_term_seen: 0.
    let winner = predict_winner(
        &shard(SHARD),
        0,
        next_term,
        &[candidate_a.clone(), candidate_b.clone()],
    );
    // A 2-member electorate (vote quorum 2), so the node lands in and stays in Candidate.
    let (self_id, peer) = if winner == candidate_a {
        (candidate_a, candidate_b)
    } else {
        (candidate_b, candidate_a)
    };
    let network = make_network(&clock, &[self_id.clone(), peer.clone()]);
    let (mut node, _membership) = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), peer.clone()],
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));
    node.tick();
    node.tick();
    assert_eq!(node.state(), WorkerState::RollCall);
    network.pump();
    network.poll_inbox(peer.clone());

    let call = roll_call(
        "external-call-1",
        peer.clone(),
        vec![observation(peer.clone(), 0)],
    );
    node.on_message(peer.clone(), roll_call_message(call));
    assert_eq!(node.state(), WorkerState::Candidate);
    network.pump();
    network.poll_inbox(peer.clone()); // drain the VoteRequest it sends.

    node.begin_drain();

    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "begin_drain from a state with no legal edge to Draining must be a complete no-op"
    );
    network.pump();
    assert!(
        network.poll_inbox(peer).is_empty(),
        "no SelfRemove (or any other message) should be sent by a no-op begin_drain"
    );
}

#[test]
fn begin_drain_applies_self_remove_to_own_membership() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let peer = worker("w2");
    let network = make_network(&clock, &[self_id.clone(), peer.clone()]);
    let (mut node, membership) = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), peer.clone()],
        suspect_timeout,
    );

    node.begin_drain();

    assert_eq!(node.state(), WorkerState::Stopped);
    assert_eq!(
        membership.effective_electorate(),
        [peer].into_iter().collect::<BTreeSet<_>>(),
        "a draining node must stop counting itself in its own effective_electorate()"
    );
}

// A 2 -> 1 shrink rather than 3 -> 2: `n/2 + 1` is 2 for both 3 and 2, so
// only a change like 2 -> 1 (threshold 2 to 1) shows the removal reached the
// membership the roll call's quorum check reads.
#[test]
fn on_self_remove_delegates_and_shrinks_effective_electorate_for_subsequent_quorum() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let other = worker("w2");
    let network = make_network(&clock, &[self_id.clone(), other.clone()]);
    let (mut node, _membership) = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), other.clone()],
        suspect_timeout,
    );

    // Deliver, through the full on_message dispatcher, a SelfRemove naming the OTHER member.
    node.on_message(other.clone(), self_remove_message(self_remove_for(&other)));

    // The electorate is now {self} (quorum 1), so a fresh roll call's own
    // observation meets quorum and the node reaches Leader in one tick. That is
    // only possible if on_self_remove mutated this node's membership.
    clock.advance(Duration::from_ticks(11));
    node.tick(); // Active -> LeaderSuspect
    assert_eq!(node.state(), WorkerState::LeaderSuspect);
    node.tick(); // LeaderSuspect -> RollCall -> quorum-of-1 -> ... -> Leader
    assert_eq!(node.state(), WorkerState::Leader);
}

#[test]
fn on_self_remove_mismatched_shard_id_is_silently_ignored() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let other = worker("w2");
    let network = make_network(&clock, &[self_id.clone(), other.clone()]);
    let (mut node, membership) = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), other.clone()],
        suspect_timeout,
    );

    node.on_self_remove(&self_remove_for_shard(&other, "other-shard"));

    assert_eq!(
        membership.effective_electorate(),
        [self_id, other].into_iter().collect::<BTreeSet<_>>(),
        "a shard_id mismatch must produce zero mutation to this node's membership view"
    );
}

#[test]
fn on_self_remove_delivered_twice_shrinks_electorate_exactly_once() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let other = worker("w2");
    let network = make_network(&clock, &[self_id.clone(), other.clone()]);
    let (mut node, membership) = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), other.clone()],
        suspect_timeout,
    );

    let msg = self_remove_for(&other);
    node.on_self_remove(&msg);
    node.on_self_remove(&msg);

    assert_eq!(
        membership.effective_electorate(),
        [self_id].into_iter().collect::<BTreeSet<_>>(),
    );
    assert_eq!(
        membership.membership_generation(),
        1,
        "a duplicate delivery must not double-bump the generation counter"
    );
}

#[test]
fn self_remove_from_a_sender_other_than_the_departing_worker_is_ignored() {
    let clock = FakeClock::new();
    let self_id = worker("w1");
    let victim = worker("w2");
    let impostor = worker("w3");
    let network = make_network(&clock, &[self_id.clone(), victim.clone(), impostor.clone()]);
    let electorate = [self_id.clone(), victim.clone(), impostor.clone()];
    let (mut node, membership) = make_node_with_ring(
        &clock,
        &network,
        self_id,
        &electorate,
        Duration::from_ticks(10),
    );

    node.on_message(impostor, self_remove_message(self_remove_for(&victim)));

    assert_eq!(
        membership.effective_electorate(),
        electorate.into_iter().collect::<BTreeSet<_>>(),
        "a worker may only remove itself, not evict another worker"
    );
    assert_eq!(membership.membership_generation(), 0);
}
