mod support;

use support::builders::{make_network, observation, roll_call, roll_call_message, shard, worker};

use support::candidate::predict_winner;

use std::collections::BTreeSet;

use kabudachi_core::election::{WorkerNode, candidate_priority};
use kabudachi_core::hashing::HashFunction;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{ElectionMessage, LeaderHeartbeatAck, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::clock::FakeClock;
use support::coordination_authority::FakeCoordinationAuthority;
use support::network::FakeNetwork;

const SHARD: &str = "shard-1";

fn heartbeat_ack(shard_id: &str, recovery_epoch: u64, term: u64) -> LeaderHeartbeatAck {
    LeaderHeartbeatAck {
        shard_id: Some(shard(shard_id).into()),
        leader_id: Some(worker("leader-1").into()),
        recovery_epoch,
        term,
        membership_generation: 0,
    }
}

fn make_node_with_ring(
    clock: &FakeClock,
    network: &FakeNetwork,
    my_id: WorkerId,
    electorate: &[WorkerId],
    suspect_timeout: Duration,
) -> WorkerNode<FakeClock, FakeNetwork, RingMembership, FakeCoordinationAuthority> {
    WorkerNode::new(
        my_id,
        IncarnationId::new("incarnation-1"),
        shard(SHARD),
        clock.clone(),
        network.clone(),
        RingMembership::new(electorate.iter().cloned().collect()),
        FakeCoordinationAuthority::new(),
        suspect_timeout,
    )
}

#[test]
fn tick_from_leader_suspect_begins_roll_call_and_sends_to_successor() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let successor = worker("w2");
    let network = make_network(&clock, &[self_id.clone(), successor.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), successor.clone()],
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));
    node.tick(); // Active -> LeaderSuspect.
    assert_eq!(node.state(), WorkerState::LeaderSuspect);

    node.tick(); // LeaderSuspect -> RollCall.
    assert_eq!(node.state(), WorkerState::RollCall);

    network.pump();
    let inbox = network.poll_inbox(successor);
    assert_eq!(
        inbox.len(),
        1,
        "expected exactly one message sent to the ring successor"
    );
    match &inbox[0].1.payload {
        Some(election_message::Payload::RollCall(call)) => {
            assert_eq!(call.initiator_id(), self_id);
            assert_eq!(
                call.responses.len(),
                1,
                "self's own observation must be included"
            );
        }
        other => panic!("expected RollCall payload, got {other:?}"),
    }
}

#[test]
fn on_roll_call_fresh_call_appends_observation_and_forwards_to_ring_successor() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let succ1 = worker("w2");
    let succ2 = worker("w3");
    let network = make_network(&clock, &[self_id.clone(), succ1.clone(), succ2.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), succ1.clone(), succ2.clone()],
        suspect_timeout,
    );

    let initiator = worker("initiator");
    let call = roll_call("call-1", initiator, vec![]);
    node.on_message(worker("someone"), roll_call_message(call));

    network.pump();
    let inbox = network.poll_inbox(succ1.clone());
    assert_eq!(
        inbox.len(),
        1,
        "expected the call forwarded to the immediate ring successor"
    );
    match &inbox[0].1.payload {
        Some(election_message::Payload::RollCall(forwarded)) => {
            assert_eq!(forwarded.roll_call_id, "call-1");
            assert_eq!(forwarded.responses.len(), 1);
            assert_eq!(forwarded.responses[0].worker_id(), self_id);
        }
        other => panic!("expected RollCall payload, got {other:?}"),
    }

    // Hop-by-hop relay, not a broadcast: the second successor gets nothing.
    assert!(network.poll_inbox(succ2).is_empty());
}

#[test]
fn on_roll_call_duplicate_id_is_a_no_op() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let succ1 = worker("w2");
    let succ2 = worker("w3");
    let network = make_network(&clock, &[self_id.clone(), succ1.clone(), succ2.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), succ1.clone(), succ2.clone()],
        suspect_timeout,
    );

    let initiator = worker("initiator");
    node.on_message(
        worker("someone"),
        roll_call_message(roll_call("dup-call", initiator.clone(), vec![])),
    );
    network.pump();
    let first_delivery = network.poll_inbox(succ1.clone());
    assert_eq!(
        first_delivery.len(),
        1,
        "first delivery must forward normally"
    );

    // A duplicated delivery is a no-op: no re-append, no re-forward.
    node.on_message(
        worker("someone"),
        roll_call_message(roll_call("dup-call", initiator, vec![])),
    );
    network.pump();
    let second_delivery = network.poll_inbox(succ1);
    assert!(
        second_delivery.is_empty(),
        "duplicate roll_call_id must not be forwarded again"
    );
}

// A single-member electorate: quorum 1 is met by the node's own observation
// and the self-vote alone wins, so the node goes all the way to `Leader` (via
// an unobservable `LeaderReconciling`).
#[test]
fn tick_reaching_quorum_of_one_transitions_self_to_leader() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let solo = worker("solo");
    let network = make_network(&clock, std::slice::from_ref(&solo));
    let mut node = make_node_with_ring(
        &clock,
        &network,
        solo.clone(),
        std::slice::from_ref(&solo),
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));
    node.tick(); // Active -> LeaderSuspect
    assert_eq!(node.state(), WorkerState::LeaderSuspect);

    node.tick();
    assert_eq!(node.state(), WorkerState::Leader);
}

#[test]
fn on_roll_call_quorum_reached_but_not_winner_forwards_without_becoming_candidate() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);

    // Assign the node under test to whichever of two workers loses the
    // priority comparison, so the "not the winner" branch is exercised
    // deterministically.
    let candidate_a = worker("candidate-a");
    let candidate_b = worker("candidate-b");
    let next_term = 1;
    let winner = predict_winner(
        &shard(SHARD),
        0,
        next_term,
        &[candidate_a.clone(), candidate_b.clone()],
    );
    let (self_id, other_id) = if winner == candidate_a {
        (candidate_b, candidate_a)
    } else {
        (candidate_a, candidate_b)
    };

    let network = make_network(&clock, &[self_id.clone(), other_id.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), other_id.clone()],
        suspect_timeout,
    );

    // Start the node's own roll call (its observation alone is short of quorum 2) and drain the forward.
    clock.advance(Duration::from_ticks(11));
    node.tick();
    node.tick();
    assert_eq!(node.state(), WorkerState::RollCall);
    network.pump();
    network.poll_inbox(other_id.clone());

    // A separate roll call already carrying other_id's observation brings responses to quorum.
    let call = roll_call(
        "external-call-1",
        other_id.clone(),
        vec![observation(other_id.clone(), 0)],
    );
    node.on_message(other_id.clone(), roll_call_message(call));

    assert_eq!(node.state(), WorkerState::RollCall);

    // The call is still forwarded, now carrying both observations.
    network.pump();
    let inbox = network.poll_inbox(other_id);
    assert_eq!(inbox.len(), 1);
    match &inbox[0].1.payload {
        Some(election_message::Payload::RollCall(forwarded)) => {
            assert_eq!(forwarded.roll_call_id, "external-call-1");
            assert_eq!(forwarded.responses.len(), 2);
        }
        other => panic!("expected RollCall payload, got {other:?}"),
    }
}

// README §12.4: forward whenever quorum wasn't reached or this node isn't the
// winner. A winner that can't accept (still `Active`, or already `Candidate`)
// must forward too.

#[test]
fn process_roll_call_when_winning_but_still_active_forwards_instead_of_dropping() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);

    // As above but inverted: the node under test is the winner, yet cannot currently accept.
    let candidate_a = worker("candidate-a");
    let candidate_b = worker("candidate-b");
    let next_term = 1; // both observations below carry highest_term_seen: 0.
    let winner = predict_winner(
        &shard(SHARD),
        0,
        next_term,
        &[candidate_a.clone(), candidate_b.clone()],
    );
    let (self_id, other_id) = if winner == candidate_a {
        (candidate_a, candidate_b)
    } else {
        (candidate_b, candidate_a)
    };

    let network = make_network(&clock, &[self_id.clone(), other_id.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), other_id.clone()],
        suspect_timeout,
    );

    // Never ticked: the node is still Active when the call reaches it.
    assert_eq!(node.state(), WorkerState::Active);

    let call = roll_call(
        "external-call-1",
        other_id.clone(),
        vec![observation(other_id.clone(), 0)],
    );
    node.on_message(other_id.clone(), roll_call_message(call));

    // The winner, but the table has no (Active, Candidate) edge: it must stay Active...
    assert_eq!(node.state(), WorkerState::Active);

    // ...and still forward the call.
    network.pump();
    let inbox = network.poll_inbox(other_id);
    assert_eq!(
        inbox.len(),
        1,
        "a roll call this node can't yet accept winning must still be forwarded onward"
    );
    match &inbox[0].1.payload {
        Some(election_message::Payload::RollCall(forwarded)) => {
            assert_eq!(forwarded.roll_call_id, "external-call-1");
            assert_eq!(forwarded.responses.len(), 2);
        }
        other => panic!("expected RollCall payload, got {other:?}"),
    }
}

#[test]
fn process_roll_call_when_winning_but_already_candidate_forwards_instead_of_dropping() {
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
    let (self_id, other_id) = if winner == candidate_a {
        (candidate_a, candidate_b)
    } else {
        (candidate_b, candidate_a)
    };

    let network = make_network(&clock, &[self_id.clone(), other_id.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), other_id.clone()],
        suspect_timeout,
    );

    // Start the node's own roll call and drain the forward.
    clock.advance(Duration::from_ticks(11));
    node.tick();
    node.tick();
    assert_eq!(node.state(), WorkerState::RollCall);
    network.pump();
    network.poll_inbox(other_id.clone());

    // Round 1: a call reaching quorum with this node as the winner, while it is RollCall, makes it Candidate.
    let round_1 = roll_call(
        "round-1",
        other_id.clone(),
        vec![observation(other_id.clone(), 0)],
    );
    node.on_message(other_id.clone(), roll_call_message(round_1));
    assert_eq!(node.state(), WorkerState::Candidate);

    // Drain the VoteRequest sent on becoming Candidate.
    network.pump();
    network.poll_inbox(other_id.clone());

    // Round 2: another call reaches quorum with this node still the winner,
    // but it is now Candidate. The table has no (Candidate, Candidate) or
    // (Candidate, Active) edge, so the state stays and the call must still be
    // forwarded.
    let round_2 = roll_call(
        "round-2",
        other_id.clone(),
        vec![observation(other_id.clone(), 0)],
    );
    node.on_message(other_id.clone(), roll_call_message(round_2));

    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "must remain Candidate, unaffected by an already-won subsequent round"
    );

    network.pump();
    let inbox = network.poll_inbox(other_id);
    assert_eq!(
        inbox.len(),
        1,
        "round 2's roll call must still be forwarded onward, not dropped"
    );
    match &inbox[0].1.payload {
        Some(election_message::Payload::RollCall(forwarded)) => {
            assert_eq!(forwarded.roll_call_id, "round-2");
        }
        other => panic!("expected RollCall payload, got {other:?}"),
    }
}

#[test]
fn forwarding_falls_back_to_second_successor_when_first_is_unreachable() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let succ1 = worker("w2");
    let succ2 = worker("w3");
    let network = make_network(&clock, &[self_id.clone(), succ1.clone(), succ2.clone()]);
    network.partition(
        BTreeSet::from([self_id.clone()]),
        BTreeSet::from([succ1.clone()]),
    );

    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), succ1.clone(), succ2.clone()],
        suspect_timeout,
    );

    let initiator = worker("initiator");
    node.on_message(
        worker("someone"),
        roll_call_message(roll_call("call-1", initiator, vec![])),
    );

    network.pump();
    assert!(
        network.poll_inbox(succ1).is_empty(),
        "must not send to the unreachable first successor"
    );
    let inbox = network.poll_inbox(succ2);
    assert_eq!(
        inbox.len(),
        1,
        "must fall back to the second, reachable successor"
    );
}

#[test]
fn on_leader_ack_returns_roll_call_to_active_but_never_from_candidate() {
    // Part A: a valid ack while RollCall transitions to Active.
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let successor = worker("w2");
    let network = make_network(&clock, &[self_id.clone(), successor.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), successor.clone()],
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));
    node.tick();
    node.tick();
    assert_eq!(node.state(), WorkerState::RollCall);

    node.on_leader_ack(&heartbeat_ack(SHARD, 0, 0));
    assert_eq!(node.state(), WorkerState::Active);

    // Part B: a Candidate is unaffected by an ack (no (Candidate, Active)
    // edge). Staying Candidate needs a 2-member electorate, since with one
    // member the self-vote wins outright.
    let clock2 = FakeClock::new();
    let candidate_a = worker("candidate-a");
    let candidate_b = worker("candidate-b");
    let next_term = 1; // both observations below carry highest_term_seen: 0.
    let winner = predict_winner(
        &shard(SHARD),
        0,
        next_term,
        &[candidate_a.clone(), candidate_b.clone()],
    );
    let (solo, peer) = if winner == candidate_a {
        (candidate_a, candidate_b)
    } else {
        (candidate_b, candidate_a)
    };
    let network2 = make_network(&clock2, &[solo.clone(), peer.clone()]);
    let mut candidate_node = make_node_with_ring(
        &clock2,
        &network2,
        solo.clone(),
        &[solo.clone(), peer.clone()],
        suspect_timeout,
    );

    // Start the node's own roll call and drain the forward.
    clock2.advance(Duration::from_ticks(11));
    candidate_node.tick();
    candidate_node.tick();
    assert_eq!(candidate_node.state(), WorkerState::RollCall);
    network2.pump();
    network2.poll_inbox(peer.clone());

    // A call reaching quorum 2 with this node as the winner makes it Candidate;
    // the self-vote (1) is short of vote quorum 2.
    let round_1 = roll_call("round-1", peer.clone(), vec![observation(peer.clone(), 0)]);
    candidate_node.on_message(peer.clone(), roll_call_message(round_1));
    assert_eq!(candidate_node.state(), WorkerState::Candidate);
    network2.pump();
    network2.poll_inbox(peer); // drain the VoteRequest it sends.

    candidate_node.on_leader_ack(&heartbeat_ack(SHARD, 0, 0));
    assert_eq!(
        candidate_node.state(),
        WorkerState::Candidate,
        "a valid ack must never move a Candidate node to Active"
    );
}

/// A 5-member electorate (quorum 3) whose node under test always wins at term
/// 1, already in `RollCall`. Returns `(node, self_id, other_members, network)`.
fn five_member_roll_call_node() -> (
    WorkerNode<FakeClock, FakeNetwork, RingMembership, FakeCoordinationAuthority>,
    WorkerId,
    Vec<WorkerId>,
    FakeNetwork,
) {
    let clock = FakeClock::new();
    let members: Vec<WorkerId> = (1..=5).map(|i| worker(&format!("member-{i}"))).collect();
    let self_id = predict_winner(&shard(SHARD), 0, 1, &members);
    let others: Vec<WorkerId> = members.iter().filter(|m| **m != self_id).cloned().collect();

    let mut registered = members.clone();
    registered.extend((1..=8).map(|i| worker(&format!("outsider-{i}"))));
    let network = make_network(&clock, &registered);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &members,
        Duration::from_ticks(10),
    );

    clock.advance(Duration::from_ticks(11));
    node.tick();
    node.tick();
    assert_eq!(node.state(), WorkerState::RollCall);
    network.pump();
    for member in &others {
        network.poll_inbox(member.clone());
    }
    (node, self_id, others, network)
}

#[test]
fn duplicate_observations_count_once_towards_quorum() {
    let (mut node, self_id, others, _network) = five_member_roll_call_node();

    let call = roll_call(
        "duplicated",
        others[0].clone(),
        vec![
            observation(others[0].clone(), 0),
            observation(others[0].clone(), 0),
        ],
    );
    node.on_message(others[0].clone(), roll_call_message(call));

    assert_eq!(
        node.state(),
        WorkerState::RollCall,
        "{self_id:?} saw only 2 distinct members of a 5-member electorate, short of quorum 3"
    );
}

#[test]
fn observations_from_non_members_do_not_count_towards_quorum() {
    let (mut node, self_id, others, _network) = five_member_roll_call_node();
    let outsider = (1..=8)
        .map(|i| worker(&format!("outsider-{i}")))
        .find(|o| predict_winner(&shard(SHARD), 0, 1, &[self_id.clone(), o.clone()]) == self_id)
        .expect("some outsider must lose to the node under test");

    let call = roll_call(
        "with-outsider",
        others[0].clone(),
        vec![observation(others[0].clone(), 0), observation(outsider, 0)],
    );
    node.on_message(others[0].clone(), roll_call_message(call));

    assert_eq!(
        node.state(),
        WorkerState::RollCall,
        "an outsider's observation must not supply the third response needed for quorum"
    );
}

#[test]
fn stopped_node_drops_roll_calls_without_forwarding() {
    let clock = FakeClock::new();
    let members = [worker("w1"), worker("w2"), worker("w3")];
    let network = make_network(&clock, &members);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        members[0].clone(),
        &members,
        Duration::from_ticks(10),
    );

    node.begin_drain();
    assert_eq!(node.state(), WorkerState::Stopped);
    network.pump();
    for member in &members[1..] {
        network.poll_inbox(member.clone());
    }

    node.on_message(
        members[1].clone(),
        roll_call_message(roll_call("after-stop", members[1].clone(), vec![])),
    );

    network.pump();
    for member in &members[1..] {
        assert!(network.poll_inbox(member.clone()).is_empty());
    }
    assert_eq!(node.state(), WorkerState::Stopped);
}

#[test]
fn heartbeat_ack_from_someone_other_than_the_named_leader_is_ignored() {
    let (mut node, _self_id, _others, _network) = five_member_roll_call_node();
    let ack = ElectionMessage {
        payload: Some(election_message::Payload::HeartbeatAck(heartbeat_ack(
            SHARD, 0, 0,
        ))),
    };

    node.on_message(worker("impostor"), ack.clone());
    assert_eq!(
        node.state(),
        WorkerState::RollCall,
        "ack sent by a non-leader must be ignored"
    );

    node.on_message(worker("leader-1"), ack);
    assert_eq!(
        node.state(),
        WorkerState::Active,
        "ack sent by the leader it names is honoured"
    );
}

/// The node's state after it starts a roll call in a two-member electorate and
/// receives `other`'s observation, ranking candidates with `hash_function`.
fn state_after_two_member_roll_call(
    me: &WorkerId,
    other: &WorkerId,
    hash_function: HashFunction,
) -> WorkerState {
    let clock = FakeClock::new();
    let members = [me.clone(), other.clone()];
    let network = make_network(&clock, &members);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        me.clone(),
        &members,
        Duration::from_ticks(10),
    )
    .with_hash_function(hash_function);

    clock.advance(Duration::from_ticks(11));
    node.tick();
    node.tick();
    assert_eq!(node.state(), WorkerState::RollCall);

    let call = roll_call(
        "external",
        other.clone(),
        vec![observation(other.clone(), 0)],
    );
    node.on_message(other.clone(), roll_call_message(call));
    node.state()
}

#[test]
fn candidate_choice_follows_the_configured_hash_function() {
    let default_hash = HashFunction::default();
    let sha3 = HashFunction::new::<sha3::Sha3_256>();
    let winner = |hash: &HashFunction, x: &WorkerId, y: &WorkerId| {
        let priority = |w: &WorkerId| candidate_priority(hash, &shard(SHARD), 0, 1, w);
        if priority(x) > priority(y) {
            x.clone()
        } else {
            y.clone()
        }
    };

    let labels: Vec<WorkerId> = (1..=8).map(|i| worker(&format!("w{i}"))).collect();
    let (x, y) = labels
        .iter()
        .flat_map(|x| labels.iter().map(move |y| (x, y)))
        .find(|(x, y)| x < y && winner(&default_hash, x, y) != winner(&sha3, x, y))
        .expect("some pair must be ranked differently by the two hash functions");
    let sha3_winner = winner(&sha3, x, y);
    let other = if sha3_winner == *x { y } else { x };

    assert_eq!(
        state_after_two_member_roll_call(&sha3_winner, other, sha3),
        WorkerState::Candidate
    );
    assert_eq!(
        state_after_two_member_roll_call(&sha3_winner, other, default_hash),
        WorkerState::RollCall,
        "under the default hash function this node is not the winner and only forwards"
    );
}

#[test]
fn roll_call_for_another_shard_or_recovery_epoch_is_ignored() {
    let clock = FakeClock::new();
    let self_id = worker("w1");
    let succ = worker("w2");
    let network = make_network(&clock, &[self_id.clone(), succ.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id, succ.clone()],
        Duration::from_ticks(10),
    );

    let mut other_shard = roll_call("call-1", worker("initiator"), vec![]);
    other_shard.shard_id = Some(shard("other-shard").into());
    node.on_message(worker("someone"), roll_call_message(other_shard));

    let mut other_epoch = roll_call("call-2", worker("initiator"), vec![]);
    other_epoch.recovery_epoch = 1;
    node.on_message(worker("someone"), roll_call_message(other_epoch));

    network.pump();
    assert!(
        network.poll_inbox(succ).is_empty(),
        "a roll call for another shard or epoch must be neither answered nor forwarded"
    );
}
