//! The roll call, from both sides: the initiator that collects replies and
//! ignores those not addressed to it, the answerers' hold on their own calls
//! after answering, the roll calls a node drops, and what an initiator does
//! with a refusal from another recovery epoch or lineage.

use crate::support::builders::{
    ack_message, leader_ack, configuration_of, g0, message, past_any_suspicion, roll_call,
    roll_call_message, roll_call_reply, shard, timings, worker,
};
use crate::support::builders::checked;
use crate::support::clock::FakeClock;
use kabudachi_core::protocol::checked::{Checked, CheckedPayload};
use crate::support::node::{
    TestNode, close_roll_call, connect, deliver, elect, published_roll_calls,
    rejects_sent_to, sent_to, start_roll_call, state_changes, tick, voter_node,
};
use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{Entry, Identity, Input, KnownConfiguration, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    ElectionMessage, ElectionReject, ElectionRejectReason, KnownLeader,
    RollCall, RollCallReply, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Clock, Duration, Instant};

/// Every node here suspects its leader after this many ticks.
const SUSPECT: u64 = 10;

/// The replies among `outputs` addressed to `initiator`.
fn replies_to(outputs: &[Output], initiator: &WorkerId) -> Vec<Checked<RollCallReply>> {
    sent_to(outputs, initiator)
        .into_iter()
        .filter_map(|message| match checked(message).into_payload() {
            Some(CheckedPayload::RollCallReply(reply)) => Some(reply),
            _ => None,
        })
        .collect()
}

/// `me`, a voter of `voter_count`, whose leader contact has gone stale, so
/// it takes part in the next roll call it hears. It stays `Active`.
fn stale_voter(clock: &FakeClock, me: &WorkerId, voter_count: usize) -> TestNode {
    let node = voter_node(clock, me, voter_count, SUSPECT);
    clock.advance(past_any_suspicion(SUSPECT));
    node
}

/// `me`, a voter of `voter_count`, in `RollCall` with its own call just
/// published. Returns the node and its call.
fn initiator(clock: &FakeClock, me: &WorkerId, voter_count: usize) -> (TestNode, Checked<RollCall>) {
    let mut node = voter_node(clock, me, voter_count, SUSPECT);
    let outputs = start_roll_call(&mut node, clock, SUSPECT);
    let calls = published_roll_calls(&outputs);
    assert_eq!(calls.len(), 1, "setup invariant: one roll call published");
    (node, calls.into_iter().next().unwrap())
}

/// Closes `node`'s roll call at its deadline and returns what that asked for.
fn close(node: &mut TestNode, clock: &FakeClock) -> Vec<Output> {
    close_roll_call(node, clock, SUSPECT)
}

#[test]
fn a_reply_to_another_call_or_from_another_sender_is_ignored() {
    let clock = FakeClock::new();
    let (mut node, call) = initiator(&clock, &worker("w1"), 3);
    let p1 = worker("p1");
    let me = call.initiator_id();

    let wrong_term = roll_call_reply(&me, call.term + 1, &p1, Some(g0()));
    let wrong_initiator = roll_call_reply(&worker("someone"), call.term, &p1, Some(g0()));
    let mut wrong_shard = roll_call_reply(&me, call.term, &p1, Some(g0()));
    if let Some(election_message::Payload::RollCallReply(reply)) = &mut wrong_shard.payload {
        reply.shard_id = Some(shard("shard-2").into());
    }
    for reply in [wrong_term, wrong_initiator, wrong_shard] {
        deliver(&mut node, &p1, reply);
    }
    deliver(
        &mut node,
        &worker("impostor"),
        roll_call_reply(&me, call.term, &p1, Some(g0())),
    );

    assert_eq!(
        state_changes(&close(&mut node, &clock)),
        vec![WorkerState::NoQuorum]
    );
}

// ---- Holds ----

/// `me`, a stale voter already `LeaderSuspect`, that answers `initiator`'s
/// call for term 1. Returns the node and the instant it answered at.
fn suspecting_voter_that_answered(
    clock: &FakeClock,
    me: &WorkerId,
    initiator: &WorkerId,
) -> (TestNode, kabudachi_core::time::Instant) {
    let mut node = stale_voter(clock, me, 3);
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::LeaderSuspect, "setup invariant");
    let answered = deliver(
        &mut node,
        initiator,
        roll_call_message(roll_call(initiator, 1, &configuration_of(3), 0)),
    );
    assert_eq!(replies_to(&answered, initiator).len(), 1, "setup invariant");
    (node, clock.now())
}

fn base_deadline() -> Duration {
    timings(Duration::from_ticks(SUSPECT)).roll_call_deadline
}

/// How long answering a call holds a node back from its own.
fn hold_window() -> Duration {
    Duration::from_ticks(2 * base_deadline().as_ticks() + SUSPECT)
}

/// Ticks `node` from now, one tick of `clock` at a time, until it publishes a
/// roll call or `until` is reached; the instant it published at and its term.
fn first_call_before(
    node: &mut TestNode,
    clock: &FakeClock,
    until: Instant,
) -> Option<(Instant, u64)> {
    loop {
        if let Some(call) = published_roll_calls(&tick(node)).first() {
            return Some((clock.now(), call.term));
        }
        if clock.now() + Duration::from_ticks(1) >= until {
            return None;
        }
        clock.advance(Duration::from_ticks(1));
    }
}

// Answering a call holds a node back from its own, and callers taking turns
// chain those holds, which the episode's end (two hold windows after the
// first answer) cuts: the hold runs to the episode's end, or one minimum
// past it for an answer that comes after the hold's own cut, and an answer
// at the very instant the episode ends leaves the node a call of its own.
#[test]
fn answers_in_turn_hold_a_node_back_no_longer_than_its_episode_and_one_minimum_more() {
    let (first, second, third) = (worker("w1"), worker("w2"), worker("w4"));
    let window = hold_window().as_ticks();
    let span = 2 * base_deadline().as_ticks();
    let episode_end = 2 * window;
    let paced = window * 9 / 10;
    // (answers as (ticks after the first answer, term, caller), the instant
    // of the node's first call as ticks after it, and that call's term)
    let rows = [
        (
            vec![(paced, 2, &second), (2 * paced, 3, &first)],
            episode_end,
            4,
        ),
        (
            vec![
                (window - 1, 2, &second),
                (episode_end - 2, 3, &first),
                (episode_end + 1, 4, &second),
            ],
            episode_end + span,
            5,
        ),
        (
            vec![
                (paced, 2, &second),
                (2 * paced, 3, &first),
                (episode_end, 4, &third),
            ],
            episode_end,
            5,
        ),
    ];

    for (answers, called_at, term) in rows {
        let clock = FakeClock::new();
        let (mut node, start) = suspecting_voter_that_answered(&clock, &worker("w3"), &first);
        for (after, term, caller) in answers {
            clock.advance(start + Duration::from_ticks(after) - clock.now());
            let answered = deliver(
                &mut node,
                caller,
                roll_call_message(roll_call(caller, term, &configuration_of(3), 0)),
            );
            assert_eq!(replies_to(&answered, caller).len(), 1, "answered");
        }

        let called = first_call_before(
            &mut node,
            &clock,
            start + Duration::from_ticks(episode_end + span + 2),
        );

        assert_eq!(
            called,
            Some((start + Duration::from_ticks(called_at), term)),
            "the node calls once the hold ends, no sooner and no later"
        );
    }
}

// ---- Roll calls a node drops ----

#[test]
fn a_roll_call_a_node_cannot_use_is_dropped() {
    let clock = FakeClock::new();
    let initiator = worker("w1");
    let call = || roll_call(&initiator, 1, &configuration_of(3), 0);
    let mut other_shard = call();
    other_shard.shard_id = Some(shard("shard-2").into());
    // The epoch-1 node is built first, so the stale voters' clock advance
    // also makes its leader contact stale.
    let at_epoch_1 = voter_at_epoch_1(&clock, &worker("voter"), 3);
    let mut stopped = stale_voter(&clock, &worker("voter"), 3);
    let _ = stopped.step(Input::Drain);
    assert_eq!(stopped.state(), WorkerState::Stopped, "setup invariant");
    clock.advance(past_any_suspicion(SUSPECT));
    let rows = [
        ("another shard", stale_voter(&clock, &worker("voter"), 3), initiator.clone(), other_shard),
        ("a relay", stale_voter(&clock, &worker("voter"), 3), worker("relay"), call()),
        ("a stopped node", stopped, initiator.clone(), call()),
        (
            "a newer recovery epoch",
            at_epoch_1,
            initiator.clone(),
            roll_call(&initiator, 1, &Configuration::genesis(2), 0),
        ),
    ];

    for (name, mut node, from, call) in rows {
        let outputs = deliver(&mut node, &from, roll_call_message(call));

        assert!(outputs.is_empty(), "{name}: {outputs:?}");
    }
}

// ---- What a refusal tells the initiator ----

fn reject_message(
    call: &RollCall,
    rejecter: &WorkerId,
    reason: ElectionRejectReason,
    highest_term_seen: u64,
    leader: Option<(&WorkerId, u64)>,
) -> ElectionMessage {
    message(election_message::Payload::ElectionReject(ElectionReject {
        shard_id: Some(shard("shard-1").into()),
        term: call.term,
        initiator_id: call.initiator_id.clone(),
        rejecter_id: Some(rejecter.clone().into()),
        reason: reason as i32,
        highest_term_seen,
        configuration: Some((&configuration_of(3)).into()),
        leader: leader.map(|(leader, term)| KnownLeader {
            leader_id: Some(leader.clone().into()),
            term,
        }),
        recovery_epoch: Some(0),
        recovery_epoch_lineage: None,
    }))
}

/// `me`, a voter of `voters` at recovery epoch 1, whose leader contact has
/// not yet gone stale.
fn voter_at_epoch_1(clock: &FakeClock, me: &WorkerId, voters: usize) -> TestNode {
    let epoch_1 = Generation::new(1, 0, 0);
    WorkerNode::start(
        Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(SUSPECT)),
        },
        Entry::Known(KnownConfiguration {
            configuration: Configuration::single(Single {
                generation: epoch_1,
                base: epoch_1,
                voter_count: voters,
            }).expect("valid"),
            admission: Some(epoch_1),
        }),
        clock.clone(),
        None,
    )
    .0
}

// A refusal from a lower recovery
// epoch counts that epoch's terms, which order nothing here.
fn epoch_1_configuration(voters: usize) -> Configuration {
    let epoch_1 = Generation::new(1, 0, 0);
    Configuration::single(Single {
        generation: epoch_1,
        base: epoch_1,
        voter_count: voters,
    })
    .expect("valid")
}

/// `refusal` as `epoch` and `lineage` name the epoch its refuser stands in,
/// carrying `configuration` instead of the builder's, or none.
fn refusal_at(
    mut refusal: ElectionMessage,
    epoch: Option<u64>,
    lineage: Option<u64>,
    configuration: Option<Configuration>,
) -> ElectionMessage {
    let Some(election_message::Payload::ElectionReject(reject)) = refusal.payload.as_mut() else {
        panic!("reject_message builds a refusal");
    };
    reject.recovery_epoch = epoch;
    reject.recovery_epoch_lineage = lineage;
    reject.configuration = configuration.as_ref().map(Into::into);
    refusal
}

// A refusal that names no epoch (its rejecter has joined no shard) is read as
// this node's own; one from a lower epoch, or from another lineage at this
// node's own epoch number, counts terms and holds a configuration that mean
// nothing here and is dropped; one from a higher-numbered foreign epoch only
// names its leader, which the node heartbeats, and raises no term.
#[test]
fn a_refusal_is_placed_by_the_epoch_and_lineage_it_names() {
    // (what the refusal names, the term it carries and whether it names a
    // leader, then what the initiator holds: the highest term seen, its
    // state and whether it heartbeats the named leader)
    let rows = [
        (
            "a lower epoch with its configuration",
            Some(0),
            None,
            Some(configuration_of(3)),
            ElectionRejectReason::LeaderStillValid,
            (0, WorkerState::RollCall, false),
        ),
        (
            "a lower epoch without a configuration",
            Some(0),
            None,
            None,
            ElectionRejectReason::StaleTerm,
            (0, WorkerState::RollCall, false),
        ),
        (
            "another lineage at this epoch number",
            Some(1),
            Some(1),
            Some(epoch_1_configuration(3)),
            ElectionRejectReason::LeaderStillValid,
            (0, WorkerState::RollCall, false),
        ),
        (
            "a higher-numbered foreign epoch",
            Some(2),
            Some(1),
            None,
            ElectionRejectReason::LeaderStillValid,
            (0, WorkerState::RollCall, true),
        ),
        (
            "no epoch",
            None,
            None,
            None,
            ElectionRejectReason::StaleTerm,
            (5, WorkerState::LeaderSuspect, false),
        ),
    ];

    for (name, epoch, lineage_offset, configuration, reason, (term_seen, state, heartbeats)) in
        rows
    {
        let clock = FakeClock::new();
        let mut node = voter_at_epoch_1(&clock, &worker("w1"), 3);
        let call = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
        let (rejecter, their_leader) = (worker("w2"), worker("their-leader"));
        let named = (reason == ElectionRejectReason::LeaderStillValid)
            .then_some((&their_leader, 9));
        let own_lineage = node.recovery_lineage().expect("a joined node has one");
        let refusal = refusal_at(
            reject_message(&call, &rejecter, reason, if named.is_some() { 9 } else { 5 }, named),
            epoch,
            lineage_offset.map(|offset| own_lineage + offset),
            configuration,
        );

        let configuration_before = node.configuration().cloned();

        let outputs = deliver(&mut node, &rejecter, refusal);

        assert_eq!(node.configuration(), configuration_before.as_ref(), "{name}");
        assert_eq!(node.highest_term_seen(), term_seen, "{name}");
        assert_eq!(node.state(), state, "{name}");
        assert_eq!(!sent_to(&outputs, &their_leader).is_empty(), heartbeats, "{name}");
    }
}

/// A node cut off from its leader's acks while a new one is elected keeps
/// heartbeating the old leader and rolling calls under a configuration older
/// than everyone's. The followers refuse it for its stale generation and the
/// leader for being no voter, so each refusal must name the leader, or the
/// stranded node never learns who to follow.
#[test]
fn a_refusal_names_the_leader_whatever_its_reason() {
    let clock = FakeClock::new();
    let stranded = worker("stranded");
    let call = roll_call(&stranded, 5, &configuration_of(3), 0);

    let leader = worker("leader");
    let (peers, mut leading) = (
        [worker("p1"), worker("p2")],
        voter_node(&clock, &leader, 3, SUSPECT),
    );
    connect(&mut leading, &peers);
    elect(&mut leading, &clock, SUSPECT, &peers);
    assert_eq!(leading.state(), WorkerState::Leader, "setup invariant");

    let follower = worker("follower");
    let mut following = voter_node(&clock, &follower, 3, SUSPECT);
    let newer = Configuration::single(Single {
        generation: Generation::new(0, 0, 1),
        base: g0(),
        voter_count: 3,
    })
    .expect("valid");
    deliver(
        &mut following,
        &leader,
        ack_message(leader_ack(&leader, 0, &newer, Some(g0()))),
    );

    for (name, mut node, reason) in [
        ("a follower", following, ElectionRejectReason::StaleGeneration),
        ("the leader", leading, ElectionRejectReason::NotEligible),
    ] {
        let outputs = deliver(&mut node, &stranded, roll_call_message(call.clone()));

        let refusals = rejects_sent_to(&outputs, &stranded);
        assert_eq!(refusals.len(), 1, "{name}");
        assert_eq!(refusals[0].reason(), reason, "{name}");
        assert_eq!(
            refusals[0].named_leader().map(|(id, _)| id),
            Some(leader.clone()),
            "{name}"
        );
    }
}

/// Decode refuses a term of `u64::MAX` from any peer, so a node reaches
/// `u64::MAX` only by contesting it itself: a peer that names `u64::MAX - 1`
/// makes its next roll call contest `u64::MAX`. A node whose highest term
/// seen is already `u64::MAX` has no next term to contest, and panics rather
/// than wrap back to term 0.
#[test]
#[should_panic(expected = "term overflowed u64::MAX")]
fn a_node_that_has_seen_the_highest_representable_term_panics_rather_than_contest_term_zero() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    let rejecter = worker("w2");
    deliver(
        &mut node,
        &rejecter,
        reject_message(
            &roll_call(&me, 1, &configuration_of(3), 0),
            &rejecter,
            ElectionRejectReason::StaleTerm,
            u64::MAX - 1,
            None,
        ),
    );

    // The first roll call contests `u64::MAX`; the second has no term left.
    start_roll_call(&mut node, &clock, SUSPECT);
    close_roll_call(&mut node, &clock, SUSPECT);
    start_roll_call(&mut node, &clock, SUSPECT);
}
