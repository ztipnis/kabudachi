//! The roll call (ADR-0001 decisions 3 to 6), from both sides: the
//! initiator that publishes it and collects replies until its deadline,
//! when a returning quorum makes it the candidate, and the workers that
//! answer it, pass over a repeat of it, or refuse it and say why.

use crate::support::builders::{
    ack_message, configuration_of, g0, leader_ack, message, past_any_suspicion, roll_call,
    roll_call_message, roll_call_reply, shard, timings, vote_request, vote_request_message, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{
    TestNode, close_roll_call, deliver, published_roll_calls, recipients_of, rejects_sent_to, sent,
    sent_to, start_roll_call, state_changes, tick, voter_node,
};
use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::election::{Entry, Identity, Input, KnownConfiguration, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    ElectionMessage, ElectionReject, ElectionRejectReason, KnownLeader, RollCall, RollCallReply,
    election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Clock, Duration};

/// Every node here suspects its leader after this many ticks.
const SUSPECT: u64 = 10;

fn is_vote_request(payload: &election_message::Payload) -> bool {
    matches!(payload, election_message::Payload::VoteRequest(_))
}

/// The replies among `outputs` addressed to `initiator`.
fn replies_to(outputs: &[Output], initiator: &WorkerId) -> Vec<RollCallReply> {
    sent_to(outputs, initiator)
        .into_iter()
        .filter_map(|message| match message.payload {
            Some(election_message::Payload::RollCallReply(reply)) => Some(reply),
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
fn initiator(clock: &FakeClock, me: &WorkerId, voter_count: usize) -> (TestNode, RollCall) {
    let mut node = voter_node(clock, me, voter_count, SUSPECT);
    let outputs = start_roll_call(&mut node, clock, SUSPECT);
    let calls = published_roll_calls(&outputs);
    assert_eq!(calls.len(), 1, "setup invariant: one roll call published");
    (node, calls.into_iter().next().unwrap())
}

fn reply_from(
    node: &mut TestNode,
    call: &RollCall,
    responder: &WorkerId,
    admission: Option<Generation>,
) -> Vec<Output> {
    deliver(
        node,
        responder,
        roll_call_reply(&call.initiator_id(), call.term, responder, admission),
    )
}

// ---- The initiator ----

#[test]
fn a_suspecting_voter_publishes_a_roll_call_for_the_next_term_under_its_configuration() {
    let clock = FakeClock::new();
    clock.set_wall_clock_millis(1_700);
    let me = worker("w1");
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    clock.advance(past_any_suspicion(SUSPECT));
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::LeaderSuspect);

    let outputs = tick(&mut node);

    assert_eq!(state_changes(&outputs), vec![WorkerState::RollCall]);
    assert_eq!(
        published_roll_calls(&outputs),
        vec![roll_call(&me, 1, &configuration_of(3), 1_700)],
        "term = highest term seen + 1, the node's configuration, its wall clock, itself as \
         initiator and no address"
    );
    assert!(
        sent(&outputs).is_empty(),
        "a roll call is published, not sent"
    );
}

#[test]
fn a_roll_call_contests_the_term_after_the_highest_the_node_has_seen() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    deliver(
        &mut node,
        &worker("leader"),
        ack_message(leader_ack(
            &worker("leader"),
            4,
            &configuration_of(3),
            Some(g0()),
        )),
    );

    let outputs = start_roll_call(&mut node, &clock, SUSPECT);

    assert_eq!(published_roll_calls(&outputs)[0].term, 5);
}

/// Closes `node`'s roll call at its deadline and returns what that asked for.
fn close(node: &mut TestNode, clock: &FakeClock) -> Vec<Output> {
    close_roll_call(node, clock, SUSPECT)
}

#[test]
fn a_pending_respondent_is_a_new_voter_and_does_not_count_toward_the_returning_quorum() {
    let clock = FakeClock::new();
    let (pending, returning) = (worker("pending"), worker("returning"));
    let (mut short, short_call) = initiator(&clock, &worker("w1"), 3);
    reply_from(&mut short, &short_call, &pending, None);
    assert_eq!(
        state_changes(&close(&mut short, &clock)),
        vec![WorkerState::NoQuorum]
    );

    let clock = FakeClock::new();
    let (mut node, call) = initiator(&clock, &worker("w1"), 3);
    reply_from(&mut node, &call, &pending, None);
    reply_from(&mut node, &call, &returning, Some(g0()));
    let stood = close(&mut node, &clock);

    assert_eq!(node.state(), WorkerState::Candidate);
    assert_eq!(
        recipients_of(&stood, is_vote_request),
        vec![pending, returning],
        "a new voter is still asked for its vote"
    );
}

#[test]
fn a_respondent_admitted_outside_the_configuration_is_no_returning_voter() {
    let clock = FakeClock::new();
    let (mut node, call) = initiator(&clock, &worker("w1"), 3);

    reply_from(
        &mut node,
        &call,
        &worker("left-out"),
        Some(Generation::new(0, 0, 7)),
    );

    assert_eq!(
        state_changes(&close(&mut node, &clock)),
        vec![WorkerState::NoQuorum]
    );
}

#[test]
fn a_duplicate_reply_counts_once() {
    let clock = FakeClock::new();
    let (mut node, call) = initiator(&clock, &worker("w1"), 5);
    let p1 = worker("p1");

    reply_from(&mut node, &call, &p1, Some(g0()));
    reply_from(&mut node, &call, &p1, Some(g0()));

    assert_eq!(
        state_changes(&close(&mut node, &clock)),
        vec![WorkerState::NoQuorum]
    );
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

#[test]
fn a_reply_that_arrives_once_the_initiator_stands_is_asked_for_its_vote() {
    let clock = FakeClock::new();
    let (mut node, call) = initiator(&clock, &worker("w1"), 5);
    reply_from(&mut node, &call, &worker("p1"), Some(g0()));
    reply_from(&mut node, &call, &worker("p2"), Some(g0()));
    close(&mut node, &clock);
    assert_eq!(node.state(), WorkerState::Candidate, "setup invariant");

    let late = reply_from(&mut node, &call, &worker("p3"), Some(g0()));

    assert_eq!(recipients_of(&late, is_vote_request), vec![worker("p3")]);
}

#[test]
fn a_genesis_node_wins_its_own_roll_call_at_its_deadline() {
    let clock = FakeClock::new();
    let me = worker("founder");
    let mut node: TestNode = WorkerNode::start(
        Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(SUSPECT)),
        },
        Entry::Founding {
            recovery_epoch: RecoveryEpoch::new(2, 9),
            registered_at: None,
        },
        clock.clone(),
        None,
    )
    .0;
    assert_eq!(node.recovery_epoch(), 2);
    assert_eq!(node.recovery_lineage(), Some(9));

    let started = start_roll_call(&mut node, &clock, SUSPECT);
    assert_eq!(state_changes(&started), vec![WorkerState::RollCall]);

    let outputs = close(&mut node, &clock);

    assert_eq!(
        state_changes(&outputs),
        vec![
            WorkerState::Candidate,
            WorkerState::LeaderReconciling,
            WorkerState::Leader,
        ]
    );
    assert_eq!(node.term(), 1);
    assert_eq!(node.known_leader(), Some((me, 1)));
}

// ---- Suppression ----

#[test]
fn a_node_that_accepted_a_roll_call_starts_none_of_its_own_until_that_calls_vote_could_have_ended()
{
    let clock = FakeClock::new();
    let me = worker("w2");
    let mut node = stale_voter(&clock, &me, 3);
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::LeaderSuspect);
    let other = worker("w1");
    deliver(
        &mut node,
        &other,
        roll_call_message(roll_call(&other, 1, &configuration_of(3), 0)),
    );
    // The call closes within a deadline, and its candidate's vote runs for
    // up to another.
    let accepted_at = clock.now();
    let deadline = timings(Duration::from_ticks(SUSPECT)).roll_call_deadline;
    let election_ends = accepted_at + deadline + deadline;

    let suppressed = node.step(Input::Tick);

    assert_eq!(node.state(), WorkerState::LeaderSuspect);
    assert!(published_roll_calls(&suppressed.outputs).is_empty());
    assert_eq!(suppressed.next_deadline, Some(election_ends));

    clock.advance(election_ends - clock.now());
    let started = tick(&mut node);

    assert_eq!(state_changes(&started), vec![WorkerState::RollCall]);
    assert_eq!(
        published_roll_calls(&started)[0].term,
        2,
        "the term after the call it accepted"
    );
}

#[test]
fn a_repeat_of_an_accepted_roll_call_does_not_extend_its_suppression() {
    let clock = FakeClock::new();
    let me = worker("w2");
    let mut node = stale_voter(&clock, &me, 3);
    tick(&mut node);
    let other = worker("w1");
    let call = roll_call(&other, 1, &configuration_of(3), 0);
    deliver(&mut node, &other, roll_call_message(call.clone()));
    let deadline = timings(Duration::from_ticks(SUSPECT)).roll_call_deadline;
    let election_ends = clock.now() + deadline + deadline;
    clock.advance(Duration::from_ticks(1));

    let repeated = deliver(&mut node, &other, roll_call_message(call));
    let suppressed = node.step(Input::Tick);

    assert!(sent(&repeated).is_empty(), "a repeat is not answered again");
    assert_eq!(suppressed.next_deadline, Some(election_ends));
}

#[test]
fn a_roll_call_contests_the_term_after_the_latest_roll_call_the_node_accepted() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("w2"), 3);
    let other = worker("w1");
    deliver(
        &mut node,
        &other,
        roll_call_message(roll_call(&other, 4, &configuration_of(3), 0)),
    );

    let outputs = start_roll_call(&mut node, &clock, SUSPECT);

    assert_eq!(published_roll_calls(&outputs)[0].term, 5);
}

#[test]
fn a_roll_call_that_failed_does_not_hold_its_term() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    let first = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
    // Its leader was alive after all: the call gathers no quorum.
    let leader = worker("leader");
    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 0, &configuration_of(3), Some(g0()))),
    );
    assert_eq!(node.state(), WorkerState::Active, "setup invariant");

    let second = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);

    assert_eq!((first.term, second.term), (1, 2));
}

#[test]
fn a_roll_call_the_node_refused_does_not_suppress_its_own() {
    let clock = FakeClock::new();
    let mut node = voter_node(&clock, &worker("w2"), 3, SUSPECT);
    let other = worker("w1");
    // Its leader contact is still fresh, so it refuses the call.
    let refused = deliver(
        &mut node,
        &other,
        roll_call_message(roll_call(&other, 1, &configuration_of(3), 0)),
    );
    assert_eq!(
        rejects_sent_to(&refused, &other).len(),
        1,
        "setup invariant"
    );

    let outputs = start_roll_call(&mut node, &clock, SUSPECT);

    assert_eq!(published_roll_calls(&outputs).len(), 1);
}

// ---- The tie-break ----

#[test]
fn an_initiator_that_hears_a_better_call_for_its_term_answers_it_and_abandons_its_own() {
    let clock = FakeClock::new();
    clock.set_wall_clock_millis(500);
    let (mut node, own) = initiator(&clock, &worker("w2"), 3);
    let deadline = clock.now() + timings(Duration::from_ticks(SUSPECT)).roll_call_deadline;
    let better = worker("w1");

    let outputs = deliver(
        &mut node,
        &better,
        roll_call_message(roll_call(&better, own.term, &configuration_of(3), 500)),
    );

    let replies = replies_to(&outputs, &better);
    assert_eq!(replies.len(), 1, "it answers the better call");
    assert_eq!(replies[0].admission(), Some(g0()));
    assert_eq!(node.state(), WorkerState::RollCall, "it stays RollCall");
    reply_from(&mut node, &own, &worker("w3"), Some(g0()));
    clock.advance(deadline - clock.now());
    let gave_up = node.step(Input::Tick);
    assert_eq!(
        state_changes(&gave_up.outputs),
        vec![WorkerState::LeaderSuspect],
        "an abandoned call collects no more replies and never stands"
    );
    assert!(
        gave_up
            .next_deadline
            .is_some_and(|retry| retry > deadline + Duration::from_ticks(SUSPECT)),
        "it suspects again a full suspicion timeout after giving up: {gave_up:?}"
    );
}

#[test]
fn an_earlier_timestamp_beats_a_lower_worker_id() {
    let clock = FakeClock::new();
    clock.set_wall_clock_millis(900);
    let (mut node, own) = initiator(&clock, &worker("w1"), 3);
    let earlier = worker("w9");

    let outputs = deliver(
        &mut node,
        &earlier,
        roll_call_message(roll_call(&earlier, own.term, &configuration_of(3), 100)),
    );

    assert_eq!(replies_to(&outputs, &earlier).len(), 1);
}

/// The one refusal among `outputs`, addressed to `initiator` and nothing
/// else to it, with the reason it gives.
fn the_refusal(outputs: &[Output], initiator: &WorkerId) -> ElectionRejectReason {
    let rejects = rejects_sent_to(outputs, initiator);
    assert_eq!(sent_to(outputs, initiator).len(), 1, "one message");
    assert_eq!(rejects.len(), 1, "a refusal");
    rejects[0].reason()
}

#[test]
fn an_initiator_refuses_a_worse_call_for_its_term_as_not_the_best_and_keeps_collecting() {
    let clock = FakeClock::new();
    let (mut node, own) = initiator(&clock, &worker("w1"), 3);
    let worse = worker("w2");

    let outputs = deliver(
        &mut node,
        &worse,
        roll_call_message(roll_call(&worse, own.term, &configuration_of(3), 0)),
    );

    assert_eq!(
        the_refusal(&outputs, &worse),
        ElectionRejectReason::NotBestRollCall
    );
    reply_from(&mut node, &own, &worker("w3"), Some(g0()));
    close(&mut node, &clock);
    assert_eq!(node.state(), WorkerState::Candidate);
}

#[test]
fn a_voter_answers_the_first_call_and_any_better_one_and_refuses_a_worse_one() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"), 3);
    let (first, better, worse) = (worker("w5"), worker("w2"), worker("w7"));

    let to_first = deliver(
        &mut node,
        &first,
        roll_call_message(roll_call(&first, 1, &configuration_of(3), 10)),
    );
    let to_better = deliver(
        &mut node,
        &better,
        roll_call_message(roll_call(&better, 1, &configuration_of(3), 10)),
    );
    let to_worse = deliver(
        &mut node,
        &worse,
        roll_call_message(roll_call(&worse, 1, &configuration_of(3), 10)),
    );

    assert_eq!(replies_to(&to_first, &first).len(), 1);
    assert_eq!(replies_to(&to_better, &better).len(), 1);
    assert_eq!(
        the_refusal(&to_worse, &worse),
        ElectionRejectReason::NotBestRollCall
    );
}

#[test]
fn a_refusal_as_not_the_best_call_deposes_no_worse_initiator() {
    let clock = FakeClock::new();
    let mut voter = stale_voter(&clock, &worker("voter"), 3);
    let (better, worse) = (worker("w1"), worker("w2"));
    deliver(
        &mut voter,
        &better,
        roll_call_message(roll_call(&better, 1, &configuration_of(3), 0)),
    );
    let (mut initiator, own) = initiator(&clock, &worse, 3);
    let refused = deliver(
        &mut voter,
        &worse,
        roll_call_message(roll_call(&worse, own.term, &configuration_of(3), 10)),
    );

    let outputs = deliver(
        &mut initiator,
        &worker("voter"),
        sent_to(&refused, &worse).remove(0),
    );

    assert!(state_changes(&outputs).is_empty());
    assert_eq!(initiator.state(), WorkerState::RollCall);
}

#[test]
fn answering_a_roll_call_does_not_raise_the_highest_term_seen() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"), 3);
    let initiator = worker("w1");
    deliver(
        &mut node,
        &initiator,
        roll_call_message(roll_call(&initiator, 3, &configuration_of(3), 0)),
    );
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::LeaderSuspect);

    // An ack at term 1 is accepted only if the highest term seen is still
    // at most 1.
    let leader = worker("leader");
    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 1, &configuration_of(3), Some(g0()))),
    );

    assert_eq!(node.state(), WorkerState::Active);
}

// ---- Refusals ----

/// The one refusal `node` sends `initiator` for `call`.
fn refusal_of(node: &mut TestNode, initiator: &WorkerId, call: RollCall) -> ElectionReject {
    let outputs = deliver(node, initiator, roll_call_message(call));
    let mut rejects = rejects_sent_to(&outputs, initiator);
    assert_eq!(
        rejects.len(),
        1,
        "exactly one refusal, sent to the initiator"
    );
    assert_eq!(
        sent_to(&outputs, initiator).len(),
        1,
        "and nothing else to the initiator"
    );
    rejects.remove(0)
}

#[test]
fn a_call_for_a_term_already_seen_is_refused_as_stale_naming_the_highest_term_seen() {
    let clock = FakeClock::new();
    let me = worker("voter");
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    let leader = worker("leader");
    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 2, &configuration_of(3), Some(g0()))),
    );
    clock.advance(past_any_suspicion(SUSPECT));
    let initiator = worker("w1");

    let reject = refusal_of(
        &mut node,
        &initiator,
        roll_call(&initiator, 2, &configuration_of(3), 0),
    );

    assert_eq!(reject.reason(), ElectionRejectReason::StaleTerm);
    assert_eq!(reject.term, 2);
    assert_eq!(reject.initiator_id(), initiator);
    assert_eq!(reject.rejecter_id(), me);
    assert_eq!(reject.shard_id(), shard("shard-1"));
    assert_eq!(reject.highest_term_seen, 2);
    assert_eq!(reject.configuration(), Some(configuration_of(3)));
    assert_eq!(reject.leader, None);
}

#[test]
fn a_call_under_an_older_configuration_is_refused_carrying_the_newer_one() {
    let clock = FakeClock::new();
    let newer = Configuration::single(Single {
        generation: Generation::new(0, 1, 2),
        base: g0(),
        voter_count: 3,
    });
    let mut node: TestNode = WorkerNode::start(
        Identity {
            id: worker("voter"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(SUSPECT)),
        },
        Entry::Known(KnownConfiguration {
            configuration: newer.clone(),
            admission: Some(g0()),
        }),
        clock.clone(),
        None,
    )
    .0;
    clock.advance(past_any_suspicion(SUSPECT));
    let initiator = worker("w1");

    let reject = refusal_of(
        &mut node,
        &initiator,
        roll_call(&initiator, 1, &configuration_of(3), 0),
    );

    assert_eq!(reject.reason(), ElectionRejectReason::StaleGeneration);
    assert_eq!(reject.configuration(), Some(newer));
}

#[test]
fn a_call_from_an_older_recovery_epoch_is_refused_and_one_from_a_newer_is_dropped() {
    let clock = FakeClock::new();
    let mut node: TestNode = WorkerNode::start(
        Identity {
            id: worker("voter"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(SUSPECT)),
        },
        Entry::Known(KnownConfiguration {
            configuration: Configuration::genesis(1),
            admission: Some(Generation::genesis(1)),
        }),
        clock.clone(),
        None,
    )
    .0;
    clock.advance(past_any_suspicion(SUSPECT));
    let initiator = worker("w1");

    let reject = refusal_of(
        &mut node,
        &initiator,
        roll_call(&initiator, 1, &Configuration::genesis(0), 0),
    );
    assert_eq!(reject.reason(), ElectionRejectReason::StaleGeneration);

    let newer = deliver(
        &mut node,
        &initiator,
        roll_call_message(roll_call(&initiator, 1, &Configuration::genesis(2), 0)),
    );
    assert!(sent(&newer).is_empty(), "a newer epoch's call is dropped");
}

#[test]
fn a_node_with_fresh_leader_contact_refuses_naming_its_leader_and_term() {
    let clock = FakeClock::new();
    let mut node = voter_node(&clock, &worker("voter"), 3, SUSPECT);
    let leader = worker("leader");
    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 0, &configuration_of(3), Some(g0()))),
    );
    let initiator = worker("w1");

    let reject = refusal_of(
        &mut node,
        &initiator,
        roll_call(&initiator, 1, &configuration_of(3), 0),
    );

    assert_eq!(reject.reason(), ElectionRejectReason::LeaderStillValid);
    let named = reject.leader.expect("the refusal names the leader");
    assert_eq!((named.leader_id(), named.term), (leader, 0));
}

#[test]
fn a_candidate_refuses_another_initiators_call_as_not_eligible() {
    let clock = FakeClock::new();
    let (mut node, call) = initiator(&clock, &worker("w1"), 5);
    reply_from(&mut node, &call, &worker("p1"), Some(g0()));
    reply_from(&mut node, &call, &worker("p2"), Some(g0()));
    close(&mut node, &clock);
    assert_eq!(node.state(), WorkerState::Candidate, "setup invariant");
    let rival = worker("w0");

    let reject = refusal_of(
        &mut node,
        &rival,
        roll_call(&rival, 2, &configuration_of(5), 0),
    );

    assert_eq!(reject.reason(), ElectionRejectReason::NotEligible);
}

#[test]
fn a_roll_call_for_another_shard_or_not_from_its_initiator_is_dropped() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"), 3);
    let initiator = worker("w1");
    let mut other_shard = roll_call(&initiator, 1, &configuration_of(3), 0);
    other_shard.shard_id = Some(shard("shard-2").into());

    let from_other_shard = deliver(&mut node, &initiator, roll_call_message(other_shard));
    let relayed = deliver(
        &mut node,
        &worker("relay"),
        roll_call_message(roll_call(&initiator, 1, &configuration_of(3), 0)),
    );

    assert!(sent(&from_other_shard).is_empty());
    assert!(sent(&relayed).is_empty());
}

#[test]
fn a_stopped_node_drops_roll_calls() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"), 3);
    let _ = node.step(Input::Drain);
    assert_eq!(node.state(), WorkerState::Stopped, "setup invariant");
    let initiator = worker("w1");

    let outputs = deliver(
        &mut node,
        &initiator,
        roll_call_message(roll_call(&initiator, 1, &configuration_of(3), 0)),
    );

    assert!(outputs.is_empty());
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
    }))
}

#[test]
fn a_refusal_naming_a_leader_makes_the_initiator_heartbeat_it_until_its_ack_returns_it() {
    let clock = FakeClock::new();
    let (mut node, call) = initiator(&clock, &worker("w1"), 3);
    let (rejecter, leader) = (worker("w2"), worker("leader"));

    let outputs = deliver(
        &mut node,
        &rejecter,
        reject_message(
            &call,
            &rejecter,
            ElectionRejectReason::LeaderStillValid,
            0,
            Some((&leader, 0)),
        ),
    );

    assert_eq!(node.state(), WorkerState::RollCall);
    let to_leader = sent_to(&outputs, &leader);
    assert_eq!(to_leader.len(), 1);
    assert!(matches!(
        to_leader[0].payload,
        Some(election_message::Payload::Heartbeat(_))
    ));

    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 0, &configuration_of(3), Some(g0()))),
    );
    assert_eq!(node.state(), WorkerState::Active);
    assert_eq!(node.known_leader(), Some((leader, 0)));
}

#[test]
fn a_refusal_naming_a_higher_term_raises_the_initiators_highest_term_seen() {
    let clock = FakeClock::new();
    let (mut node, call) = initiator(&clock, &worker("w1"), 3);
    let rejecter = worker("w2");
    deliver(
        &mut node,
        &rejecter,
        reject_message(&call, &rejecter, ElectionRejectReason::StaleTerm, 5, None),
    );

    // A term later than its own call's: it steps down.
    assert_eq!(node.state(), WorkerState::LeaderSuspect);

    // An ack below term 5 is now ignored; one at term 5 is accepted.
    let leader = worker("leader");
    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 4, &configuration_of(3), Some(g0()))),
    );
    assert_eq!(node.state(), WorkerState::LeaderSuspect);
    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 5, &configuration_of(3), Some(g0()))),
    );
    assert_eq!(node.state(), WorkerState::Active);
}

/// A leader named as still valid at a term below this node's term seen is
/// heartbeated (what that tells it: see `election_step_down_test`), but its
/// acks stay below the node's floor.
#[test]
fn a_refusal_naming_a_leader_from_an_older_term_is_heartbeated_but_not_followed() {
    let clock = FakeClock::new();
    let (mut node, call) = initiator(&clock, &worker("w1"), 3);
    let (rejecter, leader) = (worker("w2"), worker("leader"));

    let outputs = deliver(
        &mut node,
        &rejecter,
        reject_message(
            &call,
            &rejecter,
            ElectionRejectReason::LeaderStillValid,
            3,
            Some((&leader, 2)),
        ),
    );
    assert!(
        !sent_to(&outputs, &leader).is_empty(),
        "it heartbeats the named leader"
    );

    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 2, &configuration_of(3), Some(g0()))),
    );
    assert_ne!(node.state(), WorkerState::Active);
    assert_eq!(node.known_leader(), None);
}

/// Decode refuses a term of `u64::MAX` from any peer, so only a peer bug
/// reaches this: a node whose highest term seen is already `u64::MAX` has no
/// next term to contest, and panics rather than wrap back to term 0.
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
            u64::MAX,
            None,
        ),
    );

    start_roll_call(&mut node, &clock, SUSPECT);
}

// ---- Leaving a roll call ----

#[test]
fn a_vote_request_before_any_roll_call_answered_is_refused_as_not_the_best_call() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"), 3);
    let candidate = worker("w1");

    let outputs = deliver(
        &mut node,
        &candidate,
        vote_request_message(vote_request(candidate.clone(), 0, 1)),
    );

    let rejects = rejects_sent_to(&outputs, &candidate);
    assert_eq!(rejects.len(), 1);
    assert_eq!(rejects[0].reason(), ElectionRejectReason::NotBestRollCall);
}
