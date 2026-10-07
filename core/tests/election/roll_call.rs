//! The roll call, from both sides: the
//! initiator that publishes it and collects replies until its deadline,
//! when a returning quorum makes it the candidate, and the workers that
//! answer it, pass over a repeat of it, or refuse it and say why.

use crate::support::builders::{
    ack_message, configuration_of, g0, leader_ack, message, past_any_suspicion, roll_call,
    roll_call_message, roll_call_reply, shard, timings, vote_grant, vote_grant_message,
    vote_request, vote_request_message, voter_of, worker,
};
use crate::support::builders::checked;
use crate::support::clock::FakeClock;
use kabudachi_core::protocol::checked::{Checked, CheckedPayload};
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
    ElectionMessage, ElectionReject, ElectionRejectReason, KnownLeader, LeaderHeartbeatAck,
    RollCall, RollCallReply, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Clock, Duration};

/// Every node here suspects its leader after this many ticks.
const SUSPECT: u64 = 10;

fn is_vote_request(payload: &election_message::Payload) -> bool {
    matches!(payload, election_message::Payload::VoteRequest(_))
}

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

fn reply_from(
    node: &mut TestNode,
    call: &Checked<RollCall>,
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
    let published: Vec<RollCall> = published_roll_calls(&outputs)
        .into_iter()
        .map(|call| (*call).clone())
        .collect();
    assert_eq!(
        published,
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

// ---- Backoff ----

/// Ticks `node` once per tick of `clock` until it publishes a roll call, and
/// returns that call.
fn next_roll_call(node: &mut TestNode, clock: &FakeClock) -> Checked<RollCall> {
    for _ in 0..4 * SUSPECT {
        clock.advance(Duration::from_ticks(1));
        if let Some(call) = published_roll_calls(&tick(node)).into_iter().next() {
            return call;
        }
    }
    panic!("the node published no roll call");
}

#[test]
fn replies_that_always_outlast_the_base_deadline_still_elect_the_node_once_its_calls_widen() {
    let clock = FakeClock::new();
    let mut node = voter_node(&clock, &worker("w1"), 3, SUSPECT);
    let peer = worker("p1");
    // The base deadline is a quarter of SUSPECT, 2 ticks; a reply takes 3.
    const ROUND_TRIP: u64 = 3;

    for _ in 0..5 {
        let call = next_roll_call(&mut node, &clock);
        let me = call.initiator_id();
        for elapsed in 1..=2 * SUSPECT {
            clock.advance(Duration::from_ticks(1));
            tick(&mut node);
            if elapsed == ROUND_TRIP {
                deliver(
                    &mut node,
                    &peer,
                    roll_call_reply(&me, call.term, &peer, Some(g0())),
                );
            }
            if node.state() == WorkerState::Candidate {
                deliver(
                    &mut node,
                    &peer,
                    vote_grant_message(vote_grant(me.clone(), peer.clone(), call.term)),
                );
                break;
            }
            if node.state() == WorkerState::NoQuorum {
                break;
            }
        }
        if node.state() == WorkerState::Leader {
            return;
        }
    }
    panic!("the node never led; it ended {:?}", node.state());
}

#[test]
fn each_roll_call_that_finds_no_quorum_doubles_the_next_deadline_up_to_the_suspicion_timeout_and_a_leaders_ack_resets_it() {
    let clock = FakeClock::new();
    let mut node = voter_node(&clock, &worker("w1"), 3, SUSPECT);
    let base = timings(Duration::from_ticks(SUSPECT))
        .roll_call_deadline
        .as_ticks();

    for streak in 0..5 {
        next_roll_call(&mut node, &clock);
        let deadline = (base << streak).min(SUSPECT);
        clock.advance(Duration::from_ticks(deadline - 1));
        tick(&mut node);
        assert_eq!(
            node.state(),
            WorkerState::RollCall,
            "call {streak} closed early"
        );
        clock.advance(Duration::from_ticks(1));
        tick(&mut node);
        assert_eq!(
            node.state(),
            WorkerState::NoQuorum,
            "call {streak} ran past {deadline}"
        );
    }

    // A leader's ack ends the streak: the next call runs the base deadline.
    let leader = worker("leader-2");
    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 50, &configuration_of(3), Some(g0()))),
    );
    assert_eq!(node.state(), WorkerState::Active, "setup invariant");
    next_roll_call(&mut node, &clock);
    clock.advance(Duration::from_ticks(base - 1));
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::RollCall, "closed early");
    clock.advance(Duration::from_ticks(1));
    tick(&mut node);
    assert_eq!(
        node.state(),
        WorkerState::NoQuorum,
        "the call after a leader's ack did not run the base deadline"
    );
}

#[test]
fn a_node_that_won_after_widening_calls_again_at_the_base_deadline_once_it_loses_its_quorum() {
    let clock = FakeClock::new();
    let mut node = voter_node(&clock, &worker("w1"), 3, SUSPECT);
    let peer = worker("p1");
    let base = timings(Duration::from_ticks(SUSPECT))
        .roll_call_deadline
        .as_ticks();

    // Two calls find no quorum, widening the third to four times the base.
    for deadline in [base, 2 * base] {
        next_roll_call(&mut node, &clock);
        clock.advance(Duration::from_ticks(deadline));
        tick(&mut node);
        assert_eq!(node.state(), WorkerState::NoQuorum, "setup invariant");
    }
    let call = next_roll_call(&mut node, &clock);
    let me = call.initiator_id();
    deliver(
        &mut node,
        &peer,
        roll_call_reply(&me, call.term, &peer, Some(g0())),
    );
    clock.advance(Duration::from_ticks(4 * base));
    tick(&mut node);
    deliver(
        &mut node,
        &peer,
        vote_grant_message(vote_grant(me, peer.clone(), call.term)),
    );
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");

    // No follower confirms an ack, so its lease runs out.
    let lease_end = tick_step_deadline(&mut node);
    clock.advance(lease_end - clock.now());
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::NoQuorum, "setup invariant");

    next_roll_call(&mut node, &clock);
    clock.advance(Duration::from_ticks(base));
    tick(&mut node);
    assert_eq!(
        node.state(),
        WorkerState::NoQuorum,
        "the call after a win did not run the base deadline"
    );
}

fn tick_step_deadline(node: &mut TestNode) -> kabudachi_core::time::Instant {
    node.step(Input::Tick)
        .next_deadline
        .expect("a leader has a lease end")
}

// ---- Suppression ----

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

#[test]
fn an_answered_call_that_never_resolves_releases_the_node_after_the_suspicion_timeout_beyond_the_election_window()
 {
    let clock = FakeClock::new();
    let (me, caller) = (worker("w2"), worker("w1"));
    let (mut node, answered_at) = suspecting_voter_that_answered(&clock, &me, &caller);
    let deadline = base_deadline();
    let release = answered_at + deadline + deadline + Duration::from_ticks(SUSPECT);
    clock.advance(Duration::from_ticks(1));
    let repeated = deliver(
        &mut node,
        &caller,
        roll_call_message(roll_call(&caller, 1, &configuration_of(3), 0)),
    );
    assert!(sent(&repeated).is_empty(), "a repeat is not answered again");

    // The caller's vote request is slow: it arrives after the call and the
    // vote window of a base-width election have passed on the caller's clock.
    // Granting its vote does not release the node either.
    clock.advance(Duration::from_ticks(deadline.as_ticks()));
    let voted = deliver(
        &mut node,
        &caller,
        vote_request_message(vote_request(caller.clone(), 0, 1)),
    );
    assert_eq!(
        recipients_of(&voted, |payload| matches!(
            payload,
            election_message::Payload::VoteGrant(_)
        )),
        vec![caller.clone()],
        "setup invariant"
    );

    let held = node.step(Input::Tick);

    assert!(published_roll_calls(&held.outputs).is_empty());
    assert_eq!(
        held.next_deadline,
        Some(release),
        "a repeat of the answered call does not push the release back"
    );

    clock.advance(Duration::from_ticks(
        release.as_ticks() - 1 - clock.now().as_ticks(),
    ));
    assert!(published_roll_calls(&tick(&mut node)).is_empty());
    clock.advance(Duration::from_ticks(1));
    let started = tick(&mut node);

    assert_eq!(state_changes(&started), vec![WorkerState::RollCall]);
    assert_eq!(
        published_roll_calls(&started)[0].term,
        2,
        "the term after the call it answered"
    );
}

#[test]
fn answering_a_better_call_holds_the_node_back_from_that_answer_on() {
    let clock = FakeClock::new();
    let (me, first, second) = (worker("w3"), worker("w1"), worker("w2"));
    let (mut node, answered_at) = suspecting_voter_that_answered(&clock, &me, &first);
    clock.advance(Duration::from_ticks(SUSPECT));
    let answered = deliver(
        &mut node,
        &second,
        roll_call_message(roll_call(&second, 2, &configuration_of(3), 0)),
    );
    assert_eq!(replies_to(&answered, &second).len(), 1, "setup invariant");
    let release_of = |answered_at| {
        answered_at + base_deadline() + base_deadline() + Duration::from_ticks(SUSPECT)
    };
    let second_release = release_of(clock.now());
    // The first call's release has passed; the second's has not.
    clock.advance(release_of(answered_at) + Duration::from_ticks(1) - clock.now());

    assert!(published_roll_calls(&tick(&mut node)).is_empty());

    clock.advance(second_release - clock.now());
    let started = tick(&mut node);

    assert_eq!(published_roll_calls(&started)[0].term, 3);
}

/// `me`, a voter of three that suspects its leader after `LONG` ticks and
/// whose roll call runs half that, so a call it answers holds it back for
/// two suspicion timeouts: longer than any suspicion that starts after the
/// answer, however jittered.
const LONG: u64 = 100;

fn voter_that_answered_a_call_at_a_long_hold(
    clock: &FakeClock,
    me: &WorkerId,
    caller: &WorkerId,
    term: u64,
) -> (TestNode, kabudachi_core::time::Instant) {
    let mut node = WorkerNode::start(
        Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(LONG))
                .with_roll_call_deadline(Duration::from_ticks(LONG / 2)),
        },
        Entry::Known(voter_of(3)),
        clock.clone(),
        None,
    )
    .0;
    clock.advance(past_any_suspicion(LONG));
    tick(&mut node);
    let answered = deliver(
        &mut node,
        caller,
        roll_call_message(roll_call(caller, term, &configuration_of(3), 0)),
    );
    assert_eq!(replies_to(&answered, caller).len(), 1, "setup invariant");
    let release = clock.now() + Duration::from_ticks(2 * LONG);
    (node, release)
}

/// Ticks `node` one tick at a time until it publishes a roll call or
/// `until` is reached; the instant it published at, if before `until`.
fn first_call_before(
    node: &mut TestNode,
    clock: &FakeClock,
    until: kabudachi_core::time::Instant,
) -> Option<kabudachi_core::time::Instant> {
    while clock.now() + Duration::from_ticks(1) < until {
        clock.advance(Duration::from_ticks(1));
        if !published_roll_calls(&tick(node)).is_empty() {
            return Some(clock.now());
        }
    }
    None
}

#[test]
fn a_leaders_ack_of_the_answered_term_or_a_later_one_frees_the_node_to_call_when_it_next_suspects()
 {
    let clock = FakeClock::new();
    let (me, caller) = (worker("w2"), worker("w1"));
    let (mut node, release) = voter_that_answered_a_call_at_a_long_hold(&clock, &me, &caller, 1);
    // The leader that won is of a later term than the call it answered: the
    // call's caller lost the election to another's.
    deliver(
        &mut node,
        &caller,
        ack_message(leader_ack(&caller, 2, &configuration_of(3), Some(g0()))),
    );
    assert_eq!(node.state(), WorkerState::Active, "setup invariant");

    // Its leader then falls silent: the node suspects within one and a half
    // suspicion timeouts of the ack, well before the release.
    let called_at = first_call_before(&mut node, &clock, release);

    assert!(
        called_at.is_some(),
        "no call before the release at {release:?}"
    );
}

#[test]
fn a_retry_of_a_call_the_node_answered_holds_it_through_the_retrys_census_and_vote() {
    let clock = FakeClock::new();
    let (me, caller) = (worker("w2"), worker("w1"));
    let (mut node, _) = suspecting_voter_that_answered(&clock, &me, &caller);

    // The caller's first call found no quorum; it retries as the first
    // answer's hold is about to run out, in a new term this node answers.
    clock.advance(Duration::from_ticks(base_deadline().as_ticks() + SUSPECT));
    let answered = deliver(
        &mut node,
        &caller,
        roll_call_message(roll_call(&caller, 2, &configuration_of(3), 0)),
    );
    assert_eq!(replies_to(&answered, &caller).len(), 1, "setup invariant");

    // A call of this node's own before the retry's census and vote end would
    // contest the term of the election it is helping.
    let election_end = clock.now() + Duration::from_ticks(2 * base_deadline().as_ticks());
    let called_at = first_call_before(&mut node, &clock, election_end + Duration::from_ticks(1));

    assert_eq!(called_at, None, "the retry's election was contested");
}

/// How long answering a call holds a node back from its own.
fn hold_window() -> Duration {
    Duration::from_ticks(2 * base_deadline().as_ticks() + SUSPECT)
}

#[test]
fn initiators_taking_turns_with_failing_calls_cost_an_answerer_no_more_than_two_hold_windows() {
    let clock = FakeClock::new();
    let (me, first, second) = (worker("w3"), worker("w1"), worker("w2"));
    let (mut node, answered_at) = suspecting_voter_that_answered(&clock, &me, &first);
    let window = hold_window().as_ticks();
    let release = answered_at + Duration::from_ticks(2 * window);

    // The two initiators never reach each other: each fails and the other
    // calls next, each call a new term this node answers before the hold
    // from the previous answer ends.
    for (term, caller) in [(2, &second), (3, &first)] {
        clock.advance(Duration::from_ticks(window * 9 / 10));
        let answered = deliver(
            &mut node,
            caller,
            roll_call_message(roll_call(caller, term, &configuration_of(3), 0)),
        );
        assert_eq!(replies_to(&answered, caller).len(), 1, "setup invariant");
    }
    clock.advance(Duration::from_ticks(
        release.as_ticks() - 1 - clock.now().as_ticks(),
    ));
    assert!(
        published_roll_calls(&tick(&mut node)).is_empty(),
        "setup invariant: held until the episode's end"
    );
    clock.advance(Duration::from_ticks(1));
    let started = tick(&mut node);

    assert_eq!(
        published_roll_calls(&started)
            .first()
            .map(|call| call.term),
        Some(4),
        "alternating callers must not chain holds past the episode's end"
    );
}

#[test]
fn an_answer_late_in_an_episode_holds_the_node_for_two_base_deadlines_but_not_past_one_more_than_the_episodes_end() {
    let clock = FakeClock::new();
    let (me, first, second) = (worker("w3"), worker("w1"), worker("w2"));
    let (mut node, start) = suspecting_voter_that_answered(&clock, &me, &first);
    let span = 2 * base_deadline().as_ticks();
    let window = hold_window().as_ticks();
    let episode_end = start + Duration::from_ticks(2 * window);

    // Callers answer in turn, each while the previous hold is live: the
    // second answer comes just short of the first hold's end, the third just
    // short of the episode's end, the fourth just after it.
    let answers = [
        (start + Duration::from_ticks(window - 1), 2, &second),
        (start + Duration::from_ticks(2 * window - 2), 3, &first),
        (episode_end + Duration::from_ticks(1), 4, &second),
    ];
    for (at, term, caller) in answers {
        clock.advance(at - clock.now());
        let answered = deliver(
            &mut node,
            caller,
            roll_call_message(roll_call(caller, term, &configuration_of(3), 0)),
        );
        assert_eq!(replies_to(&answered, caller).len(), 1, "setup invariant");
    }

    // The third answer's own window is cut at the episode's end, but a
    // minimum after it reaches past it; the fourth, answered while that hold
    // is live, would hold the node a minimum after itself, further still,
    // but is cut at one minimum past the episode's end.
    let called_at = first_call_before(
        &mut node,
        &clock,
        episode_end + Duration::from_ticks(span + 1),
    );

    assert_eq!(
        called_at,
        Some(episode_end + Duration::from_ticks(span)),
        "the hold must run to one minimum past the episode's end, no less and no more"
    );
}

#[test]
fn a_call_answered_at_the_instant_an_episode_ends_does_not_hold_the_node_from_its_own_call() {
    let clock = FakeClock::new();
    let (me, first, second, third) = (worker("w3"), worker("w1"), worker("w2"), worker("w4"));
    let (mut node, answered_at) = suspecting_voter_that_answered(&clock, &me, &first);
    let window = hold_window().as_ticks();
    let episode_end = answered_at + Duration::from_ticks(2 * window);

    // Two callers taking turns keep the node held until the episode's cap.
    clock.advance(Duration::from_ticks(window * 9 / 10));
    deliver(
        &mut node,
        &second,
        roll_call_message(roll_call(&second, 2, &configuration_of(3), 0)),
    );
    clock.advance(Duration::from_ticks(window * 9 / 10));
    deliver(
        &mut node,
        &first,
        roll_call_message(roll_call(&first, 3, &configuration_of(3), 0)),
    );

    // A third caller retrying in step with the episode is answered at the
    // very instant the cap ends, before the node's tick runs.
    clock.advance(episode_end - clock.now());
    let answered = deliver(
        &mut node,
        &third,
        roll_call_message(roll_call(&third, 4, &configuration_of(3), 0)),
    );
    assert_eq!(replies_to(&answered, &third).len(), 1, "it is still answered");
    let started = tick(&mut node);

    assert_eq!(
        published_roll_calls(&started)
            .first()
            .map(|call| call.term),
        Some(5),
        "an answer at the cap's end must leave the node a call of its own"
    );
}

/// A node held by two callers taking turns until the episode's cap, at the
/// instant the cap ends, so it is owed a call. `first` and `second` are the
/// callers. Returns the node.
fn node_owed_a_call(
    clock: &FakeClock,
    me: &WorkerId,
    first: &WorkerId,
    second: &WorkerId,
) -> TestNode {
    let (mut node, answered_at) = suspecting_voter_that_answered(clock, me, first);
    let window = hold_window().as_ticks();
    let episode_end = answered_at + Duration::from_ticks(2 * window);
    clock.advance(Duration::from_ticks(window * 9 / 10));
    deliver(
        &mut node,
        second,
        roll_call_message(roll_call(second, 2, &configuration_of(3), 0)),
    );
    clock.advance(Duration::from_ticks(window * 9 / 10));
    deliver(
        &mut node,
        first,
        roll_call_message(roll_call(first, 3, &configuration_of(3), 0)),
    );
    clock.advance(episode_end - clock.now());
    node
}

#[test]
fn a_leaders_ack_of_a_term_below_the_answered_call_still_clears_a_call_the_node_was_owed() {
    let clock = FakeClock::new();
    let (me, first, second, third) = (worker("w3"), worker("w1"), worker("w2"), worker("w4"));
    let mut node = node_owed_a_call(&clock, &me, &first, &second);

    // A leader of term 1, below every call answered, acks the node.
    deliver(
        &mut node,
        &first,
        ack_message(leader_ack(&first, 1, &configuration_of(3), Some(g0()))),
    );
    assert_eq!(node.state(), WorkerState::Active, "setup invariant");

    // That leader goes silent, and a fresh call is answered before the node
    // ticks. The ack paid what the node was owed, so this answer holds it.
    clock.advance(past_any_suspicion(SUSPECT));
    let answered = deliver(
        &mut node,
        &third,
        roll_call_message(roll_call(&third, 4, &configuration_of(3), 0)),
    );
    assert_eq!(replies_to(&answered, &third).len(), 1, "setup invariant");
    let called_at = first_call_before(&mut node, &clock, clock.now() + hold_window());

    assert_eq!(
        called_at, None,
        "an answer after the ack must hold the node as a fresh episode's first"
    );
}

#[test]
fn a_roll_call_of_its_own_clears_a_call_the_node_was_owed() {
    let clock = FakeClock::new();
    let (me, first, second, third) = (worker("w3"), worker("w1"), worker("w2"), worker("w4"));
    let mut node = node_owed_a_call(&clock, &me, &first, &second);

    // Owed, the node calls at once, then answers a fresh call for a later
    // term while it collects replies. The call it made paid what it was
    // owed, so this answer holds it from its next call.
    let started = tick(&mut node);
    assert_eq!(published_roll_calls(&started).len(), 1, "setup invariant");
    let answered = deliver(
        &mut node,
        &third,
        roll_call_message(roll_call(&third, 5, &configuration_of(3), 0)),
    );
    assert_eq!(replies_to(&answered, &third).len(), 1, "setup invariant");
    let called_at = first_call_before(&mut node, &clock, clock.now() + hold_window());

    assert_eq!(
        called_at, None,
        "an answer after the node's own call must hold it as a fresh episode's first"
    );
}

#[test]
fn an_answer_after_the_hold_expired_holds_the_node_again() {
    let clock = FakeClock::new();
    let (me, caller) = (worker("w2"), worker("w1"));
    let (mut node, answered_at) = suspecting_voter_that_answered(&clock, &me, &caller);
    // The first call fails and its hold runs out unnoticed by the node.
    clock.advance(answered_at + hold_window() + Duration::from_ticks(1) - clock.now());
    let answered = deliver(
        &mut node,
        &caller,
        roll_call_message(roll_call(&caller, 3, &configuration_of(3), 0)),
    );
    assert_eq!(replies_to(&answered, &caller).len(), 1, "setup invariant");
    let release = clock.now() + hold_window();

    clock.advance(Duration::from_ticks(
        release.as_ticks() - 1 - clock.now().as_ticks(),
    ));
    assert!(
        published_roll_calls(&tick(&mut node)).is_empty(),
        "a new call answered after the old hold expired starts a fresh hold"
    );
    clock.advance(Duration::from_ticks(1));

    assert_eq!(
        published_roll_calls(&tick(&mut node))
            .first()
            .map(|call| call.term),
        Some(4)
    );
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
    let mut node = voter_node(&clock, &worker("w1"), 3, SUSPECT);
    let first = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
    // No one answers: the call closes short of its quorum.
    close(&mut node, &clock);
    assert_eq!(node.state(), WorkerState::NoQuorum, "setup invariant");

    let second = next_roll_call(&mut node, &clock);

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
fn a_call_a_leaders_ack_outlived_does_not_outrank_its_successors_call_for_that_term() {
    let clock = FakeClock::new();
    let (me, leader, successor) = (worker("w2"), worker("leader"), worker("w1"));
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    let ack_of_term_1 = || ack_message(leader_ack(&leader, 1, &configuration_of(3), Some(g0())));
    deliver(&mut node, &leader, ack_of_term_1());
    // Its leader's heartbeats run late: it suspects a live leader and calls
    // term 2, early on its wall clock, so its call ranks well.
    clock.set_wall_clock_millis(100);
    let needless = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
    assert_eq!(needless.term, 2, "setup invariant");
    deliver(&mut node, &leader, ack_of_term_1());
    assert_eq!(node.state(), WorkerState::Active, "setup invariant");

    // The leader is then lost, and another worker calls term 2, later on
    // its wall clock.
    clock.advance(past_any_suspicion(SUSPECT));
    let call = roll_call(&successor, 2, &configuration_of(3), 200);
    let answered = deliver(&mut node, &successor, roll_call_message(call));
    let voted = deliver(
        &mut node,
        &successor,
        vote_request_message(vote_request(successor.clone(), 0, 2)),
    );

    assert_eq!(replies_to(&answered, &successor).len(), 1, "{answered:?}");
    assert_eq!(
        recipients_of(&voted, |payload| matches!(
            payload,
            election_message::Payload::VoteGrant(_)
        )),
        vec![successor]
    );
}

#[test]
fn a_node_whose_call_a_leaders_ack_outlived_contests_that_term_again_when_it_next_suspects() {
    let clock = FakeClock::new();
    let (me, leader) = (worker("w2"), worker("leader"));
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    let ack_of_term_1 = || ack_message(leader_ack(&leader, 1, &configuration_of(3), Some(g0())));
    deliver(&mut node, &leader, ack_of_term_1());
    let needless = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
    assert_eq!(needless.term, 2, "setup invariant");
    deliver(&mut node, &leader, ack_of_term_1());
    assert_eq!(node.state(), WorkerState::Active, "setup invariant");

    clock.advance(past_any_suspicion(SUSPECT));
    let next = next_roll_call(&mut node, &clock);

    assert_eq!(next.term, 2, "the call the ack outlived took no term");
}

#[test]
fn a_leaders_ack_forgets_the_nodes_answer_to_a_call_above_the_followed_term() {
    let clock = FakeClock::new();
    let (me, leader, first, later) = (worker("w2"), worker("leader"), worker("w1"), worker("w3"));
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    let ack_of_term_1 = || ack_message(leader_ack(&leader, 1, &configuration_of(3), Some(g0())));
    deliver(&mut node, &leader, ack_of_term_1());
    // Its leader's heartbeats run late: its contact is stale, but no tick
    // has moved it out of `Active` when it answers a call for term 2, which
    // the live leader then proves false.
    clock.advance(past_any_suspicion(SUSPECT));
    assert_eq!(node.state(), WorkerState::Active, "setup invariant");
    deliver(
        &mut node,
        &first,
        roll_call_message(roll_call(&first, 2, &configuration_of(3), 50)),
    );
    deliver(&mut node, &leader, ack_of_term_1());
    assert_eq!(node.state(), WorkerState::Active, "setup invariant");

    // The leader is then lost, and another worker's call for term 2, ranking
    // below the one it answered, is the call that elects the successor.
    clock.advance(past_any_suspicion(SUSPECT));
    let answered = deliver(
        &mut node,
        &later,
        roll_call_message(roll_call(&later, 2, &configuration_of(3), 300)),
    );
    let voted = deliver(
        &mut node,
        &later,
        vote_request_message(vote_request(later.clone(), 0, 2)),
    );

    assert_eq!(replies_to(&answered, &later).len(), 1, "{answered:?}");
    assert_eq!(
        recipients_of(&voted, |payload| matches!(
            payload,
            election_message::Payload::VoteGrant(_)
        )),
        vec![later]
    );
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
fn refusal_of(node: &mut TestNode, initiator: &WorkerId, call: RollCall) -> Checked<ElectionReject> {
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
    assert_eq!(reject.recovery_epoch, Some(0));
    assert_eq!(reject.recovery_epoch_lineage, node.recovery_lineage());
    assert_eq!(reject.leader, None);
}

#[test]
fn a_call_under_an_older_configuration_is_refused_carrying_the_newer_one() {
    let clock = FakeClock::new();
    let newer = Configuration::single(Single {
        generation: Generation::new(0, 1, 2),
        base: g0(),
        voter_count: 3,
    }).expect("valid");
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
    assert_eq!(
        reject.named_leader(),
        Some((leader, 0)),
        "the refusal names the leader"
    );
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
        recovery_epoch: Some(0),
        recovery_epoch_lineage: None,
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
#[test]
fn a_refusal_from_a_lower_recovery_epoch_is_dropped() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let mut node = voter_at_epoch_1(&clock, &me, 3);
    let call = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
    let (rejecter, their_leader) = (worker("w2"), worker("old-leader"));
    // `reject_message` carries `configuration_of(3)`: epoch 0.
    let outputs = deliver(
        &mut node,
        &rejecter,
        reject_message(
            &call,
            &rejecter,
            ElectionRejectReason::LeaderStillValid,
            9,
            Some((&their_leader, 9)),
        ),
    );

    assert_eq!(
        node.state(),
        WorkerState::RollCall,
        "no step-down on another epoch's term"
    );
    assert_eq!(node.highest_term_seen(), 0);
    assert!(
        sent_to(&outputs, &their_leader).is_empty(),
        "no heartbeat to another epoch's leader"
    );
    assert_eq!(
        node.configuration().map(Configuration::generation),
        Some(Generation::new(1, 0, 0))
    );

    // As a candidate too: stand with two epoch-1 replies, then the same
    // refusal of its vote request.
    reply_from(&mut node, &call, &worker("w2"), Some(Generation::new(1, 0, 0)));
    reply_from(&mut node, &call, &worker("w3"), Some(Generation::new(1, 0, 0)));
    close(&mut node, &clock);
    assert_eq!(node.state(), WorkerState::Candidate, "setup invariant");
    deliver(
        &mut node,
        &rejecter,
        reject_message(&call, &rejecter, ElectionRejectReason::StaleTerm, 9, None),
    );
    assert_eq!(node.state(), WorkerState::Candidate);
    assert_eq!(node.highest_term_seen(), 0);
}

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

// A refusal names its
// refuser's epoch and lineage itself, so one without a configuration is
// placed too.
#[test]
fn a_refusal_from_a_lower_epoch_is_dropped_even_without_a_configuration() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let mut node = voter_at_epoch_1(&clock, &me, 3);
    let call = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
    let rejecter = worker("w2");
    let refusal = refusal_at(
        reject_message(&call, &rejecter, ElectionRejectReason::StaleTerm, 5, None),
        Some(0),
        None,
        None,
    );

    deliver(&mut node, &rejecter, refusal);

    assert_eq!(node.highest_term_seen(), 0);
    assert_eq!(node.state(), WorkerState::RollCall);
}

// Another lineage's epoch is another shard's, so its terms and
// configuration mean nothing here even when its number is this node's own.
#[test]
fn a_refusal_from_another_lineage_at_this_nodes_epoch_number_is_dropped() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let mut node = voter_at_epoch_1(&clock, &me, 3);
    let call = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
    let (rejecter, their_leader) = (worker("w2"), worker("foreign-leader"));
    let own_lineage = node.recovery_lineage().expect("a joined node has one");
    let refusal = refusal_at(
        reject_message(
            &call,
            &rejecter,
            ElectionRejectReason::LeaderStillValid,
            9,
            Some((&their_leader, 9)),
        ),
        Some(1),
        Some(own_lineage + 1),
        Some(epoch_1_configuration(3)),
    );

    let outputs = deliver(&mut node, &rejecter, refusal);

    assert_eq!(node.highest_term_seen(), 0);
    assert_eq!(node.state(), WorkerState::RollCall);
    assert!(sent_to(&outputs, &their_leader).is_empty());
}

// A higher-numbered epoch of another lineage is taken on by
// the leader's ack, as `on_leader_ack` does, so a refusal from it only names
// that leader: no term is raised.
#[test]
fn a_refusal_from_a_higher_numbered_foreign_epoch_names_its_leader_and_raises_no_term() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let mut node = voter_at_epoch_1(&clock, &me, 3);
    let call = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
    let (rejecter, their_leader) = (worker("w2"), worker("foreign-leader"));
    let own_lineage = node.recovery_lineage().expect("a joined node has one");
    let refusal = refusal_at(
        reject_message(
            &call,
            &rejecter,
            ElectionRejectReason::LeaderStillValid,
            9,
            Some((&their_leader, 9)),
        ),
        Some(2),
        Some(own_lineage + 1),
        None,
    );

    let outputs = deliver(&mut node, &rejecter, refusal);

    assert_eq!(node.highest_term_seen(), 0);
    assert_eq!(node.state(), WorkerState::RollCall);
    assert!(
        !sent_to(&outputs, &their_leader).is_empty(),
        "it heartbeats the named leader"
    );
}

// A refusal that names no epoch (its rejecter has joined no shard) is still
// read as this node's own.
#[test]
fn a_refusal_naming_no_epoch_is_read_as_the_nodes_own() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let mut node = voter_at_epoch_1(&clock, &me, 3);
    let call = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
    let rejecter = worker("w2");
    let refusal = refusal_at(
        reject_message(&call, &rejecter, ElectionRejectReason::StaleTerm, 5, None),
        None,
        None,
        None,
    );

    deliver(&mut node, &rejecter, refusal);

    assert_eq!(node.highest_term_seen(), 5);
    assert_eq!(node.state(), WorkerState::LeaderSuspect);
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
