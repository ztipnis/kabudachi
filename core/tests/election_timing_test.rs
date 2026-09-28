//! The election's timers (ADR-0001 decision 15): the jitter on a node's
//! suspicion timeout, and the deadlines by which a roll call and a vote
//! must succeed.

mod support;

use std::collections::BTreeSet;

use kabudachi_core::election::{Input, WorkerNode};
use kabudachi_core::hashing::HashFunction;
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::RollCall;
use kabudachi_core::protocol::messages::election_message::Payload;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Clock, Duration, Instant};
use support::builders::{
    ack_message, configuration_of, g0, leader_ack, past_any_suspicion, roll_call,
    roll_call_message, roll_call_reply, shard, timings, voter_of, worker,
};
use support::clock::FakeClock;
use support::node::{
    TestNode, deliver, published_roll_calls, recipients_of, sent_to, state_changes, voter_node,
};

/// Long enough that jitter of up to a half shows at tick resolution.
const SUSPECT_TIMEOUT: u64 = 1_000;

fn node_of(clock: &FakeClock, my_id: &WorkerId) -> TestNode {
    WorkerNode::new(
        my_id.clone(),
        IncarnationId::new("incarnation-1"),
        shard("shard-1"),
        clock.clone(),
        voter_of(3),
        None,
        timings(Duration::from_ticks(SUSPECT_TIMEOUT)),
    )
}

/// How long after `since` `node` suspects its leader, found by ticking it
/// at each deadline it reports (a heartbeat to its leader may come first)
/// until it does.
fn suspicion_after(node: &mut TestNode, clock: &FakeClock, since: Instant) -> u64 {
    let mut deadline = node.step(Input::Tick).next_deadline;
    while node.state() == WorkerState::Active {
        let due = deadline.expect("an active node is due to suspect its leader");
        clock.advance(due - clock.now());
        deadline = node.step(Input::Tick).next_deadline;
    }
    assert_eq!(node.state(), WorkerState::LeaderSuspect);
    (clock.now() - since).as_ticks()
}

fn workers(count: usize) -> Vec<WorkerId> {
    (0..count).map(|i| worker(&format!("w{i}"))).collect()
}

// ---- Jitter ----

#[test]
fn a_nodes_suspicion_timeout_is_lengthened_by_less_than_half() {
    for id in workers(50) {
        let clock = FakeClock::new();
        let mut node = node_of(&clock, &id);

        let after = suspicion_after(&mut node, &clock, Instant::at(0));

        assert!(
            (SUSPECT_TIMEOUT + 1..SUSPECT_TIMEOUT + SUSPECT_TIMEOUT / 2 + 1).contains(&after),
            "{id:?} suspects {after} ticks after its last leader contact"
        );
    }
}

#[test]
fn a_node_suspects_its_leader_exactly_at_the_deadline_it_reports() {
    let clock = FakeClock::new();
    let mut node = node_of(&clock, &worker("w1"));
    let due = node
        .step(Input::Tick)
        .next_deadline
        .expect("due to suspect");

    clock.advance(Duration::from_ticks((due - clock.now()).as_ticks() - 1));
    let _ = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::Active);

    clock.advance(Duration::from_ticks(1));
    let _ = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::LeaderSuspect);
}

#[test]
fn the_same_worker_in_the_same_term_always_gets_the_same_timeout() {
    let (first, second) = (FakeClock::new(), FakeClock::new());
    let id = worker("w1");

    assert_eq!(
        suspicion_after(&mut node_of(&first, &id), &first, Instant::at(0)),
        suspicion_after(&mut node_of(&second, &id), &second, Instant::at(0))
    );
}

#[test]
fn workers_in_the_same_term_get_different_timeouts() {
    let timeouts: BTreeSet<u64> = workers(20)
        .iter()
        .map(|id| {
            let clock = FakeClock::new();
            suspicion_after(&mut node_of(&clock, id), &clock, Instant::at(0))
        })
        .collect();

    assert!(timeouts.len() > 10, "{timeouts:?}");
}

#[test]
fn a_workers_timeout_changes_with_the_term() {
    let leader = worker("leader-1");
    let timeouts: BTreeSet<u64> = (1..=20)
        .map(|term| {
            let clock = FakeClock::new();
            let mut node = node_of(&clock, &worker("w1"));
            deliver(
                &mut node,
                &leader,
                ack_message(leader_ack(&leader, term, &configuration_of(3), Some(g0()))),
            );
            suspicion_after(&mut node, &clock, Instant::at(0))
        })
        .collect();

    assert!(timeouts.len() > 10, "{timeouts:?}");
}

#[test]
fn the_jitter_follows_the_configured_hash_function() {
    let differs = workers(20).iter().any(|id| {
        let (clock, sha3_clock) = (FakeClock::new(), FakeClock::new());
        let default = suspicion_after(&mut node_of(&clock, id), &clock, Instant::at(0));
        let mut sha3 =
            node_of(&sha3_clock, id).with_hash_function(HashFunction::new::<sha3::Sha3_256>());
        default != suspicion_after(&mut sha3, &sha3_clock, Instant::at(0))
    });

    assert!(differs);
}

#[test]
fn leader_contact_stops_being_fresh_at_the_unjittered_timeout() {
    let clock = FakeClock::new();
    let (me, initiator) = (worker("w1"), worker("w2"));
    let mut node = node_of(&clock, &me);
    let due = node
        .step(Input::Tick)
        .next_deadline
        .expect("due to suspect");
    assert!(
        due > Instant::at(SUSPECT_TIMEOUT + 1),
        "pick a worker whose jitter is not zero"
    );

    // Still `Active`, but its leader contact is stale: it takes part.
    clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT + 1));
    let outputs = deliver(
        &mut node,
        &initiator,
        roll_call_message(roll_call(&initiator, 1, &configuration_of(3), 0)),
    );

    assert_eq!(node.state(), WorkerState::Active);
    assert!(
        matches!(
            sent_to(&outputs, &initiator)[0].payload,
            Some(kabudachi_core::protocol::messages::election_message::Payload::RollCallReply(_))
        ),
        "{outputs:?}"
    );
}

// ---- The roll call's deadline, then the vote's ----

/// How long a roll call runs, and a candidate then has to win, for nodes
/// built with `timings(SUSPECT_TIMEOUT)`.
fn roll_call_deadline() -> Duration {
    timings(Duration::from_ticks(SUSPECT_TIMEOUT)).roll_call_deadline
}

/// `me`, a voter of `voter_count` with no leader, that has just started a
/// roll call. Returns the node, its call, and the deadline it then reported.
fn initiator(
    clock: &FakeClock,
    me: &WorkerId,
    voter_count: usize,
) -> (TestNode, RollCall, Instant) {
    let mut node = voter_node(clock, me, voter_count, SUSPECT_TIMEOUT);
    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
    let _ = node.step(Input::Tick);
    let started = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::RollCall, "setup invariant");
    let call = published_roll_calls(&started.outputs).remove(0);
    let deadline = started.next_deadline.expect("a roll call has a deadline");
    (node, call, deadline)
}

fn is_vote_request(payload: &Payload) -> bool {
    matches!(payload, Payload::VoteRequest(_))
}

fn advance_to(clock: &FakeClock, instant: Instant) {
    clock.advance(instant - clock.now());
}

#[test]
fn a_roll_call_is_due_at_its_deadline() {
    let clock = FakeClock::new();
    let (_node, _call, deadline) = initiator(&clock, &worker("w1"), 3);

    assert_eq!(deadline, clock.now() + roll_call_deadline());
}

#[test]
fn an_initiator_stands_only_at_its_deadline_though_its_returning_quorum_came_sooner() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let (mut node, call, deadline) = initiator(&clock, &me, 3);
    let (p1, p2) = (worker("p1"), worker("p2"));

    let replied = deliver(
        &mut node,
        &p1,
        roll_call_reply(&me, call.term, &p1, Some(g0())),
    );
    assert!(state_changes(&replied).is_empty(), "{replied:?}");
    clock.advance(Duration::from_ticks(1));
    deliver(
        &mut node,
        &p2,
        roll_call_reply(&me, call.term, &p2, Some(g0())),
    );
    advance_to(&clock, Instant::at(deadline.as_ticks() - 1));
    let _ = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::RollCall);

    advance_to(&clock, deadline);
    let stood = node.step(Input::Tick);

    assert_eq!(state_changes(&stood.outputs), vec![WorkerState::Candidate]);
    assert_eq!(
        recipients_of(&stood.outputs, is_vote_request),
        vec![p1, p2],
        "every respondent is asked for its vote"
    );
    assert_eq!(
        stood.next_deadline,
        Some(deadline + roll_call_deadline()),
        "the vote has as long again from standing"
    );
}

#[test]
fn an_initiator_short_of_a_returning_quorum_at_its_deadline_goes_no_quorum_and_retries() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let (mut node, call, deadline) = initiator(&clock, &me, 5);
    let p1 = worker("p1");
    deliver(
        &mut node,
        &p1,
        roll_call_reply(&me, call.term, &p1, Some(g0())),
    );

    advance_to(&clock, deadline);
    let failed = node.step(Input::Tick);

    assert_eq!(state_changes(&failed.outputs), vec![WorkerState::NoQuorum]);
    let retry_at = failed.next_deadline.expect("due to retry");
    assert!(
        retry_at > deadline + Duration::from_ticks(SUSPECT_TIMEOUT)
            && retry_at <= deadline + past_any_suspicion(SUSPECT_TIMEOUT),
        "{retry_at:?}"
    );
    advance_to(&clock, retry_at);
    let retried = node.step(Input::Tick);
    assert_eq!(
        published_roll_calls(&retried.outputs)[0].term,
        call.term + 1
    );
}

#[test]
fn a_candidate_not_won_by_its_vote_deadline_suspects_again_and_retries_later() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let (mut node, call, deadline) = initiator(&clock, &me, 3);
    let p1 = worker("p1");
    deliver(
        &mut node,
        &p1,
        roll_call_reply(&me, call.term, &p1, Some(g0())),
    );
    advance_to(&clock, deadline);
    let vote_deadline = node
        .step(Input::Tick)
        .next_deadline
        .expect("a candidate has a deadline");
    assert_eq!(node.state(), WorkerState::Candidate, "setup invariant");

    advance_to(&clock, vote_deadline);
    let lost = node.step(Input::Tick);

    assert_eq!(
        state_changes(&lost.outputs),
        vec![WorkerState::LeaderSuspect]
    );
    let retry_at = lost.next_deadline.expect("due to retry");
    assert!(
        retry_at > vote_deadline + Duration::from_ticks(SUSPECT_TIMEOUT)
            && retry_at <= vote_deadline + past_any_suspicion(SUSPECT_TIMEOUT),
        "{retry_at:?}"
    );
    advance_to(&clock, retry_at);
    let retried = node.step(Input::Tick);
    assert_eq!(
        published_roll_calls(&retried.outputs)[0].term,
        call.term + 1
    );
}

#[test]
fn an_initiator_that_abandoned_its_call_and_heard_no_leader_suspects_again_at_its_deadline() {
    let clock = FakeClock::new();
    clock.set_wall_clock_millis(500);
    let (mut node, own, deadline) = initiator(&clock, &worker("w2"), 3);
    let better = worker("w1");
    deliver(
        &mut node,
        &better,
        roll_call_message(roll_call(&better, own.term, &configuration_of(3), 500)),
    );

    advance_to(&clock, deadline);
    let gave_up = node.step(Input::Tick);

    assert_eq!(
        state_changes(&gave_up.outputs),
        vec![WorkerState::LeaderSuspect]
    );
    assert!(
        gave_up
            .next_deadline
            .is_some_and(|retry| retry > deadline + Duration::from_ticks(SUSPECT_TIMEOUT)),
        "{gave_up:?}"
    );
}
