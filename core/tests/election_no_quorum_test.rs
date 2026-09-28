//! Leaving `NoQuorum` with no coordination authority (ADR-0001 decision 13):
//! the node retries a roll call every jittered suspicion timeout, takes part
//! in other nodes' elections meanwhile, and follows a leader whose ack
//! reaches it.

mod support;

use kabudachi_core::election::Input;
use kabudachi_core::protocol::messages::election_message::Payload;
use kabudachi_core::protocol::messages::{ElectionMessage, VoteRequest};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Clock, Duration};
use support::builders::{
    ack_message, configuration_of, g0, leader_ack, past_any_suspicion, roll_call,
    roll_call_message, vote_request, vote_request_message, worker,
};
use support::clock::FakeClock;
use support::node::{
    TestNode, deliver, elect, published_roll_calls, sent_to, state_changes, voter_node,
};

/// Every node here suspects its leader after this many ticks.
const SUSPECT: u64 = 10;

/// `w1`, elected leader of a configuration of 3 in term 1, whose lease ran
/// out with no follower confirming an ack: `NoQuorum`, with no authority.
fn leader_in_no_quorum(clock: &FakeClock) -> TestNode {
    let mut node = voter_node(clock, &worker("w1"), 3, SUSPECT);
    elect(&mut node, clock, SUSPECT, &[worker("p1"), worker("p2")]);
    let lease_end = node
        .step(Input::Tick)
        .next_deadline
        .expect("a leader of three has a lease end");
    clock.advance(lease_end - clock.now());
    let _ = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::NoQuorum, "setup invariant");
    node
}

fn ack_from_new_leader(term: u64) -> ElectionMessage {
    ack_message(leader_ack(
        &worker("leader-2"),
        term,
        &configuration_of(3),
        Some(g0()),
    ))
}

#[test]
fn a_node_that_lost_its_quorum_retries_a_roll_call_after_a_jittered_suspicion_timeout() {
    let clock = FakeClock::new();
    let mut node = leader_in_no_quorum(&clock);
    let lost_at = clock.now();

    let step = node.step(Input::Tick);
    assert!(published_roll_calls(&step.outputs).is_empty());
    let retry_at = step.next_deadline.expect("due to retry");
    assert!(
        retry_at > lost_at + Duration::from_ticks(SUSPECT)
            && retry_at <= lost_at + past_any_suspicion(SUSPECT),
        "{retry_at:?}"
    );

    clock.advance(retry_at - clock.now());
    let retried = node.step(Input::Tick);

    assert_eq!(state_changes(&retried.outputs), vec![WorkerState::RollCall]);
    assert_eq!(published_roll_calls(&retried.outputs)[0].term, 2);
}

#[test]
fn a_node_in_no_quorum_answers_roll_calls_and_grants_votes() {
    let clock = FakeClock::new();
    let mut node = leader_in_no_quorum(&clock);
    let rival = worker("w0");
    let shards_configuration = node.configuration().cloned().expect("it led one");

    let answered = deliver(
        &mut node,
        &rival,
        roll_call_message(roll_call(&rival, 2, &shards_configuration, 0)),
    );
    let voted = deliver(
        &mut node,
        &rival,
        vote_request_message(VoteRequest {
            roll_call_generation: Some(shards_configuration.generation().into()),
            ..vote_request(rival.clone(), 0, 2)
        }),
    );

    assert!(matches!(
        sent_to(&answered, &rival)[0].payload,
        Some(Payload::RollCallReply(_))
    ));
    assert!(matches!(
        sent_to(&voted, &rival)[0].payload,
        Some(Payload::VoteGrant(_))
    ));
    assert_eq!(node.state(), WorkerState::NoQuorum);
}

#[test]
fn an_ack_from_a_leader_returns_a_node_in_no_quorum_to_active() {
    let clock = FakeClock::new();
    let mut node = leader_in_no_quorum(&clock);

    let outputs = deliver(&mut node, &worker("leader-2"), ack_from_new_leader(2));

    assert_eq!(state_changes(&outputs), vec![WorkerState::Active]);
    assert_eq!(node.known_leader(), Some((worker("leader-2"), 2)));
}

#[test]
fn a_drain_kept_in_no_quorum_applies_once_a_leader_acks_the_node() {
    let clock = FakeClock::new();
    let mut node = leader_in_no_quorum(&clock);
    assert!(node.step(Input::Drain).outputs.is_empty());

    let outputs = deliver(&mut node, &worker("leader-2"), ack_from_new_leader(2));

    assert_eq!(
        state_changes(&outputs),
        vec![
            WorkerState::Active,
            WorkerState::Draining,
            WorkerState::Stopped
        ]
    );
}
