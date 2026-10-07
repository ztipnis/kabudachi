//! Leaving `NoQuorum` with no coordination authority:
//! the node retries a roll call every jittered suspicion timeout, takes part
//! in other nodes' elections meanwhile, and follows a leader whose ack
//! reaches it.

use crate::support::builders::{
    ack_message, configuration_of, g0, leader_ack, roll_call, roll_call_message, vote_request,
    vote_request_message, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{TestNode, deliver, elect, sent_to, state_changes, voter_node};
use kabudachi_core::election::Input;
use kabudachi_core::protocol::messages::election_message::Payload;
use kabudachi_core::protocol::messages::{ElectionMessage, VoteRequest};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Clock;

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
