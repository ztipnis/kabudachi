//! `WorkerNode::bootstrapping` + `WorkerNode::finish_joining`, the core-side
//! half of the bootstrap join protocol, and what a joiner does once joined.
//! The wire handshake that produces the leader pointer (dialing seeds,
//! sending `JOIN_REQUEST`, taking the first `JOIN_RESPONSE` that names a
//! leader) is `net`'s concern (`net/tests/three_node_join_test.rs` covers
//! that end to end); these tests only exercise the plain state-machine
//! surface `net` calls once it has one.
//!
//! A joiner is a pending member: it has no admission generation, and no
//! configuration until its leader's first ack carries one. It answers roll
//! calls and grants votes as a new voter, starts roll calls of its own once
//! it knows a configuration, and is admitted by the first election whose
//! roll call it answers.

mod support;

use std::collections::BTreeMap;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{Input, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    ElectionMessage, JoinResponse, LeaderHeartbeatAck, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;

use support::builders::{
    ack_message, configuration_of, g0, leader_ack, past_any_suspicion, roll_call,
    roll_call_message, timings, vote_request, vote_request_message,
};
use support::clock::FakeClock;
use support::node::{
    TestNode, connect, deliver, deliver_all, published_roll_calls, sent, sent_to, tick, voter_node,
};

const SHARD: &str = "shard-1";
const SUSPECT_TIMEOUT_TICKS: u64 = 10;

fn worker(id: &str) -> WorkerId {
    WorkerId::new(id)
}

fn pointer(leader: &str, term: u64, recovery_epoch: u64) -> JoinResponse {
    JoinResponse {
        leader_id: Some(worker(leader).into()),
        leader_multiaddr: "/ip4/127.0.0.1/tcp/4001".into(),
        term,
        recovery_epoch,
        recovery_epoch_lineage: 0,
    }
}

fn bootstrapping_node(id: WorkerId, clock: &FakeClock) -> TestNode {
    WorkerNode::bootstrapping(
        id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", id.as_str())),
        ShardId::new(SHARD),
        clock.clone(),
        None,
        timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)),
    )
}

/// A joiner that joined `leader`, elected in term 1 at recovery epoch 0.
fn joined(clock: &FakeClock, leader: &str) -> TestNode {
    let mut node = bootstrapping_node(worker("joiner"), clock);
    let _ = node.finish_joining(&pointer(leader, 1, 0));
    node
}

/// An ack from `leader` in term 1 carrying a configuration of `voter_count`
/// and `recipient_admission`.
fn ack_of(
    leader: &str,
    voter_count: usize,
    recipient_admission: Option<Generation>,
) -> LeaderHeartbeatAck {
    leader_ack(
        &worker(leader),
        1,
        &configuration_of(voter_count),
        recipient_admission,
    )
}

#[test]
fn bootstrapping_constructs_a_node_in_the_bootstrapping_state() {
    let clock = FakeClock::new();

    let node = bootstrapping_node(worker("joiner"), &clock);

    assert_eq!(node.state(), WorkerState::Bootstrapping);
    assert_eq!(node.known_leader(), None);
    assert_eq!(node.configuration(), None);
    assert!(
        node.is_pending_member(),
        "a joiner has no admission generation from the start"
    );
}

#[test]
fn finish_joining_makes_the_joiner_an_active_pending_member_of_the_leader_it_was_pointed_at() {
    let clock = FakeClock::new();
    let mut node = bootstrapping_node(worker("joiner"), &clock);

    let _ = node.finish_joining(&pointer("leader", 4, 2));

    assert_eq!(node.state(), WorkerState::Active);
    assert!(node.is_pending_member());
    assert_eq!(node.configuration(), None);
    assert_eq!(node.known_leader(), Some((worker("leader"), 4)));
    assert_eq!(node.recovery_epoch(), 2);
}

#[test]
fn finish_joining_without_a_leader_leaves_the_node_bootstrapping() {
    let clock = FakeClock::new();
    let mut node = bootstrapping_node(worker("joiner"), &clock);

    let _ = node.finish_joining(&JoinResponse::default());

    assert_eq!(node.state(), WorkerState::Bootstrapping);
    assert_eq!(node.known_leader(), None);
}

#[test]
fn finish_joining_is_a_noop_outside_bootstrapping() {
    let clock = FakeClock::new();
    let mut node = voter_node(&clock, &worker("a"), 2, SUSPECT_TIMEOUT_TICKS);

    let _ = node.finish_joining(&pointer("leader", 4, 2));

    assert_eq!(node.state(), WorkerState::Active);
    assert!(!node.is_pending_member());
    assert_eq!(node.known_leader(), None);
    assert_eq!(node.recovery_epoch(), 0);
    assert_eq!(node.configuration(), Some(&configuration_of(2)));
}

#[test]
fn tick_does_not_move_a_bootstrapping_node_even_past_the_suspicion_timeout() {
    let clock = FakeClock::new();
    let mut node = bootstrapping_node(worker("joiner"), &clock);

    clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS * 5));
    for _ in 0..5 {
        tick(&mut node);
    }

    assert_eq!(
        node.state(),
        WorkerState::Bootstrapping,
        "a Tick is a deliberate no-op in Bootstrapping/Joining (core/src/election.rs); only \
         finish_joining moves a bootstrapping node forward"
    );
}

#[test]
fn finish_joining_resets_the_leader_contact_timer_so_the_new_follower_is_not_immediately_suspect() {
    let clock = FakeClock::new();
    let mut node = bootstrapping_node(worker("joiner"), &clock);

    // If finish_joining did not reset last_leader_contact, this much elapsed
    // time before joining would make the node suspect on its next Tick.
    clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS * 5));
    let _ = node.finish_joining(&pointer("leader", 1, 0));
    tick(&mut node);

    assert_eq!(node.state(), WorkerState::Active);
}

#[test]
fn a_freshly_joined_node_still_becomes_leader_suspect_once_its_own_timeout_elapses() {
    let clock = FakeClock::new();
    let mut node = bootstrapping_node(worker("joiner"), &clock);

    let _ = node.finish_joining(&pointer("leader", 1, 0));
    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT_TICKS));
    tick(&mut node);

    assert_eq!(node.state(), WorkerState::LeaderSuspect);
}

/// An ack from `leader`, elected in term 4, at `recovery_epoch`, carrying
/// a configuration of two founded at that epoch, as every configuration an
/// ack of that epoch carries is.
fn ack_at_epoch(recovery_epoch: u64) -> ElectionMessage {
    let founded = Generation::genesis(recovery_epoch);
    let configuration = Configuration::single(Single {
        generation: founded,
        base: founded,
        voter_count: 2,
    });
    let ack = LeaderHeartbeatAck {
        recovery_epoch,
        term: 4,
        ..leader_ack(&worker("leader"), 4, &configuration, None)
    };
    ack_message(ack)
}

/// A joiner takes the recovery epoch its pointer names: it accepts its
/// leader's acks of that epoch, and ignores those of an earlier one, which a
/// joiner left at epoch 0 would have taken for a later epoch and accepted.
#[test]
fn a_joiner_accepts_its_leaders_acks_at_the_recovery_epoch_it_was_pointed_at() {
    let clock = FakeClock::new();
    let mut at_its_epoch = bootstrapping_node(worker("joiner-1"), &clock);
    let mut at_an_earlier_one = bootstrapping_node(worker("joiner-2"), &clock);
    for node in [&mut at_its_epoch, &mut at_an_earlier_one] {
        let _ = node.finish_joining(&pointer("leader", 4, 2));
    }

    clock.advance(Duration::from_ticks(8));
    for (node, epoch) in [(&mut at_its_epoch, 2), (&mut at_an_earlier_one, 1)] {
        deliver(node, &worker("leader"), ack_at_epoch(epoch));
    }

    // 15 ticks after joining but only 7 after the ack: a joiner that ignored
    // the ack suspects its leader, one that accepted it does not.
    clock.advance(Duration::from_ticks(7));
    tick(&mut at_its_epoch);
    tick(&mut at_an_earlier_one);

    assert_eq!(at_its_epoch.state(), WorkerState::Active);
    assert_eq!(at_an_earlier_one.state(), WorkerState::LeaderSuspect);
}

#[test]
fn a_joiner_with_no_configuration_starts_no_roll_call_and_keeps_heartbeating_its_leader() {
    let clock = FakeClock::new();
    let mut joiner = joined(&clock, "leader");

    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT_TICKS));
    let mut outputs: Vec<Output> = tick(&mut joiner);
    assert_eq!(joiner.state(), WorkerState::LeaderSuspect);
    for _ in 0..5 {
        clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS));
        outputs.extend(tick(&mut joiner));
    }

    assert_eq!(joiner.state(), WorkerState::LeaderSuspect);
    assert!(published_roll_calls(&outputs).is_empty());
    // All it sends is heartbeats to the leader it joined through.
    let sent = sent(&outputs);
    assert!(!sent.is_empty(), "it keeps heartbeating its leader");
    for (to, message) in &sent {
        assert!(
            *to == worker("leader")
                && matches!(
                    message.payload,
                    Some(election_message::Payload::Heartbeat(_))
                ),
            "expected only heartbeats to its leader, got {message:?} to {to:?}"
        );
    }
}

#[test]
fn a_joiner_adopts_its_configuration_from_its_leaders_ack_and_stays_pending_without_an_admission() {
    let clock = FakeClock::new();
    let mut joiner = joined(&clock, "leader");

    deliver(
        &mut joiner,
        &worker("leader"),
        ack_message(ack_of("leader", 3, None)),
    );

    assert_eq!(joiner.configuration(), Some(&configuration_of(3)));
    assert!(joiner.is_pending_member());
}

#[test]
fn a_joiner_takes_the_admission_its_leaders_ack_names() {
    let clock = FakeClock::new();
    let mut joiner = joined(&clock, "leader");

    deliver(
        &mut joiner,
        &worker("leader"),
        ack_message(ack_of("leader", 3, Some(g0()))),
    );

    assert_eq!(joiner.admission(), Some(g0()));
    assert!(!joiner.is_pending_member());
}

#[test]
fn a_pending_member_that_knows_a_configuration_starts_a_roll_call_as_a_new_voter() {
    let clock = FakeClock::new();
    let mut joiner = joined(&clock, "leader");
    deliver(
        &mut joiner,
        &worker("leader"),
        ack_message(ack_of("leader", 3, None)),
    );

    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT_TICKS));
    tick(&mut joiner);
    let outputs = tick(&mut joiner);

    assert_eq!(joiner.state(), WorkerState::RollCall);
    let calls = published_roll_calls(&outputs);
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].term, 2, "the term after its leader's");
    assert_eq!(calls[0].configuration(), configuration_of(3));
    assert!(
        joiner.is_pending_member(),
        "starting a roll call admits no one: it counts only as a new voter"
    );
}

#[test]
fn a_pending_member_answers_roll_calls_and_grants_votes() {
    let clock = FakeClock::new();
    let mut joiner = joined(&clock, "leader");
    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT_TICKS));
    let candidate = worker("a");

    let answered = deliver(
        &mut joiner,
        &candidate,
        roll_call_message(roll_call(&candidate, 5, &configuration_of(2), 0)),
    );
    let granted = deliver(
        &mut joiner,
        &candidate,
        vote_request_message(vote_request(candidate.clone(), 0, 5)),
    );

    assert!(matches!(
        sent_to(&answered, &candidate)[0].payload,
        Some(election_message::Payload::RollCallReply(ref reply)) if reply.admission.is_none()
    ));
    assert!(matches!(
        sent_to(&granted, &candidate)[0].payload,
        Some(election_message::Payload::VoteGrant(_))
    ));
}

#[test]
fn an_election_admits_a_pending_joiner_that_answered_its_roll_call() {
    let clock = FakeClock::new();
    let node_a = voter_node(&clock, &worker("a"), 2, SUSPECT_TIMEOUT_TICKS);
    let node_b = voter_node(&clock, &worker("b"), 2, SUSPECT_TIMEOUT_TICKS);
    let joiner = joined(&clock, "a");
    let mut nodes = BTreeMap::from([
        (worker("a"), node_a),
        (worker("b"), node_b),
        (worker("joiner"), joiner),
    ]);
    let ids: Vec<WorkerId> = nodes.keys().cloned().collect();
    for id in &ids {
        let peers: Vec<WorkerId> = ids.iter().filter(|peer| *peer != id).cloned().collect();
        connect(nodes.get_mut(id).unwrap(), &peers);
    }

    // a and b suspect their (never-seen) leader and elect one between them,
    // while the joiner ticks alongside and receives whatever reaches it.
    // Every node ticks before any message it sends is delivered.
    for _ in 0..(SUSPECT_TIMEOUT_TICKS * 5) {
        clock.advance(Duration::from_ticks(1));
        let ticked: Vec<(WorkerId, Vec<Output>)> = ids
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    nodes.get_mut(id).unwrap().step(Input::Tick).outputs,
                )
            })
            .collect();
        for (id, outputs) in ticked {
            deliver_all(&mut nodes, &id, outputs);
        }
    }

    // A live election did happen around the joiner: exactly one of a and b
    // leads, and the other follows it.
    let member_states = [nodes[&worker("a")].state(), nodes[&worker("b")].state()];
    assert!(
        member_states.contains(&WorkerState::Leader)
            && member_states.contains(&WorkerState::Active),
        "a and b should have elected a leader between them, got {member_states:?}"
    );
    // The joiner answered the winning roll call, so the configuration the
    // election founded counts it. Heartbeats since have committed it: every
    // one of the three holds the founded voters alone, admitted at its base.
    let committed = nodes
        .values()
        .find(|node| node.state() == WorkerState::Leader)
        .and_then(|leader| leader.configuration().cloned())
        .expect("the leader holds a configuration");
    let founded = committed.base();
    assert!(founded > configuration_of(2).generation());
    assert_eq!(
        committed,
        Configuration::single(Single {
            generation: committed.generation(),
            base: founded,
            voter_count: 3,
        })
    );
    for id in &ids {
        assert_eq!(nodes[id].configuration(), Some(&committed), "{id:?}");
        assert_eq!(
            nodes[id].admission(),
            Some(founded),
            "{id:?} is admitted at the founded generation"
        );
        assert_eq!(nodes[id].prior_admission(), None, "{id:?}");
    }
}

#[test]
fn a_joiner_with_no_configuration_that_suspects_its_leader_still_names_it_to_joiners() {
    let clock = FakeClock::new();
    let mut joiner = bootstrapping_node(worker("joiner"), &clock);
    let _ = joiner.finish_joining(&pointer("leader", 4, 2));

    // No leader runs here to ack this joiner, so it drifts to LeaderSuspect;
    // it cannot elect a replacement, so its recorded leader is still the
    // best pointer it can give a later joiner.
    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT_TICKS));
    tick(&mut joiner);

    assert_eq!(joiner.state(), WorkerState::LeaderSuspect);
    assert_eq!(joiner.known_leader(), Some((worker("leader"), 4)));
}
