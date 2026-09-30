//! A follower's half of leader liveness (README §12.1-§12.2): the
//! `WorkerHeartbeat`s it sends its leader, the leader acks it accepts or
//! ignores, the suspicion timer an accepted ack resets, and the
//! configuration and admission generation it adopts from an accepted ack.

use crate::support::builders::{
    configuration_of, g0, past_any_suspicion, shard, timings, vote_request, vote_request_message,
    voter_of, worker,
};

use crate::support::clock::FakeClock;
use crate::support::node::{deliver, sent, sent_to, state_changes, tick};
use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{ElectionTimings, Entry, Identity, Input, Output, Step, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    AckEcho, ElectionMessage, JoinResponse, LeaderHeartbeatAck, WorkerHeartbeat, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Clock, Duration, Instant};

const SHARD: &str = "shard-1";
const OTHER_SHARD: &str = "shard-2";
const SUSPECT_TIMEOUT: u64 = 10;

fn heartbeat_ack(shard_id: &str, recovery_epoch: u64, term: u64) -> LeaderHeartbeatAck {
    LeaderHeartbeatAck {
        shard_id: Some(shard(shard_id).into()),
        leader_id: Some(worker("leader-1").into()),
        recovery_epoch,
        term,
        configuration: Some((&configuration_of(3)).into()),
        recipient_admission: Some(g0().into()),
        send_token: 0,
        recipient_prior_admission: None,
        heartbeat_token: None,
        recovery_epoch_lineage: None,
    }
}

/// An otherwise valid ack from `leader-1` in `term`, sent at `send_token`.
fn ack_sent_at(term: u64, send_token: u64) -> LeaderHeartbeatAck {
    LeaderHeartbeatAck {
        send_token,
        ..heartbeat_ack(SHARD, 0, term)
    }
}

type TestNode = WorkerNode<FakeClock>;

/// A voter of a configuration of 3, with no authority. `clock` is a shared
/// handle so the test keeps advancing it.
fn make_node(clock: &FakeClock, my_id: WorkerId, suspect_timeout: Duration) -> TestNode {
    WorkerNode::start(
        Identity {
            id: my_id,
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(suspect_timeout),
        },
        Entry::Known(voter_of(3)),
        clock.clone(),
        None,
    )
    .0
}

/// A node that joined through a JOIN naming `leader-1` in term 1, as a
/// pending member. Returns the node and what finishing the join produced.
fn joined_node(clock: &FakeClock) -> (TestNode, Vec<Output>) {
    let (node, joined) = WorkerNode::start(
        Identity {
            id: worker("joiner"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT)),
        },
        Entry::Joining(JoinResponse {
            leader_id: Some(worker("leader-1").into()),
            leader_multiaddr: "/ip4/127.0.0.1/tcp/4001".into(),
            term: 1,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        }),
        clock.clone(),
        None,
    );
    assert!(node.is_pending_member(), "setup invariant");
    (node, joined.outputs)
}

fn heartbeat_interval() -> Duration {
    timings(Duration::from_ticks(SUSPECT_TIMEOUT)).heartbeat_interval
}

/// Hands `node` `ack` from the leader it names, as that leader would send it.
fn receive_ack(node: &mut TestNode, ack: LeaderHeartbeatAck) -> Vec<Output> {
    let leader = ack.leader_id();
    deliver(
        node,
        &leader,
        ElectionMessage {
            payload: Some(election_message::Payload::HeartbeatAck(ack)),
        },
    )
}

/// The heartbeats among `outputs` sent to `leader`, in order; panics if
/// anything else was sent to it.
fn heartbeats_to(outputs: &[Output], leader: &str) -> Vec<WorkerHeartbeat> {
    sent_to(outputs, &worker(leader))
        .into_iter()
        .map(|message| match message.payload {
            Some(election_message::Payload::Heartbeat(heartbeat)) => heartbeat,
            other => panic!("expected only heartbeats to {leader}, got {other:?}"),
        })
        .collect()
}

fn echo(term: u64, send_token: u64) -> Option<AckEcho> {
    Some(AckEcho { term, send_token })
}

// ---- Heartbeats ----

#[test]
fn a_node_that_knows_no_leader_sends_no_heartbeat() {
    let clock = FakeClock::new();
    let mut node = make_node(&clock, worker("w1"), Duration::from_ticks(SUSPECT_TIMEOUT));

    clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT));
    let outputs = tick(&mut node);

    assert!(sent(&outputs).is_empty(), "{outputs:?}");
}

#[test]
fn a_follower_heartbeats_a_leader_as_soon_as_an_ack_names_it_then_every_interval() {
    let clock = FakeClock::new();
    let mut node = make_node(&clock, worker("w1"), Duration::from_ticks(SUSPECT_TIMEOUT));
    clock.advance(Duration::from_ticks(2));

    let outputs = receive_ack(&mut node, ack_sent_at(1, 7));

    let first = heartbeats_to(&outputs, "leader-1");
    assert_eq!(first.len(), 1, "{outputs:?}");
    assert_eq!(
        sent(&outputs).len(),
        1,
        "a heartbeat goes to the leader only"
    );
    assert_eq!(
        first[0],
        WorkerHeartbeat {
            worker_id: Some(worker("w1").into()),
            incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
            recovery_epoch_seen: 0,
            term_seen: 1,
            available_capacity: 0,
            active_task_runs_digest: vec![],
            shard_id: Some(shard(SHARD).into()),
            newest_accepted_ack: echo(1, 7),
            configuration_generation: Some(g0().into()),
            send_token: 2,
        }
    );

    clock.advance(Duration::from_ticks(heartbeat_interval().as_ticks() - 1));
    assert!(heartbeats_to(&tick(&mut node), "leader-1").is_empty());

    clock.advance(Duration::from_ticks(1));
    let second = heartbeats_to(&tick(&mut node), "leader-1");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].newest_accepted_ack, echo(1, 7));
}

#[test]
fn each_heartbeat_echoes_the_newest_ack_accepted_before_it() {
    let clock = FakeClock::new();
    let mut node = make_node(&clock, worker("w1"), Duration::from_ticks(SUSPECT_TIMEOUT));
    let _ = receive_ack(&mut node, ack_sent_at(1, 7));

    // An ack from the leader already heartbeated moves no heartbeat forward.
    let outputs = receive_ack(&mut node, ack_sent_at(1, 9));
    assert!(sent(&outputs).is_empty(), "{outputs:?}");
    // An ack from an older term is ignored, so it is never echoed.
    let _ = receive_ack(&mut node, ack_sent_at(0, 99));

    clock.advance(heartbeat_interval());
    let heartbeats = heartbeats_to(&tick(&mut node), "leader-1");

    assert_eq!(heartbeats.len(), 1);
    assert_eq!(heartbeats[0].newest_accepted_ack, echo(1, 9));
}

#[test]
fn a_joiner_heartbeats_its_leader_on_joining_with_no_echo_yet() {
    let clock = FakeClock::new();

    let (_node, joined) = joined_node(&clock);

    let heartbeats = heartbeats_to(&joined, "leader-1");
    assert_eq!(heartbeats.len(), 1, "{joined:?}");
    assert_eq!(heartbeats[0].newest_accepted_ack, None);
    assert_eq!(heartbeats[0].term_seen, 1);
}

#[test]
fn an_ack_returns_a_pending_member_that_suspects_its_leader_to_active() {
    let clock = FakeClock::new();
    let (mut node, _) = joined_node(&clock);
    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::LeaderSuspect);

    let outputs = receive_ack(&mut node, ack_sent_at(1, 3));

    assert_eq!(state_changes(&outputs), vec![WorkerState::Active]);
    assert_eq!(node.known_leader(), Some((worker("leader-1"), 1)));
}

#[test]
fn a_follower_in_a_roll_call_keeps_heartbeating_its_leader_until_an_ack_returns_it() {
    let clock = FakeClock::new();
    let mut node = make_node(&clock, worker("w1"), Duration::from_ticks(SUSPECT_TIMEOUT));
    let _ = receive_ack(&mut node, ack_sent_at(1, 0));
    // No other voter answers, so its roll call stays open.
    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
    tick(&mut node);
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::RollCall, "setup invariant");

    let step = node.step(Input::Tick);
    let due = step
        .next_deadline
        .expect("a follower in a roll call still has its heartbeat timer");
    clock.advance(due - clock.now());
    assert_eq!(heartbeats_to(&tick(&mut node), "leader-1").len(), 1);

    let outputs = receive_ack(&mut node, ack_sent_at(1, 20));
    assert_eq!(state_changes(&outputs), vec![WorkerState::Active]);
}

/// A voter of three that has just accepted an ack from `leader-1`, and
/// heartbeats every 4 ticks with a suspicion timeout of 10.
fn follower_heartbeating_every_4_ticks(clock: &FakeClock) -> (TestNode, Step) {
    let mut node = WorkerNode::start(
        Identity {
            id: worker("w1"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: ElectionTimings {
                heartbeat_interval: Duration::from_ticks(4),
                ..timings(Duration::from_ticks(10))
            },
        },
        Entry::Known(voter_of(3)),
        clock.clone(),
        None,
    )
    .0;
    let acked = node.step(Input::Message {
        from: worker("leader-1"),
        message: ElectionMessage {
            payload: Some(election_message::Payload::HeartbeatAck(ack_sent_at(1, 0))),
        },
    });
    (node, acked)
}

#[test]
fn a_follower_is_next_due_at_the_earlier_of_its_heartbeat_and_its_suspicion() {
    // When the follower suspects its leader, found by ticking one node on
    // every tick.
    let every_tick_clock = FakeClock::new();
    let (mut every_tick, _) = follower_heartbeating_every_4_ticks(&every_tick_clock);
    while every_tick.state() != WorkerState::LeaderSuspect {
        every_tick_clock.advance(Duration::from_ticks(1));
        let _ = every_tick.step(Input::Tick);
    }
    let suspects_at = every_tick_clock.now();
    assert!(
        suspects_at > Instant::at(10) && suspects_at <= Instant::at(0) + past_any_suspicion(10),
        "suspicion comes past +10, by less than half of it: {suspects_at:?}"
    );

    // A twin ticked only at the deadlines it reports heartbeats every 4
    // ticks until then, and suspects at the same instant.
    let clock = FakeClock::new();
    let (mut node, acked) = follower_heartbeating_every_4_ticks(&clock);
    let mut due = acked.next_deadline.expect("due to heartbeat");
    let mut heartbeated_at = Vec::new();
    while node.state() != WorkerState::LeaderSuspect {
        clock.advance(due - clock.now());
        let step = node.step(Input::Tick);
        if !heartbeats_to(&step.outputs, "leader-1").is_empty() {
            heartbeated_at.push(clock.now());
        }
        due = step
            .next_deadline
            .expect("a follower always has a deadline");
    }
    assert_eq!(clock.now(), suspects_at);
    let heartbeats_before: Vec<Instant> = [4, 8, 12]
        .into_iter()
        .map(Instant::at)
        .filter(|at| *at < suspects_at)
        .collect();
    assert_eq!(heartbeated_at, heartbeats_before);
}

// ---- Accepting and ignoring acks ----

// Verified through a `Tick`: had the ack been accepted, leader contact would
// move and the node would not suspect its leader. `WorkerNode` has no
// test-only accessor for it.
#[test]
fn mismatched_shard_id_is_ignored() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let mut node = make_node(&clock, worker("w1"), suspect_timeout);

    // Otherwise-valid ack, but for a different shard: must be ignored.
    clock.advance(Duration::from_ticks(6));
    receive_ack(&mut node, heartbeat_ack(OTHER_SHARD, 0, 0));

    // Elapsed since construction is 15, past any jittered timeout of 10, so
    // Suspect; had the mismatched ack been accepted it would be 9 and Active.
    clock.advance(Duration::from_ticks(9));
    tick(&mut node);

    assert_eq!(node.state(), WorkerState::LeaderSuspect);
}

// An ack from a later recovery epoch means the shard was recovered through
// the authority without this node: it adopts that epoch and the leader's
// configuration there, and holds no admission from the old epoch, even
// though the leader's term is below the highest it had seen.
#[test]
fn a_voter_adopts_a_later_recovery_epoch_from_its_leaders_ack() {
    let clock = FakeClock::new();
    let mut node = make_node(&clock, worker("w1"), Duration::from_ticks(10));
    receive_ack(&mut node, heartbeat_ack(SHARD, 0, 7));
    let recovered = Configuration::single(Single {
        generation: Generation::new(1, 2, 1),
        base: Generation::new(1, 2, 1),
        voter_count: 2,
    }).expect("valid");

    receive_ack(
        &mut node,
        LeaderHeartbeatAck {
            leader_id: Some(worker("recovered-leader").into()),
            recovery_epoch: 1,
            term: 2,
            configuration: Some((&recovered).into()),
            recipient_admission: None,
            ..heartbeat_ack(SHARD, 1, 2)
        },
    );

    assert_eq!(node.recovery_epoch(), 1);
    assert_eq!(node.highest_term_seen(), 2);
    assert_eq!(node.configuration(), Some(&recovered));
    assert!(
        node.is_pending_member(),
        "its old admission counts for nothing"
    );
    assert_eq!(node.known_leader(), Some((worker("recovered-leader"), 2)));
    assert_eq!(node.state(), WorkerState::Active);

    // An ack from the old epoch's leader no longer counts.
    receive_ack(&mut node, heartbeat_ack(SHARD, 0, 8));
    assert_eq!(node.recovery_epoch(), 1);
    assert_eq!(node.known_leader(), Some((worker("recovered-leader"), 2)));
}

#[test]
fn a_node_that_suspects_its_leader_knows_no_leader() {
    let clock = FakeClock::new();
    let mut node = make_node(&clock, worker("w1"), Duration::from_ticks(10));
    receive_ack(&mut node, heartbeat_ack(SHARD, 0, 3));

    clock.advance(past_any_suspicion(10));
    tick(&mut node);

    assert_eq!(node.state(), WorkerState::LeaderSuspect);
    assert_eq!(node.known_leader(), None);
}

#[test]
fn a_leader_knows_itself_as_leader_in_its_own_term() {
    let clock = FakeClock::new();
    let mut node = WorkerNode::start(
        Identity {
            id: worker("w1"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(10)),
        },
        Entry::Known(voter_of(1)),
        clock.clone(),
        None,
    )
    .0;

    // A one-voter configuration elects its node alone: Active ->
    // LeaderSuspect, then LeaderSuspect -> RollCall, then at the roll call's
    // deadline RollCall -> Candidate -> Leader on its own reply and vote.
    clock.advance(past_any_suspicion(10));
    tick(&mut node);
    tick(&mut node);
    clock.advance(timings(Duration::from_ticks(10)).roll_call_deadline);
    tick(&mut node);

    assert_eq!(node.state(), WorkerState::Leader);
    assert_eq!(node.known_leader(), Some((worker("w1"), node.term())));
}

#[test]
fn a_node_that_has_seen_a_later_term_than_its_leaders_names_no_leader() {
    let clock = FakeClock::new();
    let mut node = make_node(&clock, worker("w1"), Duration::from_ticks(10));
    receive_ack(&mut node, heartbeat_ack(SHARD, 0, 3));
    clock.advance(past_any_suspicion(10));
    // A roll call it answers, then a vote it grants, in term 4.
    let candidate = worker("w2");
    deliver(
        &mut node,
        &candidate,
        crate::support::builders::roll_call_message(crate::support::builders::roll_call(
            &candidate,
            4,
            &configuration_of(3),
            0,
        )),
    );
    deliver(
        &mut node,
        &candidate,
        vote_request_message(vote_request(candidate.clone(), 0, 4)),
    );
    assert_eq!(node.state(), WorkerState::Active, "setup invariant");

    assert_eq!(node.known_leader(), None);
}

// ---- Adopting the leader's configuration ----

fn configuration_at(counter: u64, voter_count: usize) -> Configuration {
    Configuration::single(Single {
        generation: Generation::new(0, 0, counter),
        base: g0(),
        voter_count,
    }).expect("valid")
}

fn ack_carrying(
    configuration: &Configuration,
    recipient_admission: Option<Generation>,
) -> LeaderHeartbeatAck {
    LeaderHeartbeatAck {
        configuration: Some(configuration.into()),
        recipient_admission: recipient_admission.map(Into::into),
        ..heartbeat_ack(SHARD, 0, 1)
    }
}

#[test]
fn an_ignored_ack_changes_neither_configuration_nor_admission() {
    let clock = FakeClock::new();
    let mut node = make_node(&clock, worker("w1"), Duration::from_ticks(10));
    receive_ack(&mut node, heartbeat_ack(SHARD, 0, 3));

    clock.advance(Duration::from_ticks(6));
    let stale = LeaderHeartbeatAck {
        term: 2,
        leader_id: Some(worker("leader-2").into()),
        ..ack_carrying(&configuration_at(5, 2), Some(Generation::new(0, 2, 5)))
    };
    receive_ack(&mut node, stale);

    assert_eq!(node.configuration(), Some(&configuration_of(3)));
    assert_eq!(node.admission(), Some(g0()));
    assert_eq!(node.known_leader(), Some((worker("leader-1"), 3)));
    // Had the stale ack refreshed leader contact, the node would still be
    // within its suspicion timeout here.
    clock.advance(Duration::from_ticks(9));
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::LeaderSuspect);
}

#[test]
fn an_ack_from_someone_other_than_the_leader_it_names_is_ignored() {
    let clock = FakeClock::new();
    let mut node = make_node(&clock, worker("w1"), Duration::from_ticks(10));
    clock.advance(Duration::from_ticks(8));

    deliver(
        &mut node,
        &worker("impostor"),
        ElectionMessage {
            payload: Some(election_message::Payload::HeartbeatAck(heartbeat_ack(
                SHARD, 0, 1,
            ))),
        },
    );

    assert_eq!(node.known_leader(), None);
    // Had the ack been accepted, leader contact would be at tick 8 and tick
    // 15 would still be within the suspicion timeout.
    clock.advance(Duration::from_ticks(7));
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::LeaderSuspect);
}
