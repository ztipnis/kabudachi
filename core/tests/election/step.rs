//! `WorkerNode::step` as a driver sees it: the connected peers it keeps from
//! connection events, the next deadline it reports in each state and that a
//! `Tick` at that deadline always moves it on, a new leader's announcement
//! to its connected roster, a drain requested in any state, the order in
//! which it reports its state changes, and what applying a step's outputs
//! does to the worker's scheduler.

use crate::support::builders::{
    ack_message, configuration_of, g0, heartbeat, heartbeat_message, leader_ack, no_leader_yet,
    past_any_suspicion, roll_call, roll_call_message, roll_call_reply, shard, timings, vote_grant,
    voter_of, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{
    connect, deliver, sent, sent_to, stand_as_candidate, start_roll_call, state_changes, tick,
};
use kabudachi_core::election::{ElectionTimings, Entry, Identity, Input, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::{
    AckEcho, ElectionMessage, JoinResponse, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Clock, Duration, Instant};

const SHARD: &str = "shard-1";

/// Suspicion after 10 ticks.
const SUSPECT_TIMEOUT: u64 = 10;

type TestNode = WorkerNode<FakeClock>;

/// `my_id`'s node, a voter of a configuration with one voter per worker in
/// `electorate`.
fn node(clock: &FakeClock, my_id: &WorkerId, electorate: &[WorkerId]) -> TestNode {
    WorkerNode::start(
        Identity {
            id: my_id.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT)),
        },
        Entry::Known(voter_of(electorate.len())),
        clock.clone(),
        None,
    )
    .0
}

fn bootstrapping_node(clock: &FakeClock, my_id: &WorkerId) -> TestNode {
    WorkerNode::start(
        Identity {
            id: my_id.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT)),
        },
        Entry::Joining(no_leader_yet()),
        clock.clone(),
        None,
    )
    .0
}

fn ticks_after(start: Instant, ticks: u64) -> Instant {
    start + Duration::from_ticks(ticks)
}

fn heartbeat_ack_from(leader: &WorkerId, term: u64) -> ElectionMessage {
    ack_message(leader_ack(leader, term, &configuration_of(2), Some(g0())))
}

fn vote_grant_message(candidate: &WorkerId, voter: &WorkerId, term: u64) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::VoteGrant(vote_grant(
            candidate.clone(),
            voter.clone(),
            term,
        ))),
    }
}

fn is_heartbeat_ack(message: &ElectionMessage) -> bool {
    matches!(
        message.payload,
        Some(election_message::Payload::HeartbeatAck(_))
    )
}

/// The recipients of the heartbeat acks among `outputs`, in order.
fn acked(outputs: &[Output]) -> Vec<WorkerId> {
    sent(outputs)
        .into_iter()
        .filter(|(_, message)| is_heartbeat_ack(message))
        .map(|(to, _)| to)
        .collect()
}

/// A voter of 3, connected to both peers, standing as `Candidate` for term
/// 1 after a real roll call both peers answered. `peer_a`'s grant alone wins
/// it the election. Returns `(node, self_id, peer_a, peer_b)`.
fn candidate_of_three(clock: &FakeClock) -> (TestNode, WorkerId, WorkerId, WorkerId) {
    let (self_id, peer_a, peer_b) = (worker("w1"), worker("peer-a"), worker("peer-b"));
    let mut node = node(
        clock,
        &self_id,
        &[self_id.clone(), peer_a.clone(), peer_b.clone()],
    );
    connect(&mut node, &[peer_a.clone(), peer_b.clone()]);

    stand_as_candidate(
        &mut node,
        clock,
        SUSPECT_TIMEOUT,
        &[peer_a.clone(), peer_b.clone()],
    );
    (node, self_id, peer_a, peer_b)
}

/// `candidate_of_three`'s node after `peer_a`'s grant has won it term 1.
/// Returns `(node, self_id, peer_a, peer_b, outputs)`, where `outputs` are
/// those of the step that won.
fn leader_of_three(clock: &FakeClock) -> (TestNode, WorkerId, WorkerId, WorkerId, Vec<Output>) {
    let (mut node, self_id, peer_a, peer_b) = candidate_of_three(clock);
    let outputs = deliver(&mut node, &peer_a, vote_grant_message(&self_id, &peer_a, 1));
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");
    (node, self_id, peer_a, peer_b, outputs)
}

// ---- Connection events ----

#[test]
fn connection_events_decide_whom_a_new_leader_announces_itself_to() {
    let clock = FakeClock::new();
    let (mut node, self_id, peer_a, peer_b) = candidate_of_three(&clock);

    let _ = node.step(Input::PeerDisconnected(peer_b.clone()));
    // Reporting the same connection twice changes nothing.
    let _ = node.step(Input::PeerConnected(peer_a.clone()));
    let _ = node.step(Input::PeerConnected(peer_a.clone()));
    let won = deliver(&mut node, &peer_a, vote_grant_message(&self_id, &peer_a, 1));

    assert_eq!(acked(&won), vec![peer_a]);
}

// ---- A new leader's announcement ----

#[test]
fn a_new_leader_also_acks_a_connected_peer_that_missed_its_roll_call() {
    let clock = FakeClock::new();
    let (mut node, self_id, peer_a, _peer_b) = candidate_of_three(&clock);
    let outsider = worker("missed-the-roll-call");
    connect(&mut node, std::slice::from_ref(&outsider));

    let won = deliver(&mut node, &peer_a, vote_grant_message(&self_id, &peer_a, 1));

    let to_outsider = sent_to(&won, &outsider);
    assert_eq!(to_outsider.len(), 1, "{won:?}");
    match &to_outsider[0].payload {
        Some(election_message::Payload::HeartbeatAck(ack)) => assert_eq!(
            ack.recipient_admission, None,
            "it is not in the roster, so its ack names no admission and it keeps its own"
        ),
        other => panic!("expected an ack, got {other:?}"),
    }
}

// ---- The next deadline in each state ----

#[test]
fn an_active_node_is_next_due_just_past_its_jittered_suspicion_timeout() {
    let clock = FakeClock::new();
    let started = clock.now();
    let mut node = node(&clock, &worker("w1"), &[worker("w1"), worker("w2")]);

    let suspicion_due = node
        .step(Input::PeerConnected(worker("w2")))
        .next_deadline
        .expect("an active node is due to suspect its leader");
    assert!(
        suspicion_due > ticks_after(started, SUSPECT_TIMEOUT)
            && suspicion_due <= started + past_any_suspicion(SUSPECT_TIMEOUT),
        "{suspicion_due:?}"
    );

    // Just before its deadline the node is not yet suspicious.
    clock.advance(Duration::from_ticks(
        (suspicion_due - clock.now()).as_ticks() - 1,
    ));
    let step = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::Active);
    assert_eq!(step.next_deadline, Some(suspicion_due));

    clock.advance(Duration::from_ticks(1));
    let _ = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::LeaderSuspect);
}

#[test]
fn an_accepted_ack_moves_an_active_nodes_deadline() {
    let clock = FakeClock::new();
    let leader = worker("leader-1");
    let mut node = node(&clock, &worker("w1"), &[worker("w1"), worker("w2")]);

    // Built at 0, the node would suspect in (10, 15]; the ack at 10 moves
    // that to (20, 25].
    clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT));
    let acked_at = clock.now();
    let step = node.step(Input::Message {
        from: leader.clone(),
        message: heartbeat_ack_from(&leader, 0),
    });

    // Ticked only at the deadlines it reports (its heartbeats among them),
    // it suspects at the moved one.
    let mut due = step
        .next_deadline
        .expect("a follower always has a deadline");
    while node.state() != WorkerState::LeaderSuspect {
        clock.advance(due - clock.now());
        due = node
            .step(Input::Tick)
            .next_deadline
            .expect("a follower always has a deadline");
    }
    assert!(
        clock.now() > ticks_after(acked_at, SUSPECT_TIMEOUT)
            && clock.now() <= acked_at + past_any_suspicion(SUSPECT_TIMEOUT),
        "{:?}",
        clock.now()
    );
}

#[test]
fn a_voter_suspecting_its_leader_is_due_at_once() {
    let clock = FakeClock::new();
    let mut node = node(&clock, &worker("w1"), &[worker("w1"), worker("w2")]);

    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
    let step = node.step(Input::Tick);

    assert_eq!(node.state(), WorkerState::LeaderSuspect);
    assert_eq!(step.next_deadline, Some(clock.now()));
}

#[test]
fn a_pending_member_suspecting_its_leader_is_next_due_at_its_next_heartbeat() {
    let clock = FakeClock::new();
    let mut node = bootstrapping_node(&clock, &worker("joiner"));
    let _ = node.step(Input::JoinAnswer(JoinResponse {
        leader_id: Some(worker("leader-1").into()),
        leader_multiaddr: "/ip4/127.0.0.1/tcp/4001".into(),
        term: 1,
        recovery_epoch: 0,
        recovery_epoch_lineage: 0,
    }));

    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
    let step = node.step(Input::Tick);

    // It starts no roll call, so only its heartbeat timer is left; this
    // `Tick` sent the heartbeat that was overdue.
    assert_eq!(node.state(), WorkerState::LeaderSuspect);
    assert_eq!(
        step.next_deadline,
        Some(clock.now() + timings(Duration::from_ticks(SUSPECT_TIMEOUT)).heartbeat_interval)
    );
}

#[test]
fn a_bootstrapping_node_has_no_deadline() {
    let clock = FakeClock::new();
    let mut node = bootstrapping_node(&clock, &worker("joiner"));

    clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT * 5));
    let step = node.step(Input::Tick);

    assert_eq!(node.state(), WorkerState::Bootstrapping);
    assert_eq!(step.next_deadline, None);
}

#[test]
fn a_node_in_a_roll_call_or_standing_as_candidate_is_due_at_its_deadline() {
    let clock = FakeClock::new();
    let (self_id, other) = (worker("w1"), worker("w2"));
    let mut node = node(
        &clock,
        &self_id,
        &[self_id.clone(), other.clone(), worker("w3")],
    );
    let roll_call_deadline = timings(Duration::from_ticks(SUSPECT_TIMEOUT)).roll_call_deadline;

    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
    tick(&mut node);
    let roll_call_started = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::RollCall);
    assert_eq!(
        roll_call_started.next_deadline,
        Some(clock.now() + roll_call_deadline)
    );

    // One more reply is a quorum of 2 of 3, so the node stands at its
    // deadline.
    let _ = node.step(Input::Message {
        from: other.clone(),
        message: roll_call_reply(&self_id, 1, &other, Some(g0())),
    });
    clock.advance(roll_call_deadline);
    let stood = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::Candidate);
    assert_eq!(stood.next_deadline, Some(clock.now() + roll_call_deadline));
}

#[test]
fn a_voter_that_accepted_a_roll_call_is_next_due_when_that_calls_vote_could_have_ended() {
    let clock = FakeClock::new();
    let (w2, leader) = (worker("w2"), worker("leader-1"));
    // A call and its vote, one tick each, end before the next heartbeat is
    // due.
    let mut node = WorkerNode::start(
        Identity {
            id: worker("w1"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: ElectionTimings::new(
                Duration::from_ticks(SUSPECT_TIMEOUT),
                Duration::from_ticks(4),
            )
            .with_roll_call_deadline(Duration::from_ticks(1)),
        },
        Entry::Known(voter_of(3)),
        clock.clone(),
        None,
    )
    .0;
    deliver(&mut node, &leader, heartbeat_ack_from(&leader, 0));
    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
    deliver(
        &mut node,
        &w2,
        roll_call_message(roll_call(&w2, 1, &configuration_of(3), 0)),
    );

    let step = node.step(Input::Tick);

    assert_eq!(node.state(), WorkerState::LeaderSuspect);
    assert_eq!(step.next_deadline, Some(ticks_after(clock.now(), 2)));
}

// ---- A Tick at the node's deadline always moves it on ----

/// Steps `node` with a `Tick` at every deadline it reports, until it reports
/// none or `steps` ticks have passed, and returns each state it was ticked
/// in. Each `Tick`, at the very instant the node reported, must move the
/// node into another state or put its deadline later (or away): a driver
/// that ticks its node while its deadline has come, as `net`'s `run_driver`
/// does, would otherwise tick it for ever without letting time pass.
fn tick_at_every_deadline(
    node: &mut TestNode,
    clock: &FakeClock,
    first_deadline: Option<Instant>,
    steps: usize,
) -> Vec<WorkerState> {
    let mut ticked_in = Vec::new();
    let mut deadline = first_deadline;
    while let Some(due) = deadline
        && ticked_in.len() < steps
    {
        clock.advance(due - clock.now());
        let before = node.state();
        let next = node.step(Input::Tick).next_deadline;
        assert!(
            node.state() != before || next.is_none_or(|next| next > due),
            "a Tick at its deadline {due:?} left a {before:?} node due again at {next:?}"
        );
        ticked_in.push(before);
        deadline = next;
    }
    ticked_in
}

/// Panics unless every one of `states` is among `ticked_in`.
fn assert_ticked_in(ticked_in: &[WorkerState], states: &[WorkerState]) {
    for state in states {
        assert!(
            ticked_in.contains(state),
            "{state:?} missing: {ticked_in:?}"
        );
    }
}

#[test]
fn a_tick_at_the_deadline_moves_a_node_on_in_every_state_that_reports_one() {
    let clock = FakeClock::new();
    let (w1, w2) = (worker("w1"), worker("w2"));

    // A voter with no leader: suspects, then starts roll calls no one
    // answers, each closing short of a quorum, and retries from `NoQuorum`.
    let mut voter = node(&clock, &w1, &[w1.clone(), w2.clone()]);
    let first = voter.step(Input::PeerConnected(w2.clone())).next_deadline;
    assert_ticked_in(
        &tick_at_every_deadline(&mut voter, &clock, first, 50),
        &[
            WorkerState::Active,
            WorkerState::LeaderSuspect,
            WorkerState::RollCall,
            WorkerState::NoQuorum,
        ],
    );

    // A follower whose leader stops acking: heartbeats it, suspects it, and
    // keeps heartbeating it through its roll calls.
    let mut follower = node(&clock, &w1, &[w1.clone(), w2.clone()]);
    let first = deliver_step(&mut follower, &w2, heartbeat_ack_from(&w2, 1));
    assert_ticked_in(
        &tick_at_every_deadline(&mut follower, &clock, first, 50),
        &[
            WorkerState::Active,
            WorkerState::LeaderSuspect,
            WorkerState::RollCall,
            WorkerState::NoQuorum,
        ],
    );

    // A follower that has accepted another's roll call: suspects its
    // leader, keeps heartbeating it, and starts a roll call of its own once
    // that call's deadline has passed.
    let mut suppressed = node(&clock, &w1, &[w1.clone(), w2.clone()]);
    let _ = deliver_step(&mut suppressed, &w2, heartbeat_ack_from(&w2, 0));
    let call_at = clock.now() + past_any_suspicion(SUSPECT_TIMEOUT);
    clock.advance(call_at - clock.now());
    let initiator = worker("w3");
    let _ = deliver_step(
        &mut suppressed,
        &initiator,
        roll_call_message(roll_call(&initiator, 1, &configuration_of(2), 0)),
    );
    let first = suppressed.step(Input::Tick).next_deadline;
    assert_ticked_in(
        &tick_at_every_deadline(&mut suppressed, &clock, first, 20),
        &[WorkerState::LeaderSuspect, WorkerState::RollCall],
    );

    // A joiner with no configuration whose leader stops acking: suspects it
    // and keeps heartbeating it.
    let mut joiner = bootstrapping_node(&clock, &worker("joiner"));
    let first = joiner.step(Input::JoinAnswer(JoinResponse {
            leader_id: Some(w2.clone().into()),
            leader_multiaddr: "/ip4/127.0.0.1/tcp/4001".into(),
            term: 1,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        }))
        .next_deadline;
    assert_ticked_in(
        &tick_at_every_deadline(&mut joiner, &clock, first, 50),
        &[WorkerState::Active, WorkerState::LeaderSuspect],
    );

    // A candidate no one grants: gives up at its vote's deadline and
    // suspects its leader again.
    let (mut candidate, ..) = candidate_of_three(&clock);
    let first = candidate.step(Input::Tick).next_deadline;
    assert_ticked_in(
        &tick_at_every_deadline(&mut candidate, &clock, first, 20),
        &[WorkerState::Candidate, WorkerState::LeaderSuspect],
    );

    // An initiator that abandoned its call for a better one, whose
    // initiator never wins: gives its own up at its deadline.
    clock.set_wall_clock_millis(500);
    let mut abandoned = node(&clock, &w2, &[w1.clone(), w2.clone(), worker("w3")]);
    let _ = start_roll_call(&mut abandoned, &clock, SUSPECT_TIMEOUT);
    let first = deliver_step(
        &mut abandoned,
        &w1,
        roll_call_message(roll_call(&w1, 1, &configuration_of(3), 500)),
    );
    assert_ticked_in(
        &tick_at_every_deadline(&mut abandoned, &clock, first, 20),
        &[WorkerState::RollCall, WorkerState::LeaderSuspect],
    );

    // A leader that no member confirms, then one whose lease a member's
    // confirmation started: each goes `NoQuorum`, and from there retries a
    // roll call a suspicion timeout later.
    let (mut unconfirmed, ..) = leader_of_three(&clock);
    let first = unconfirmed.step(Input::Tick).next_deadline;
    let ticked_in = tick_at_every_deadline(&mut unconfirmed, &clock, first, 50);
    assert_eq!(ticked_in[..2], [WorkerState::Leader, WorkerState::NoQuorum]);
    assert_ticked_in(&ticked_in, &[WorkerState::RollCall]);

    let (mut confirmed, _, peer_a, ..) = leader_of_three(&clock);
    let echo = AckEcho {
        term: 1,
        send_token: clock.now().as_ticks(),
    };
    let first = deliver_step(
        &mut confirmed,
        &peer_a,
        heartbeat_message(heartbeat(&peer_a, Some(echo))),
    );
    let ticked_in = tick_at_every_deadline(&mut confirmed, &clock, first, 50);
    assert_eq!(ticked_in[..2], [WorkerState::Leader, WorkerState::NoQuorum]);

    // A lone voter elects itself at its roll call's deadline, and a lone
    // leader's lease never ends.
    let mut lone = node(&clock, &w1, std::slice::from_ref(&w1));
    let first = lone.step(Input::Tick).next_deadline;
    assert_eq!(
        tick_at_every_deadline(&mut lone, &clock, first, 50),
        vec![
            WorkerState::Active,
            WorkerState::LeaderSuspect,
            WorkerState::RollCall
        ]
    );
    assert_eq!(lone.state(), WorkerState::Leader);
}

/// Hands `node` `message` from `from` and returns the deadline it reports.
fn deliver_step(node: &mut TestNode, from: &WorkerId, message: ElectionMessage) -> Option<Instant> {
    node.step(Input::Message {
        from: from.clone(),
        message,
    })
    .next_deadline
}

// ---- A drain in any state ----

#[test]
fn a_drain_while_suspecting_the_leader_waits_until_the_node_can_drain() {
    let clock = FakeClock::new();
    let solo = worker("solo");
    let mut node = node(&clock, &solo, std::slice::from_ref(&solo));
    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::LeaderSuspect);

    let asked = node.step(Input::Drain);
    assert!(asked.outputs.is_empty(), "{asked:?}");
    assert_eq!(node.state(), WorkerState::LeaderSuspect);

    // Its roll call elects it at the call's deadline, and the kept drain
    // applies in the same step.
    let started = node.step(Input::Tick);
    assert_eq!(state_changes(&started.outputs), vec![WorkerState::RollCall]);
    clock.advance(timings(Duration::from_ticks(SUSPECT_TIMEOUT)).roll_call_deadline);
    let elected = node.step(Input::Tick);
    assert_eq!(
        state_changes(&elected.outputs),
        vec![
            WorkerState::Candidate,
            WorkerState::LeaderReconciling,
            WorkerState::Leader,
            WorkerState::Draining,
            WorkerState::Stopped,
        ]
    );
    assert_eq!(elected.next_deadline, None);
}

#[test]
fn a_drain_during_a_roll_call_applies_once_the_leader_is_heard_again() {
    let clock = FakeClock::new();
    let (w1, leader) = (worker("w1"), worker("leader-1"));
    let mut node = node(&clock, &w1, &[w1.clone(), leader.clone()]);
    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
    tick(&mut node);
    tick(&mut node);
    assert_eq!(node.state(), WorkerState::RollCall);

    assert!(node.step(Input::Drain).outputs.is_empty());
    assert_eq!(node.state(), WorkerState::RollCall);

    let heard = deliver(&mut node, &leader, heartbeat_ack_from(&leader, 0));
    assert_eq!(
        state_changes(&heard),
        vec![
            WorkerState::Active,
            WorkerState::Draining,
            WorkerState::Stopped
        ]
    );
}

#[test]
fn a_drain_while_bootstrapping_applies_once_the_node_has_joined() {
    let clock = FakeClock::new();
    let mut node = bootstrapping_node(&clock, &worker("joiner"));

    assert!(node.step(Input::Drain).outputs.is_empty());
    assert_eq!(node.state(), WorkerState::Bootstrapping);

    let joined = node.step(Input::JoinAnswer(JoinResponse {
        leader_id: Some(worker("leader-1").into()),
        leader_multiaddr: "/ip4/127.0.0.1/tcp/4001".into(),
        term: 1,
        recovery_epoch: 0,
        recovery_epoch_lineage: 0,
    }));
    assert_eq!(
        state_changes(&joined.outputs),
        vec![
            WorkerState::Joining,
            WorkerState::Active,
            WorkerState::Draining,
            WorkerState::Stopped,
        ]
    );
}

#[test]
fn a_second_drain_changes_nothing() {
    let clock = FakeClock::new();
    let (w1, w2) = (worker("w1"), worker("w2"));
    let mut node = node(&clock, &w1, &[w1.clone(), w2.clone()]);
    connect(&mut node, &[w2]);
    let _ = node.step(Input::Drain);
    assert_eq!(node.state(), WorkerState::Stopped);

    let again = node.step(Input::Drain);

    assert!(again.outputs.is_empty(), "{again:?}");
    assert_eq!(node.state(), WorkerState::Stopped);
}

// ---- The order of reported state changes ----

// ---- Construction ----

#[test]
#[should_panic(expected = "roll_call_deadline")]
fn a_zero_roll_call_deadline_is_rejected_at_construction() {
    let clock = FakeClock::new();

    let _ = WorkerNode::start(
        Identity {
            id: worker("w1"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: ElectionTimings {
                roll_call_deadline: Duration::from_ticks(0),
                ..timings(Duration::from_ticks(10))
            },
        },
        Entry::Known(voter_of(1)),
        clock,
        None,
    )
    .0;
}

#[test]
#[should_panic(expected = "heartbeat_interval")]
fn a_zero_heartbeat_interval_is_rejected_at_construction() {
    let clock = FakeClock::new();

    let _ = WorkerNode::start(
        Identity {
            id: worker("w1"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: ElectionTimings::new(Duration::from_ticks(10), Duration::from_ticks(0))
                .with_roll_call_deadline(Duration::from_ticks(1)),
        },
        Entry::Known(voter_of(1)),
        clock,
        None,
    )
    .0;
}

#[test]
#[should_panic(expected = "clock_drift_divisor")]
fn a_zero_clock_drift_divisor_is_rejected_at_construction() {
    let clock = FakeClock::new();

    let _ = WorkerNode::start(
        Identity {
            id: worker("w1"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: ElectionTimings {
                clock_drift_divisor: 0,
                ..timings(Duration::from_ticks(10))
            },
        },
        Entry::Known(voter_of(1)),
        clock,
        None,
    )
    .0;
}

#[test]
fn a_lone_voter_may_run_heartbeats_that_could_keep_no_lease() {
    // With no suspicion timeout there is no lease at all, but a node that
    // is alone a quorum never needs one.
    let node = WorkerNode::start(
        Identity {
            id: worker("w1"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(0)),
        },
        Entry::Known(voter_of(1)),
        FakeClock::new(),
        None,
    )
    .0;

    assert_eq!(node.state(), WorkerState::Active);
}

#[test]
#[should_panic(expected = "must be shorter than the lease length")]
fn a_voter_of_several_whose_heartbeats_cannot_keep_a_lease_is_rejected_at_construction() {
    let clock = FakeClock::new();

    // A lease of 10 ticks less a fifth is 8 ticks: two heartbeat intervals
    // of 4 do not fit strictly inside it.
    let _ = WorkerNode::start(
        Identity {
            id: worker("w1"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: ElectionTimings {
                heartbeat_interval: Duration::from_ticks(4),
                clock_drift_divisor: 5,
                ..timings(Duration::from_ticks(10))
            },
        },
        Entry::Known(voter_of(3)),
        clock,
        None,
    )
    .0;
}

#[test]
#[should_panic(expected = "must be shorter than the lease length")]
fn a_joining_node_whose_heartbeats_cannot_keep_a_lease_is_rejected_at_construction() {
    let clock = FakeClock::new();

    // A joiner's shard already has a leader, whose lease its heartbeats
    // must keep: here a suspicion timeout of 0 leaves no lease at all.
    let _ = WorkerNode::start(
        Identity {
            id: worker("w1"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(0)),
        },
        Entry::Joining(no_leader_yet()),
        clock,
        None,
    )
    .0;
}
