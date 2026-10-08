//! `WorkerNode::step` as a driver sees it: a `Tick` at the deadline a node
//! reports always moves it on, a drain requested while a roll call is open is
//! kept until the leader is heard, and a node refuses timings it could not run
//! an election on.

use crate::support::builders::{
    message_input,
    ack_message, configuration_of, g0, heartbeat, heartbeat_message, leader_ack, no_leader_yet,
    past_any_suspicion, roll_call, roll_call_message, shard, timings, vote_grant,
    vote_request, vote_request_message,
    voter_of, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{
    connect, deliver, finish_reconciling, stand_as_candidate, start_roll_call,
    state_changes, tick,
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
    let mut outputs = deliver(&mut node, &peer_a, vote_grant_message(&self_id, &peer_a, 1));
    outputs.extend(finish_reconciling(&mut node));
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");
    (node, self_id, peer_a, peer_b, outputs)
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
        // Every state but these reports a deadline: a node never waits for a
        // Tick it has not asked for. Only the lone voter below ends in
        // `LeaderReconciling` with none; the peered leaders end in `NoQuorum`
        // (their `ticked_in` assertions), so a peered leader never lands here.
        assert!(
            next.is_some()
                || matches!(
                    node.state(),
                    WorkerState::Bootstrapping
                        | WorkerState::Stopped
                        | WorkerState::LeaderReconciling
                        | WorkerState::Leader
                ),
            "a {:?} node reports no deadline",
            node.state()
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
fn a_node_names_its_leader_to_joiners_only_while_it_can_follow_it() {
    let clock = FakeClock::new();
    let (w1, w2) = (worker("w1"), worker("w2"));

    // A voter that suspects its leader names none to joiners.
    let mut suspecting = node(&clock, &w1, &[w1.clone(), w2.clone()]);
    let _ = deliver_step(&mut suspecting, &w2, heartbeat_ack_from(&w2, 1));
    assert_eq!(suspecting.known_leader(), Some((w2.clone(), 1)));
    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
    let _ = suspecting.step(Input::Tick);
    assert_eq!(suspecting.state(), WorkerState::LeaderSuspect);
    assert_eq!(suspecting.known_leader(), None);

    // A follower that has granted a vote in a term later than its leader's
    // names no leader: a newer one may lead by now.
    let mut outvoted = node(&clock, &w1, &[w1.clone(), w2.clone(), worker("w3")]);
    let _ = deliver_step(&mut outvoted, &w2, heartbeat_ack_from(&w2, 3));
    assert_eq!(outvoted.known_leader(), Some((w2.clone(), 3)));
    clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
    let candidate = worker("w3");
    let _ = deliver_step(
        &mut outvoted,
        &candidate,
        roll_call_message(roll_call(&candidate, 4, &configuration_of(3), 0)),
    );
    let _ = deliver_step(
        &mut outvoted,
        &candidate,
        vote_request_message(vote_request(candidate.clone(), 0, 4)),
    );
    assert_eq!(outvoted.state(), WorkerState::Active);
    assert_eq!(outvoted.known_leader(), None);
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
    assert_eq!(follower.known_leader(), Some((w2.clone(), 1)));
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
    clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT * 5));
    assert_eq!(
        joiner.step(Input::Tick).next_deadline,
        None,
        "a joining node waits for its answer with no deadline"
    );
    let pointer = JoinResponse {
        leader_id: Some(w2.clone().into()),
        leader_multiaddr: "/ip4/127.0.0.1/tcp/4001".into(),
        term: 1,
        recovery_epoch: 0,
        recovery_epoch_lineage: 0,
        shard_id: Some(shard(SHARD).into()),
    };
    // A leader of another incarnation of the shard leads nothing this node
    // belongs to.
    let _ = joiner.step(Input::JoinAnswer(JoinResponse {
        shard_id: Some(shard("shard-2").into()),
        ..pointer.clone()
    }));
    assert_eq!(joiner.state(), WorkerState::Bootstrapping);
    let first = joiner.step(Input::JoinAnswer(pointer)).next_deadline;
    let ticked_in = tick_at_every_deadline(&mut joiner, &clock, first, 50);
    assert_ticked_in(&ticked_in, &[WorkerState::Active, WorkerState::LeaderSuspect]);
    assert!(
        !ticked_in.contains(&WorkerState::RollCall),
        "a joiner with no configuration never calls a roll call: {ticked_in:?}"
    );
    assert_eq!(joiner.state(), WorkerState::LeaderSuspect);
    assert_eq!(
        joiner.known_leader(),
        Some((w2.clone(), 1)),
        "a joiner that suspects its leader still names it to joiners"
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
    assert_eq!(lone.state(), WorkerState::LeaderReconciling);
    let led = finish_reconciling(&mut lone);
    assert_eq!(state_changes(&led), vec![WorkerState::Leader]);
    assert_eq!(lone.step(Input::Tick).next_deadline, None);
}

/// Hands `node` `message` from `from` and returns the deadline it reports.
fn deliver_step(node: &mut TestNode, from: &WorkerId, message: ElectionMessage) -> Option<Instant> {
    node.step(message_input(&from, message))
    .next_deadline
}

// ---- A drain in any state ----

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

// ---- Construction ----

// A node refuses timings that could never keep a lease or elect a leader,
// except where it is a lone voter and needs neither.
#[test]
fn a_node_refuses_timings_it_could_not_run_an_election_on() {
    let ticks = Duration::from_ticks;
    let rows = [
        (
            "a zero roll call deadline",
            ElectionTimings {
                roll_call_deadline: ticks(0),
                ..timings(ticks(10))
            },
            Entry::Known(voter_of(1)),
            Some("roll_call_deadline"),
        ),
        (
            "a roll call deadline not shorter than the suspicion timeout",
            timings(ticks(10)).with_roll_call_deadline(ticks(10)),
            Entry::Known(voter_of(3)),
            Some("roll_call_deadline"),
        ),
        (
            "a zero heartbeat interval",
            ElectionTimings::new(ticks(10), ticks(0)).with_roll_call_deadline(ticks(1)),
            Entry::Known(voter_of(1)),
            Some("heartbeat_interval"),
        ),
        (
            "a zero clock drift divisor",
            ElectionTimings {
                clock_drift_divisor: 0,
                ..timings(ticks(10))
            },
            Entry::Known(voter_of(1)),
            Some("clock_drift_divisor"),
        ),
        (
            // A lease of 10 ticks less a fifth is 8 ticks: two heartbeat
            // intervals of 4 do not fit strictly inside it.
            "heartbeats that cannot keep a voter of several's lease",
            ElectionTimings {
                heartbeat_interval: ticks(4),
                clock_drift_divisor: 5,
                ..timings(ticks(10))
            },
            Entry::Known(voter_of(3)),
            Some("must be shorter than the lease length"),
        ),
        (
            // A joiner's shard already has a leader, whose lease its
            // heartbeats must keep.
            "heartbeats that cannot keep a joiner's lease",
            timings(ticks(0)),
            Entry::Joining(no_leader_yet()),
            Some("must be shorter than the lease length"),
        ),
        (
            "a lone voter's roll call deadline as long as its suspicion timeout",
            timings(ticks(10)).with_roll_call_deadline(ticks(10)),
            Entry::Known(voter_of(1)),
            None,
        ),
        (
            "a lone voter with no suspicion timeout, so no lease",
            timings(ticks(0)),
            Entry::Known(voter_of(1)),
            None,
        ),
    ];

    for (name, timings, entry, refused) in rows {
        let started = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            WorkerNode::start(
                Identity {
                    id: worker("w1"),
                    incarnation: IncarnationId::new("incarnation-1"),
                    shard: shard(SHARD),
                    timings,
                },
                entry,
                FakeClock::new(),
                None,
            )
        }));

        match (started, refused) {
            (Ok(_), None) => {}
            (Err(panic), Some(expected)) => {
                let message = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or_default();
                assert!(message.contains(expected), "{name}: {message}");
            }
            (Ok(_), Some(_)) => panic!("{name} was accepted"),
            (Err(_), None) => panic!("{name} was refused"),
        }
    }
}
