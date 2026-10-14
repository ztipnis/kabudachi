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
    commit_founding, connect, deliver, elect, finish_reconciling, known_node,
    leader_of_three as committed_leader, stand_as_candidate, start_roll_call, state_changes, tick,
};
use kabudachi_core::election::{
    ElectionTimings, Entry, HandOffTo, Identity, Input, Output, WorkerNode,
};
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

    // A leader handing its leadership to a voter that never stands, its
    // voters confirming every deadline: a tick at the hand-over's end moves
    // it on, so it grants again exactly a suspicion timeout after it began.
    let mut handing_over = committed_leader(&clock, SUSPECT_TIMEOUT);
    let (p1, p2) = (worker("p1"), worker("p2"));
    let handed_at = clock.now();
    let handed = handing_over.step(Input::TransferLeadership);
    assert!(
        handed
            .outputs
            .iter()
            .any(|output| matches!(output, Output::TransferLeadership { .. })),
        "setup invariant"
    );
    let mut deadline = handed.next_deadline;
    let mut led_again_at = None;
    for _ in 0..50 {
        let Some(due) = deadline else { break };
        // The voters heartbeat every few ticks, keeping the leader's lease
        // while it hands over; the tick comes at its deadline.
        let at = due.min(clock.now() + Duration::from_ticks(SUSPECT_TIMEOUT / 4));
        clock.advance(at - clock.now());
        for voter in [&p1, &p2] {
            deadline = confirm(&mut handing_over, &clock, voter);
        }
        if deadline.is_some_and(|due| due > clock.now()) {
            continue;
        }
        let ticked = handing_over.step(Input::Tick);
        if ticked
            .outputs
            .iter()
            .any(|output| matches!(output, Output::Grant(Some(_))))
        {
            led_again_at = Some(clock.now());
            break;
        }
        deadline = ticked.next_deadline;
    }
    assert_eq!(
        led_again_at,
        Some(handed_at + Duration::from_ticks(SUSPECT_TIMEOUT)),
        "it leads again when the hand-over lapses"
    );
    assert_eq!(handing_over.state(), WorkerState::Leader);

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

    // A drain asked while a node waits for a leader: a tick at each deadline
    // it reports leaves it stopped, without a leader, exactly when its drain
    // wait ends.
    for waiting_in in [
        WorkerState::LeaderSuspect,
        WorkerState::RollCall,
        WorkerState::NoQuorum,
    ] {
        assert_kept_drain_ends_at_the_wait_limit(waiting_in);
    }
    assert_hand_over_ends_at_the_wait_limit();

    // A drain wait shorter than the suspicion timeout, asked of a leader
    // before or during a hand-over: its wait cannot end a tick the hand-over
    // under way does not, so the node does not report the wait's end while
    // that hand-over lasts.
    assert_drain_in_office_leaves_at(DrainAskedAt::Start, SUSPECT_TIMEOUT + SUSPECT_TIMEOUT / 2 - 1);
    assert_drain_in_office_leaves_at(DrainAskedAt::DuringAHandOver, 2 * SUSPECT_TIMEOUT);
}

/// When a leader is asked to drain.
#[derive(Clone, Copy)]
enum DrainAskedAt {
    /// Before any hand-over: it begins its own at the end of its wait.
    Start,
    /// Just after it began one of its own accord.
    DuringAHandOver,
}

/// A leader whose drain wait is shorter than its suspicion timeout, asked to
/// drain as `asked` says, its voters confirming it throughout: each `Tick` at
/// a deadline it reports moves it on, and it leaves alone, with no successor
/// taking up the hand-over, `leaves_after` ticks after the request.
fn assert_drain_in_office_leaves_at(asked: DrainAskedAt, leaves_after: u64) {
    let clock = FakeClock::new();
    let wait_limit = Duration::from_ticks(SUSPECT_TIMEOUT / 2 - 1);
    let mut leader = known_node(
        &clock,
        &worker("w1"),
        voter_of(3),
        ElectionTimings {
            drain_wait_limit: wait_limit,
            ..timings(Duration::from_ticks(SUSPECT_TIMEOUT))
        },
    );
    let (p1, p2) = (worker("p1"), worker("p2"));
    connect(&mut leader, &[p1.clone(), p2.clone()]);
    elect(&mut leader, &clock, SUSPECT_TIMEOUT, &[p1.clone(), p2.clone()]);
    commit_founding(&mut leader, &clock, &[p1.clone(), p2.clone()]);

    let asked_at = clock.now();
    if let DrainAskedAt::DuringAHandOver = asked {
        let _ = leader.step(Input::TransferLeadership);
    }
    let mut deadline = leader.step(Input::Drain).next_deadline;
    let mut outputs = Vec::new();
    for _ in 0..500 {
        if leader.state() == WorkerState::Stopped {
            break;
        }
        let due = deadline.expect("a node waiting to leave reports a deadline");
        // The voters heartbeat every few ticks, keeping the leader's lease.
        let at = due.min(clock.now() + Duration::from_ticks(SUSPECT_TIMEOUT / 4));
        clock.advance(at - clock.now());
        for voter in [&p1, &p2] {
            deadline = confirm(&mut leader, &clock, voter);
        }
        if deadline.is_some_and(|due| due > clock.now()) {
            continue;
        }
        let before = leader.state();
        let ticked = leader.step(Input::Tick);
        assert!(
            leader.state() != before
                || ticked
                    .next_deadline
                    .is_none_or(|next| next > clock.now()),
            "a Tick at its deadline {:?} left a {before:?} node due again at {:?}",
            clock.now(),
            ticked.next_deadline
        );
        outputs.extend(ticked.outputs);
        deadline = ticked.next_deadline;
    }

    assert_eq!(leader.state(), WorkerState::Stopped, "it never left");
    assert_eq!(clock.now(), asked_at + Duration::from_ticks(leaves_after));
    assert!(
        outputs
            .iter()
            .any(|output| matches!(output, Output::HandOff(HandOffTo::Voters(_)))),
        "{outputs:?}"
    );
}

/// A voter with no leader, in `waiting_in`, asked to drain: each `Tick` at a
/// deadline it reports comes no later than its drain wait's end, and the one
/// at that end stops it, handing its records to nobody.
fn assert_kept_drain_ends_at_the_wait_limit(waiting_in: WorkerState) {
    let clock = FakeClock::new();
    let (w1, w2) = (worker("w1"), worker("w2"));
    let mut voter = node(&clock, &w1, &[w1.clone(), w2.clone()]);
    let mut deadline = voter.step(Input::PeerConnected(w2)).next_deadline;
    while voter.state() != waiting_in {
        let due = deadline.expect("a voter with no leader reports a deadline");
        clock.advance(due - clock.now());
        deadline = voter.step(Input::Tick).next_deadline;
    }

    let leaves_at = clock.now() + timings(Duration::from_ticks(SUSPECT_TIMEOUT)).drain_wait_limit;
    let asked = voter.step(Input::Drain);
    assert_eq!(voter.state(), waiting_in, "the drain is kept");
    let mut deadline = asked.next_deadline;
    let mut outputs = Vec::new();
    for _ in 0..500 {
        if voter.state() == WorkerState::Stopped {
            break;
        }
        let due = deadline.expect("a node waiting to leave reports a deadline");
        assert!(
            due <= leaves_at,
            "a {:?} node reports {due:?}, beyond its drain wait's end {leaves_at:?}",
            voter.state()
        );
        clock.advance(due - clock.now());
        let ticked = voter.step(Input::Tick);
        outputs.extend(ticked.outputs);
        deadline = ticked.next_deadline;
    }

    assert_eq!(voter.state(), WorkerState::Stopped, "{waiting_in:?} never left");
    assert_eq!(clock.now(), leaves_at, "left from {waiting_in:?}");
    assert!(
        outputs.contains(&Output::HandOff(HandOffTo::Nobody)),
        "{waiting_in:?}: {outputs:?}"
    );
}

/// A leader that handed its leadership over to a successor that acks it
/// without ever vouching for hearing it: it follows (`Active`), and the
/// `Tick` at each deadline it reports comes no later than its hand-over's
/// end, where it leaves through the successor.
fn assert_hand_over_ends_at_the_wait_limit() {
    let clock = FakeClock::new();
    let mut leader = committed_leader(&clock, SUSPECT_TIMEOUT);
    let (p1, p2) = (worker("p1"), worker("p2"));
    let limit = timings(Duration::from_ticks(SUSPECT_TIMEOUT)).drain_wait_limit;
    let beat_every = Duration::from_ticks(SUSPECT_TIMEOUT / 4);

    // Its voters keep its lease through the wait to hand over; at the wait's
    // end it hands over.
    let hands_over_at = clock.now() + limit;
    let _ = leader.step(Input::Drain);
    let mut handed = Vec::new();
    for _ in 0..500 {
        if !handed.is_empty() {
            break;
        }
        clock.advance(beat_every.min(hands_over_at - clock.now()));
        // `p1` has crawled the routing table since its admission, so it can
        // take the leadership; `p2` has not.
        let mut outputs = Vec::new();
        for voter in [&p1, &p2] {
            let mut beat = heartbeat(
                voter,
                Some(AckEcho {
                    term: leader.term(),
                    send_token: clock.now().as_ticks(),
                }),
            );
            let held = leader.configuration().map(|configuration| configuration.generation());
            beat.configuration_generation = held.map(Into::into);
            if *voter == p1 {
                beat.routing_crawled = true;
                beat.admission_generation = held.map(Into::into);
            }
            outputs.extend(
                leader
                    .step(message_input(voter, heartbeat_message(beat)))
                    .outputs,
            );
        }
        outputs.extend(leader.step(Input::Tick).outputs);
        handed = outputs
            .into_iter()
            .filter_map(|output| match output {
                Output::TransferLeadership { to } => Some(to),
                _ => None,
            })
            .collect();
    }
    assert!(!handed.is_empty(), "it never handed over");
    let successor = handed.remove(0);
    assert_eq!(clock.now(), hands_over_at, "setup invariant");

    let leaves_at = clock.now() + limit;
    let configuration = leader.configuration().cloned().expect("a configuration");
    let successors_term = leader.term() + 1;
    let ack = || {
        ack_message(leader_ack(
            &successor,
            successors_term,
            &configuration,
            Some(g0()),
        ))
    };
    let mut deadline = deliver_step(&mut leader, &successor, ack());
    assert_eq!(leader.state(), WorkerState::Active, "it follows its successor");
    let mut outputs = Vec::new();
    for _ in 0..500 {
        if leader.state() == WorkerState::Stopped {
            break;
        }
        let due = deadline.expect("a node waiting to leave reports a deadline");
        assert!(
            due <= leaves_at,
            "an {:?} node reports {due:?}, beyond its hand-over's end {leaves_at:?}",
            leader.state()
        );
        clock.advance(due - clock.now());
        let _ = deliver_step(&mut leader, &successor, ack());
        let ticked = leader.step(Input::Tick);
        outputs.extend(ticked.outputs);
        deadline = ticked.next_deadline;
    }

    assert_eq!(leader.state(), WorkerState::Stopped, "it never left");
    assert_eq!(clock.now(), leaves_at);
    assert!(
        outputs.contains(&Output::HandOff(HandOffTo::Leader(successor))),
        "{outputs:?}"
    );
}

/// `voter`'s heartbeat confirming the ack `leader` sent it at this instant.
fn confirm(leader: &mut TestNode, clock: &FakeClock, voter: &WorkerId) -> Option<Instant> {
    let mut beat = heartbeat(
        voter,
        Some(AckEcho {
            term: leader.term(),
            send_token: clock.now().as_ticks(),
        }),
    );
    beat.configuration_generation = leader
        .configuration()
        .map(|configuration| configuration.generation().into());
    deliver_step(leader, voter, heartbeat_message(beat))
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
