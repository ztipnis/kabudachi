//! A leader's half of leader liveness:
//! the ack it sends in answer to each follower heartbeat, the ack
//! confirmations those heartbeats echo back, and the quorum-contact lease it
//! computes from them, which decides when it goes `NoQuorum` and what
//! leadership grant it reports to its driver; and the workers it reports
//! lost once they fall silent, whose runs its scheduler
//! replays.
//!
//! Kept apart from `heartbeat.rs` because these tests need a real
//! leader of several voters, driven all the way to `Leader` through a roll
//! call and a vote. The lease counts only the members of the roster that
//! election built, each at its admission generation. The last tests run end
//! to end on the `Cluster` harness.

use crate::support::builders::checked;
use kabudachi_core::protocol::checked::{Checked, CheckedPayload};
use crate::support::builders::{
    message_input,
    founded_from_g0, g0, heartbeat, heartbeat_message, roll_call_reply, self_remove,
    self_remove_message, shard, timings, vote_grant, vote_grant_message, voter_of, worker,
};

use kabudachi_core::configuration::Generation;
use std::collections::BTreeSet;

use crate::support::clock::FakeClock;
use crate::support::harness::{Cluster, ClusterScheduler, StepRecord};
use crate::support::node::{
    close_roll_call, connect, deliver, grants, published_roll_calls, sent, sent_to,
    stand_as_candidate, start_roll_call, state_changes, tick,
};
use crate::support::scenarios::bootstrap_5_and_elect_leader;
use kabudachi_core::election::{
    ElectionTimings, Entry, Identity, Input, Output, Step, WorkerNode,
};
use kabudachi_core::protocol::ids::{IncarnationId, TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    AckEcho, ElectionMessage, LeaderHeartbeatAck, WorkerHeartbeat, election_message,
};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, Submission};
use kabudachi_core::time::{Clock, Duration, Instant};

const SHARD: &str = "shard-1";
const OTHER_SHARD: &str = "shard-2";
const SUSPECT_TIMEOUT_TICKS: u64 = 10;
/// The suspicion timeout less a tenth for clock drift.
const LEASE_TICKS: u64 = 9;

type TestNode = WorkerNode<FakeClock>;

/// A node that has just won term 1 of a configuration, connected to every
/// other voter, every one of which answered its roll call.
struct Won {
    node: TestNode,
    me: WorkerId,
    /// Every other member of its roster, those that voted for it first.
    others: Vec<WorkerId>,
    won_at: Instant,
    /// What the step that won it produced.
    outputs: Vec<Output>,
}

/// Drives a fresh node to a real `Leader` of a configuration of `size`
/// voters (at least 3) through a roll call every voter answers, and a vote
/// that the fewest of them needed grant.
fn leader_of(clock: &FakeClock, size: usize) -> Won {
    leader_with_timings(
        clock,
        size,
        timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)),
    )
}

/// `leader_of` for a node that runs `timings`.
fn leader_with_timings(clock: &FakeClock, size: usize, timings: ElectionTimings) -> Won {
    assert!(size >= 3, "leader_of needs a configuration of at least 3");
    let quorum = size / 2 + 1;
    let me = worker("leader");
    let others: Vec<WorkerId> = (0..size - 1)
        .map(|i| worker(&format!("peer-{i}")))
        .collect();

    let mut node = WorkerNode::start(
        Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings,
        },
        Entry::Known(voter_of(size)),
        clock.clone(),
        None,
    )
    .0;
    connect(&mut node, &others);

    stand_as_candidate(
        &mut node,
        clock,
        timings.suspect_timeout.as_ticks(),
        &others,
    );

    let mut outputs = Vec::new();
    for voter in &others[..quorum - 1] {
        outputs = deliver(
            &mut node,
            voter,
            vote_grant_message(vote_grant(me.clone(), voter.clone(), 1)),
        );
    }
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");

    Won {
        node,
        me,
        others,
        won_at: clock.now(),
        outputs,
    }
}

/// The only voter of its configuration, not yet elected.
fn lone_node(clock: &FakeClock) -> TestNode {
    let solo = worker("solo");
    WorkerNode::start(
        Identity {
            id: solo.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)),
        },
        Entry::Known(voter_of(1)),
        clock.clone(),
        None,
    )
    .0
}

/// Hands `node` `heartbeat` from `from` and returns the whole step.
fn receive(node: &mut TestNode, from: &WorkerId, heartbeat: WorkerHeartbeat) -> Step {
    node.step(message_input(&from, heartbeat_message(heartbeat)))
}

/// A heartbeat from `from` confirming the term-1 ack sent at `send_token`.
fn confirm(node: &mut TestNode, from: &WorkerId, send_token: Instant) -> Step {
    receive(
        node,
        from,
        heartbeat(
            from,
            Some(AckEcho {
                term: 1,
                send_token: send_token.as_ticks(),
            }),
        ),
    )
}

fn ticks_after(start: Instant, ticks: u64) -> Instant {
    start + Duration::from_ticks(ticks)
}

fn advance_to(clock: &FakeClock, instant: Instant) {
    clock.advance(instant - clock.now());
}

fn expect_heartbeat_ack(msg: ElectionMessage) -> Checked<LeaderHeartbeatAck> {
    match checked(msg).into_payload() {
        Some(CheckedPayload::HeartbeatAck(ack)) => ack,
        other => panic!("expected a LeaderHeartbeatAck payload, got {other:?}"),
    }
}

// ---- Acking heartbeats ----

#[test]
fn a_leader_acks_each_heartbeat_to_its_sender_only_with_its_send_instant() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let (sender, bystander) = (won.others[0].clone(), won.others[1].clone());
    clock.advance(Duration::from_ticks(3));

    let outputs = receive(&mut won.node, &sender, heartbeat(&sender, None)).outputs;

    assert_eq!(sent(&outputs).len(), 1, "{outputs:?}");
    let mut to_sender = sent_to(&outputs, &sender);
    let ack = expect_heartbeat_ack(to_sender.remove(0));
    assert_eq!(
        *ack,
        LeaderHeartbeatAck {
            shard_id: Some(shard(SHARD).into()),
            leader_id: Some(won.me.clone().into()),
            recovery_epoch: 0,
            term: 1,
            configuration: Some((&founded_from_g0(1, 3, 3)).into()),
            recipient_admission: Some(founded_from_g0(1, 3, 3).generation().into()),
            send_token: clock.now().as_ticks(),
            recipient_prior_admission: Some(g0().into()),
            heartbeat_token: None,
            // The lineage of its epoch, lineage 0 for a node built by `new`.
            recovery_epoch_lineage: Some(0),
        }
    );
    assert!(sent_to(&outputs, &bystander).is_empty());
}

#[test]
fn a_leader_echoes_the_heartbeat_it_answers_only_while_it_holds_a_grant() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let follower = won.others[0].clone();
    let echoed = |step: Step| {
        let mut to_follower = sent_to(&step.outputs, &follower);
        expect_heartbeat_ack(to_follower.remove(0)).heartbeat_token
    };
    clock.advance(Duration::from_ticks(2));

    // No quorum has confirmed an ack yet: a rival could still win.
    let unconfirmed = receive(
        &mut won.node,
        &follower,
        WorkerHeartbeat {
            send_token: 40,
            ..heartbeat(&follower, None)
        },
    );
    assert_eq!(echoed(unconfirmed), None);

    let confirmed = receive(
        &mut won.node,
        &follower,
        WorkerHeartbeat {
            send_token: 41,
            ..heartbeat(
                &follower,
                Some(AckEcho {
                    term: 1,
                    send_token: clock.now().as_ticks(),
                }),
            )
        },
    );
    assert_eq!(echoed(confirmed), Some(41));
}

#[test]
fn a_heartbeat_from_another_sender_shard_or_recovery_epoch_gets_no_ack() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let (sender, named) = (won.others[0].clone(), won.others[1].clone());

    let naming_another_worker = heartbeat(&named, None);
    let from_another_shard = WorkerHeartbeat {
        shard_id: Some(shard(OTHER_SHARD).into()),
        ..heartbeat(&sender, None)
    };
    let at_another_epoch = WorkerHeartbeat {
        recovery_epoch_seen: 1,
        ..heartbeat(&sender, None)
    };

    for rejected in [naming_another_worker, from_another_shard, at_another_epoch] {
        let outputs = receive(&mut won.node, &sender, rejected.clone()).outputs;
        assert!(outputs.is_empty(), "{rejected:?} got {outputs:?}");
    }
}

#[test]
fn a_node_that_does_not_lead_acks_no_heartbeat() {
    let clock = FakeClock::new();
    let (me, peer) = (worker("w1"), worker("w2"));
    let mut node: TestNode = WorkerNode::start(
        Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)),
        },
        Entry::Known(voter_of(2)),
        clock.clone(),
        None,
    )
    .0;

    let outputs = receive(&mut node, &peer, heartbeat(&peer, None)).outputs;

    assert!(outputs.is_empty(), "{outputs:?}");
}

#[test]
fn a_leader_announces_itself_to_its_connected_peers_once_on_winning() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);

    for other in &won.others {
        let acks: Vec<Checked<LeaderHeartbeatAck>> = sent_to(&won.outputs, other)
            .into_iter()
            .filter(|message| {
                matches!(
                    message.payload,
                    Some(election_message::Payload::HeartbeatAck(_))
                )
            })
            .map(expect_heartbeat_ack)
            .collect();
        assert_eq!(acks.len(), 1, "{other:?} must hear of its new leader once");
        assert_eq!(acks[0].send_token, won.won_at.as_ticks());
    }

    // After that it acks only in answer to heartbeats.
    for _ in 0..3 {
        clock.advance(timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)).heartbeat_interval);
        let outputs = tick(&mut won.node);
        assert!(sent(&outputs).is_empty(), "{outputs:?}");
    }
}

#[test]
fn a_leader_acks_a_roster_member_it_newly_connects_to_once() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let member = won.others[0].clone();
    let _ = won.node.step(Input::PeerDisconnected(member.clone()));
    clock.advance(Duration::from_ticks(3));

    let connected = won.node.step(Input::PeerConnected(member.clone())).outputs;
    let again = won.node.step(Input::PeerConnected(member.clone())).outputs;

    assert_eq!(sent(&connected).len(), 1, "{connected:?}");
    let mut to_member = sent_to(&connected, &member);
    let ack = expect_heartbeat_ack(to_member.remove(0));
    assert_eq!(ack.leader_id(), won.me);
    assert_eq!(ack.term, 1);
    assert_eq!(ack.send_token, clock.now().as_ticks());
    assert!(
        again.is_empty(),
        "a connection already reported changes nothing: {again:?}"
    );
}

#[test]
fn a_leader_acks_a_new_connection_to_a_worker_outside_its_roster_but_never_to_itself() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let outsider = worker("missed-the-roll-call");

    let to_outsider = won
        .node
        .step(Input::PeerConnected(outsider.clone()))
        .outputs;
    let to_itself = won.node.step(Input::PeerConnected(won.me.clone())).outputs;

    let mut acks = sent_to(&to_outsider, &outsider);
    assert_eq!(acks.len(), 1, "{to_outsider:?}");
    let ack = expect_heartbeat_ack(acks.remove(0));
    assert_eq!(ack.leader_id(), won.me);
    assert_eq!(
        ack.recipient_admission(),
        None,
        "a worker the roster does not hold is named no admission, so it keeps its own"
    );
    assert!(to_itself.is_empty(), "{to_itself:?}");
}

#[test]
fn a_node_that_does_not_lead_acks_no_new_connection() {
    let clock = FakeClock::new();
    let (me, peer) = (worker("w1"), worker("w2"));
    let mut active: TestNode = WorkerNode::start(
        Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)),
        },
        Entry::Known(voter_of(2)),
        clock.clone(),
        None,
    )
    .0;
    let from_active = active.step(Input::PeerConnected(peer)).outputs;
    assert!(from_active.is_empty(), "{from_active:?}");

    // A leader that has lost its quorum no longer leads.
    let mut won = leader_of(&clock, 3);
    let member = won.others[0].clone();
    let _ = won.node.step(Input::PeerDisconnected(member.clone()));
    clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS));
    let _ = tick(&mut won.node);
    assert_eq!(won.node.state(), WorkerState::NoQuorum, "setup invariant");
    let from_no_quorum = won.node.step(Input::PeerConnected(member)).outputs;
    assert!(from_no_quorum.is_empty(), "{from_no_quorum:?}");
}

// ---- The quorum-contact lease ----

#[test]
fn a_leader_goes_no_quorum_exactly_at_its_lease_end() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let follower = won.others[0].clone();
    let w = won.won_at;

    clock.advance(Duration::from_ticks(2));
    let _ = receive(&mut won.node, &follower, heartbeat(&follower, None));
    clock.advance(Duration::from_ticks(2));
    // The follower confirms the ack sent at w+2, the quorum-contact time.
    let confirmed = confirm(&mut won.node, &follower, ticks_after(w, 2));
    let lease_end = ticks_after(w, 2 + LEASE_TICKS);
    assert_eq!(confirmed.next_deadline, Some(lease_end));

    advance_to(&clock, ticks_after(w, 1 + LEASE_TICKS));
    let before = won.node.step(Input::Tick);
    assert!(state_changes(&before.outputs).is_empty(), "{before:?}");
    assert_eq!(before.next_deadline, Some(lease_end));

    clock.advance(Duration::from_ticks(1));
    let at = won.node.step(Input::Tick);
    assert_eq!(state_changes(&at.outputs), vec![WorkerState::NoQuorum]);
    advance_to(
        &clock,
        lease_end + Duration::from_ticks(SUSPECT_TIMEOUT_TICKS),
    );
    assert!(
        published_roll_calls(&tick(&mut won.node)).is_empty(),
        "from NoQuorum it retries only a suspicion timeout later"
    );
}

#[test]
fn the_drift_margin_rounds_up_so_the_lease_never_exceeds_nine_tenths() {
    // A tenth of 15 ticks is 1.5; the margin must be 2, not 1.
    let clock = FakeClock::new();
    let mut won = leader_with_timings(&clock, 3, timings(Duration::from_ticks(15)));
    let follower = won.others[0].clone();
    let w = won.won_at;

    assert_eq!(
        won.node.step(Input::Tick).next_deadline,
        Some(ticks_after(w, 13)),
        "the win instant stands in for the quorum contact"
    );
    clock.advance(Duration::from_ticks(1));
    let confirmed = confirm(&mut won.node, &follower, w);
    assert_eq!(confirmed.next_deadline, Some(ticks_after(w, 13)));
}

#[test]
fn a_smaller_drift_divisor_gives_up_a_larger_share_of_the_lease() {
    // A quarter of 15 ticks is 3.75, rounded up to 4: the lease is 11.
    let clock = FakeClock::new();
    let mut won = leader_with_timings(
        &clock,
        3,
        ElectionTimings {
            clock_drift_divisor: 4,
            ..timings(Duration::from_ticks(15))
        },
    );
    let follower = won.others[0].clone();
    let w = won.won_at;

    clock.advance(Duration::from_ticks(1));
    let confirmed = confirm(&mut won.node, &follower, w);
    assert_eq!(
        grants(&confirmed.outputs),
        vec![Some(term_1_grant(LeaseEnd::At(ticks_after(w, 11))))]
    );
    assert_eq!(confirmed.next_deadline, Some(ticks_after(w, 11)));
}

#[test]
fn before_a_majority_first_confirms_the_win_instant_stands_in_for_the_quorum_contact() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 5);
    let w = won.won_at;
    let stand_in_end = ticks_after(w, LEASE_TICKS);

    // Five members need two others' confirmations; one is not a majority.
    clock.advance(Duration::from_ticks(1));
    let one = confirm(&mut won.node, &won.others[0].clone(), w);
    assert_eq!(one.next_deadline, Some(stand_in_end));

    advance_to(&clock, ticks_after(w, LEASE_TICKS - 1));
    assert!(state_changes(&tick(&mut won.node)).is_empty());
    clock.advance(Duration::from_ticks(1));
    assert_eq!(
        state_changes(&tick(&mut won.node)),
        vec![WorkerState::NoQuorum]
    );
}

#[test]
fn confirmations_from_different_members_combine_into_the_quorum_contact() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 5);
    let w = won.won_at;
    let others = won.others.clone();

    clock.advance(Duration::from_ticks(5));
    let _ = confirm(&mut won.node, &others[0], ticks_after(w, 3));
    // Itself, plus acks sent at w+3 and w+5: the majority has heard from it
    // as late as w+3.
    let two = confirm(&mut won.node, &others[1], ticks_after(w, 5));
    assert_eq!(two.next_deadline, Some(ticks_after(w, 3 + LEASE_TICKS)));

    let three = confirm(&mut won.node, &others[2], ticks_after(w, 4));
    assert_eq!(three.next_deadline, Some(ticks_after(w, 4 + LEASE_TICKS)));

    let newer = confirm(&mut won.node, &others[0], ticks_after(w, 5));
    assert_eq!(newer.next_deadline, Some(ticks_after(w, 5 + LEASE_TICKS)));
}

#[test]
fn stale_future_and_pending_member_echoes_never_extend_the_lease() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let w = won.won_at;
    let follower = won.others[0].clone();
    let stand_in_end = Some(ticks_after(w, LEASE_TICKS));
    clock.advance(Duration::from_ticks(3));

    let stale_term = heartbeat(
        &follower,
        Some(AckEcho {
            term: 0,
            send_token: ticks_after(w, 3).as_ticks(),
        }),
    );
    assert_eq!(
        receive(&mut won.node, &follower, stale_term).next_deadline,
        stand_in_end
    );
    let from_the_future = confirm(&mut won.node, &follower, ticks_after(w, 4));
    assert_eq!(from_the_future.next_deadline, stand_in_end);
    let joiner = worker("joiner");
    let pending = confirm(&mut won.node, &joiner, ticks_after(w, 3));
    assert_eq!(pending.next_deadline, stand_in_end);

    // The same follower's real confirmation does extend it, and an older one
    // arriving late does not take the extension back.
    let real = confirm(&mut won.node, &follower, ticks_after(w, 3));
    assert_eq!(real.next_deadline, Some(ticks_after(w, 3 + LEASE_TICKS)));
    let late = confirm(&mut won.node, &follower, ticks_after(w, 1));
    assert_eq!(late.next_deadline, Some(ticks_after(w, 3 + LEASE_TICKS)));
}

#[test]
fn connections_neither_keep_nor_cost_a_leader_its_quorum() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let follower = won.others[0].clone();
    for other in won.others.clone() {
        let _ = won.node.step(Input::PeerDisconnected(other));
    }

    // Disconnected but still heartbeating: the leader keeps its quorum well
    // past its first lease.
    let interval = timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)).heartbeat_interval;
    let mut last_ack_sent_at = won.won_at;
    for _ in 0..10 {
        clock.advance(interval);
        let outputs = confirm(&mut won.node, &follower, last_ack_sent_at).outputs;
        assert!(state_changes(&outputs).is_empty(), "{outputs:?}");
        last_ack_sent_at = clock.now();
        let _ = tick(&mut won.node);
        assert_eq!(won.node.state(), WorkerState::Leader);
    }

    // Reconnected but silent: it loses its quorum at the lease end.
    for other in won.others.clone() {
        let _ = won.node.step(Input::PeerConnected(other));
    }
    clock.advance(Duration::from_ticks(LEASE_TICKS));
    assert_eq!(
        state_changes(&tick(&mut won.node)),
        vec![WorkerState::NoQuorum]
    );
}

#[test]
fn a_leader_alone_in_its_electorate_never_loses_its_quorum_and_has_no_deadline() {
    let clock = FakeClock::new();
    let mut node = lone_node(&clock);
    start_roll_call(&mut node, &clock, SUSPECT_TIMEOUT_TICKS);
    clock.advance(timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)).roll_call_deadline);
    let won = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");
    assert_eq!(won.next_deadline, None);

    clock.advance(Duration::from_ticks(100 * SUSPECT_TIMEOUT_TICKS));
    let later = node.step(Input::Tick);

    assert_eq!(node.state(), WorkerState::Leader);
    assert!(later.outputs.is_empty(), "{later:?}");
    assert_eq!(later.next_deadline, None);
}

// ---- The leadership grant ----

/// The grant for term 1 at recovery epoch 0, the term every leader here wins.
fn term_1_grant(valid_until: LeaseEnd) -> LeadershipGrant {
    LeadershipGrant {
        term: 1,
        recovery_epoch: kabudachi_core::coordination_authority::RecoveryEpoch::new(0, 0),
        valid_until,
    }
}

#[test]
fn a_lone_leader_is_granted_unbounded_leadership_on_winning() {
    let clock = FakeClock::new();
    let mut node = lone_node(&clock);
    start_roll_call(&mut node, &clock, SUSPECT_TIMEOUT_TICKS);

    let won = close_roll_call(&mut node, &clock, SUSPECT_TIMEOUT_TICKS);

    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");
    assert_eq!(grants(&won), vec![Some(term_1_grant(LeaseEnd::Unbounded))]);
}

#[test]
fn a_leader_of_several_is_granted_nothing_until_a_majority_confirms_then_its_lease() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let follower = won.others[0].clone();
    let w = won.won_at;
    assert!(grants(&won.outputs).is_empty(), "{:?}", won.outputs);

    clock.advance(Duration::from_ticks(2));
    let unconfirmed = receive(&mut won.node, &follower, heartbeat(&follower, None));
    assert!(grants(&unconfirmed.outputs).is_empty(), "{unconfirmed:?}");

    clock.advance(Duration::from_ticks(2));
    let confirmed = confirm(&mut won.node, &follower, ticks_after(w, 2));

    let lease_end = ticks_after(w, 2 + LEASE_TICKS);
    assert_eq!(
        grants(&confirmed.outputs),
        vec![Some(term_1_grant(LeaseEnd::At(lease_end)))]
    );
    assert_eq!(
        confirmed.next_deadline,
        Some(lease_end),
        "the grant ends where the leader goes NoQuorum"
    );
}

#[test]
fn each_confirmation_that_moves_the_lease_end_grants_the_new_lease_and_no_other_does() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let follower = won.others[0].clone();
    let w = won.won_at;
    clock.advance(Duration::from_ticks(1));
    let first = confirm(&mut won.node, &follower, w);
    let first_end = ticks_after(w, LEASE_TICKS);
    assert_eq!(
        grants(&first.outputs),
        vec![Some(term_1_grant(LeaseEnd::At(first_end)))]
    );
    clock.advance(Duration::from_ticks(2));

    let repeated = confirm(&mut won.node, &follower, w);
    let ticked = tick(&mut won.node);
    let newer = confirm(&mut won.node, &follower, ticks_after(w, 3));

    assert!(grants(&repeated.outputs).is_empty(), "{repeated:?}");
    assert!(grants(&ticked).is_empty(), "{ticked:?}");
    let newer_end = ticks_after(w, 3 + LEASE_TICKS);
    assert_eq!(
        grants(&newer.outputs),
        vec![Some(term_1_grant(LeaseEnd::At(newer_end)))]
    );
}

#[test]
fn losing_a_majority_confirmation_to_a_removal_withdraws_the_grant() {
    // Five members need two others' confirmations. Once the newer of the
    // two confirmers leaves, four members still need two, and only one
    // is left: the leader has no lease, though it is still `Leader`.
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 5);
    let (leaving, staying) = (won.others[0].clone(), won.others[1].clone());
    let w = won.won_at;
    clock.advance(Duration::from_ticks(3));
    let _ = confirm(&mut won.node, &leaving, ticks_after(w, 3));
    let held = confirm(&mut won.node, &staying, ticks_after(w, 1));
    let held_end = ticks_after(w, 1 + LEASE_TICKS);
    assert_eq!(
        grants(&held.outputs),
        vec![Some(term_1_grant(LeaseEnd::At(held_end)))],
        "setup invariant"
    );

    // The removal takes effect with the leader's next announcement (every
    // pending SELF_REMOVE lands in the next generation), here
    // the ack answering `staying`'s next heartbeat; until then `leaving`'s
    // confirmation still stands.
    let accepted = deliver(
        &mut won.node,
        &leaving,
        self_remove_message(self_remove(&leaving, SHARD)),
    );
    assert!(grants(&accepted).is_empty());
    let removed = confirm(&mut won.node, &staying, ticks_after(w, 1));

    assert_eq!(won.node.state(), WorkerState::Leader);
    assert_eq!(grants(&removed.outputs), vec![None]);
}

#[test]
fn losing_the_quorum_withdraws_the_grant_before_reporting_no_quorum() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let follower = won.others[0].clone();
    clock.advance(Duration::from_ticks(1));
    let _ = confirm(&mut won.node, &follower, won.won_at);
    advance_to(&clock, ticks_after(won.won_at, LEASE_TICKS));

    let lost = tick(&mut won.node);

    assert_eq!(
        lost,
        vec![
            Output::Grant(None),
            Output::StateChanged(WorkerState::NoQuorum)
        ]
    );
}

#[test]
fn draining_withdraws_the_grant_before_the_leader_announces_its_departure() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let follower = won.others[0].clone();
    clock.advance(Duration::from_ticks(1));
    let _ = confirm(&mut won.node, &follower, won.won_at);
    // A leader leaves only once every other voter has crawled its routing at
    // the admission it holds now: the first round's confirmations commit the
    // founding and re-admit them, the second round's crawls are counted.
    for other in won
        .others
        .iter()
        .chain(&won.others)
        .cloned()
        .collect::<Vec<_>>()
    {
        let mut beat = heartbeat(
            &other,
            Some(AckEcho {
                term: 1,
                send_token: won.won_at.as_ticks(),
            }),
        );
        beat.configuration_generation = won
            .node
            .configuration()
            .map(|configuration| configuration.generation().into());
        beat.routing_crawled = true;
        beat.crawl_admission = won
            .node
            .configuration()
            .map(|configuration| configuration.generation().into());
        let _ = receive(&mut won.node, &other, beat);
    }
    assert_eq!(won.node.state(), WorkerState::Leader, "setup invariant");

    let drained = won.node.step(Input::Drain).outputs;

    assert!(
        matches!(
            drained.as_slice(),
            [
                Output::Grant(None),
                Output::StateChanged(WorkerState::Draining),
                ..
            ]
        ),
        "{drained:?}"
    );
    assert_eq!(grants(&drained), vec![None]);
    assert!(!sent(&drained).is_empty(), "setup invariant: {drained:?}");
}

// ---- End to end ----

// A restarted member comes back, under a fresh WorkerId, knowing no leader;
// its old incarnation's connections close with the crash, the new one's open
// with the restart, and the leader acks it on its new connection.
#[test]
fn a_restarted_member_learns_its_leader_from_the_ack_on_reconnecting() {
    let tick_size = Duration::from_ticks(5);
    let (mut cluster, leader) =
        bootstrap_5_and_elect_leader(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS), tick_size);
    let member = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader)
        .expect("a 5-node cluster has followers");

    let member = cluster.restart_node(&member);
    assert_eq!(
        cluster.node(&member).known_leader(),
        None,
        "setup invariant"
    );

    // Well past its suspicion timeout, it still follows the leader.
    for _ in 0..6 {
        cluster.advance(tick_size);
        assert_eq!(cluster.states()[&member], WorkerState::Active);
    }
    assert_eq!(
        cluster.node(&member).known_leader().map(|(id, _)| id),
        Some(leader.clone())
    );
    assert_eq!(cluster.leader(), Some(leader));
}

// A member cut off while the others elected a leader missed the win
// announcement and knows no leader; once the partition heals, the leader
// acks it on the reopened connection, and it follows the leader from there.
#[test]
fn a_member_cut_off_when_its_leader_won_learns_of_it_once_healed() {
    let tick_size = Duration::from_ticks(5);
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(SUSPECT_TIMEOUT_TICKS));
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let cut_off = ids[2].clone();
    cluster.partition(
        ids[..2].iter().cloned().collect(),
        [cut_off.clone()].into_iter().collect(),
    );
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    cluster.run_until_quiescent(tick_size, 60);
    let leader = cluster
        .leader()
        .expect("the connected pair must elect a leader");
    assert!(
        matches!(
            cluster.states()[&cut_off],
            WorkerState::NoQuorum | WorkerState::RollCall
        ),
        "setup invariant: alone, it fails its roll calls and retries them"
    );

    cluster.heal();
    cluster.run_until_quiescent(tick_size, 60);

    assert_eq!(cluster.states()[&cut_off], WorkerState::Active);
    assert_eq!(
        cluster.node(&cut_off).known_leader().map(|(id, _)| id),
        Some(leader.clone())
    );
    // It keeps following: well past its suspicion timeout it is still Active.
    for _ in 0..6 {
        cluster.advance(tick_size);
        assert_eq!(cluster.states()[&cut_off], WorkerState::Active);
    }
    assert_eq!(cluster.leader(), Some(leader));
}

// A follower heartbeats its leader and the leader answers each heartbeat, so
// neither suspects the other, end to end through the `Cluster` harness.
#[test]
fn a_follower_and_its_leader_keep_each_other_live_through_heartbeats() {
    let suspect_timeout = Duration::from_ticks(SUSPECT_TIMEOUT_TICKS);
    let tick_size = Duration::from_ticks(5);
    let mut cluster = Cluster::bootstrap(2, suspect_timeout);

    // Converge to one Leader and one Active follower with Cluster::advance(),
    // bounded by a cap because run_until_quiescent's fixed point ignores
    // heartbeats and their acks.
    let mut converged = false;
    for _ in 0..30 {
        cluster.advance(tick_size);
        if cluster.leader().is_some() {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "expected a leader to emerge in a 2-node cluster within 30 advances"
    );

    let leader_id = cluster.leader().expect("checked above");
    let follower_id = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader_id)
        .expect("a 2-node cluster must have exactly one non-leader node");

    // Give the follower one more step to settle into Active.
    cluster.advance(tick_size);
    assert_eq!(
        cluster.states()[&follower_id],
        WorkerState::Active,
        "the follower must have returned to Active on its new leader's ack"
    );

    // Advance well past both the follower's suspicion timeout and the
    // leader's lease: only the heartbeat exchange keeps each side live.
    for _ in 0..20 {
        cluster.advance(tick_size);
        assert_eq!(
            cluster.states()[&follower_id],
            WorkerState::Active,
            "a follower whose heartbeats are answered must never become suspicious"
        );
        assert_eq!(
            cluster.leader(),
            Some(leader_id.clone()),
            "a leader whose acks are confirmed must keep its quorum"
        );
    }
}

#[test]
fn a_respondent_that_was_no_voter_of_the_roll_call_counts_toward_the_lease_on_the_new_side_only() {
    let clock = FakeClock::new();
    let (me, voter, left_out) = (worker("leader"), worker("peer-0"), worker("peer-1"));
    let mut node: TestNode = WorkerNode::start(
        Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)),
        },
        Entry::Known(voter_of(3)),
        clock.clone(),
        None,
    )
    .0;
    let call =
        published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT_TIMEOUT_TICKS)).remove(0);
    let outside = Generation::new(0, 0, 7);
    deliver(
        &mut node,
        &left_out,
        roll_call_reply(&me, call.term, &left_out, Some(outside)),
    );
    deliver(
        &mut node,
        &voter,
        roll_call_reply(&me, call.term, &voter, Some(g0())),
    );
    close_roll_call(&mut node, &clock, SUSPECT_TIMEOUT_TICKS);
    deliver(
        &mut node,
        &voter,
        vote_grant_message(vote_grant(me.clone(), voter.clone(), call.term)),
    );
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");
    let w = clock.now();
    clock.advance(Duration::from_ticks(3));

    let from_left_out = confirm(&mut node, &left_out, ticks_after(w, 3));
    assert_eq!(
        from_left_out.next_deadline,
        Some(ticks_after(w, LEASE_TICKS)),
        "two of the founded three, but one of the roll call's three: no lease yet"
    );
    let from_voter = confirm(&mut node, &voter, ticks_after(w, 3));

    assert_eq!(
        from_voter.next_deadline,
        Some(ticks_after(w, 3 + LEASE_TICKS)),
        "a majority of each side"
    );
}

#[test]
fn a_reconnect_timeout_in_the_timings_times_lost_workers() {
    const RECONNECT_TICKS: u64 = 7;
    let clock = FakeClock::new();
    let timings = timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS))
        .with_reconnect_timeout(Duration::from_ticks(RECONNECT_TICKS));
    let mut won = leader_with_timings(&clock, 3, timings);
    let (heard, silent) = (won.others[0].clone(), won.others[1].clone());
    let lost_at = ticks_after(won.won_at, SUSPECT_TIMEOUT_TICKS + RECONNECT_TICKS);
    let heartbeat_ticks = timings.heartbeat_interval.as_ticks();

    // One follower confirms every ack, so the lease holds; the other is
    // silent from the win on.
    let mut last_ack = won.won_at;
    let mut lost = Vec::new();
    while clock.now() < lost_at {
        clock.advance(Duration::from_ticks(1));
        let outputs = if (clock.now() - won.won_at).as_ticks() % heartbeat_ticks == 0 {
            let step = confirm(&mut won.node, &heard, last_ack);
            last_ack = clock.now();
            step.outputs
        } else {
            tick(&mut won.node)
        };
        if outputs.contains(&Output::WorkerLost(silent.clone())) {
            lost.push(clock.now());
        }
    }

    assert_eq!(
        lost,
        vec![lost_at],
        "lost a suspicion timeout and the configured reconnect timeout after it was last heard"
    );
}

#[test]
fn a_follower_silent_past_suspicion_and_reconnect_timeouts_is_lost_and_its_runs_replayed() {
    // Seconds rather than ticks, so the reconnect timeout's 30 s is a few
    // dozen heartbeats rather than thousands.
    let suspect_timeout = Duration::from_secs(2);
    let (mut cluster, leader) =
        bootstrap_5_and_elect_leader(suspect_timeout, Duration::from_secs(1));
    let followers: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    let (cut_off, healthy) = (followers[0].clone(), followers[1].clone());
    let scheduler = cluster.scheduler_mut(&leader);
    let submit = |scheduler: &mut ClusterScheduler, payload: &str| {
        scheduler
            .submit(Submission::new(
                TaskDefinitionId::new("billing.charge"),
                0,
                payload.as_bytes().to_vec(),
                "default",
            ))
            .expect("the leader's scheduler leads")
    };
    let claim = |scheduler: &mut ClusterScheduler, worker: &WorkerId, task: &TaskId| {
        let claim = scheduler
            .request_claim(worker, task)
            .expect("the task is queued");
        scheduler
            .report_started(worker, &claim.task_run_id)
            .expect("the claim is fresh");
        claim.task_run_id
    };
    let cut_off_task = submit(scheduler, "cut-off");
    let cut_off_run = claim(scheduler, &cut_off, &cut_off_task);
    let healthy_task = submit(scheduler, "healthy");
    let healthy_run = claim(scheduler, &healthy, &healthy_task);

    let rest: BTreeSet<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != cut_off)
        .collect();
    cluster.record_steps();
    cluster.partition(rest, [cut_off.clone()].into_iter().collect());
    let lost_after = suspect_timeout.as_ticks() + ElectionTimings::DEFAULT_RECONNECT_TIMEOUT.as_ticks();
    let run_state = |cluster: &mut Cluster, run| {
        cluster
            .scheduler_mut(&leader)
            .task_run(run)
            .expect("the leader's scheduler holds the run")
            .current_state()
    };

    // The follower was last heard at most a heartbeat interval before the cut.
    cluster.advance(Duration::from_ticks(lost_after - 600));
    assert_eq!(cluster.states()[&leader], WorkerState::Leader);
    assert_eq!(
        run_state(&mut cluster, &cut_off_run),
        TaskRunState::Running,
        "not lost before a suspicion timeout and a reconnect timeout of silence"
    );

    cluster.advance(Duration::from_ticks(1_200));
    assert_eq!(run_state(&mut cluster, &cut_off_run), TaskRunState::Lost);
    assert_eq!(
        run_state(&mut cluster, &healthy_run),
        TaskRunState::Running,
        "a follower that keeps heartbeating is never lost"
    );
    let spy = cluster.scheduler_spy(&leader);
    assert_eq!(
        spy.pending(),
        1,
        "the lost run's task is queued again for its replay"
    );
    assert_eq!(
        spy.state_of(&cut_off_task),
        TaskRunState::Queued,
        "the lost run's task is queued again for its replay"
    );

    // The cut-off worker aborts its runs before the leader
    // replays them, and a worker that keeps hearing its leader is never told
    // to abort.
    let steps = cluster.take_steps();
    let lost_at = reported_lost_at(&steps, &leader, &cut_off);
    assert_aborts_by(&steps, &cut_off, lost_at);
    assert!(
        steps
            .iter()
            .filter(|step| step.node == healthy)
            .flat_map(|step| &step.outputs)
            .all(|output| !matches!(output, Output::AbortDeadline(Some(_)))),
        "a follower that hears its leader has nothing to abort"
    );

    // Heard by its leader again, it withdraws the abort.
    cluster.heal();
    cluster.advance(suspect_timeout);
    let steps = cluster.take_steps();
    assert_eq!(
        abort_deadline_at(&steps, &cut_off, cluster.now()),
        Some(None),
        "a worker its leader hears from again withdraws its deadline and keeps its runs"
    );
}

#[test]
fn a_follower_cut_off_as_a_new_leader_takes_over_aborts_before_that_leader_replays_its_runs() {
    let suspect_timeout = Duration::from_secs(2);
    let (mut cluster, old_leader) =
        bootstrap_5_and_elect_leader(suspect_timeout, Duration::from_secs(1));
    let rest: BTreeSet<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != old_leader)
        .collect();
    cluster.record_steps();
    cluster.partition([old_leader.clone()].into_iter().collect(), rest.clone());
    let new_leader = loop {
        cluster.advance(Duration::from_ticks(10));
        if let Some(leader) = rest
            .iter()
            .find(|id| cluster.states()[*id] == WorkerState::Leader)
        {
            break leader.clone();
        }
    };

    // Cut off with the old leader, before or just after it hears the new one.
    let cut_off = rest
        .iter()
        .find(|id| **id != new_leader)
        .expect("four survivors")
        .clone();
    cluster.partition(
        [old_leader.clone(), cut_off.clone()].into_iter().collect(),
        rest.into_iter().filter(|id| *id != cut_off).collect(),
    );
    let lost_after = suspect_timeout.as_ticks() + ElectionTimings::DEFAULT_RECONNECT_TIMEOUT.as_ticks();
    cluster.advance(Duration::from_ticks(lost_after + 1_000));

    let steps = cluster.take_steps();
    let lost_at = reported_lost_at(&steps, &new_leader, &cut_off);
    assert_aborts_by(&steps, &cut_off, lost_at);
    // The old leader aborts its own runs before the new leader could replay
    // them, which is no sooner than a suspicion and a reconnect timeout
    // after it won (it does not track the old leader here, having won
    // without its answer): no rival won before the old grant ended.
    let won_at = steps
        .iter()
        .find(|step| {
            step.node == new_leader
                && step
                    .outputs
                    .contains(&Output::StateChanged(WorkerState::Leader))
        })
        .map(|step| step.at)
        .expect("the new leader's win was recorded");
    assert_aborts_by(
        &steps,
        &old_leader,
        won_at + Duration::from_ticks(lost_after),
    );
}

/// When `leader` first reported `worker` lost among `steps`.
fn reported_lost_at(steps: &[StepRecord], leader: &WorkerId, worker: &WorkerId) -> Instant {
    steps
        .iter()
        .find(|step| {
            step.node == *leader
                && step
                    .outputs
                    .iter()
                    .any(|output| *output == Output::WorkerLost(worker.clone()))
        })
        .map(|step| step.at)
        .unwrap_or_else(|| panic!("{leader:?} reported {worker:?} lost"))
}

/// The abort deadline `worker` last reported among `steps` taken no later
/// than `at`, `Some(None)` for a withdrawal; `None` if it reported none.
fn abort_deadline_at(
    steps: &[StepRecord],
    worker: &WorkerId,
    at: Instant,
) -> Option<Option<Instant>> {
    steps
        .iter()
        .filter(|step| step.node == *worker && step.at <= at)
        .flat_map(|step| &step.outputs)
        .filter_map(|output| match output {
            Output::AbortDeadline(deadline) => Some(*deadline),
            _ => None,
        })
        .next_back()
}

/// Asserts that by `replayed_at`, when a leader replays `worker`'s runs,
/// `worker` has been told to abort them no later than that.
fn assert_aborts_by(steps: &[StepRecord], worker: &WorkerId, replayed_at: Instant) {
    let deadline = abort_deadline_at(steps, worker, replayed_at).flatten();
    assert!(
        deadline.is_some_and(|by| by < replayed_at),
        "{worker:?} must abort before its runs are replayed at {replayed_at:?}, but its \
         deadline was {deadline:?}"
    );
}
