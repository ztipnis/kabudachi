//! A winner holds office from its win, doing every election duty of a
//! leader, but its scheduler is given no grant until it has reconciled; it
//! can lose office meanwhile by every edge a leader can.

use std::collections::BTreeSet;

use crate::support::builders::{
    ack_message, configuration_of, heartbeat, heartbeat_message, leader_ack, message_input,
    vote_grant, vote_grant_message, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{
    TestNode, close_roll_call, deliver, finish_reconciling, grants, sent_to, stand_as_candidate,
    start_roll_call, state_changes, voter_node,
};
use kabudachi_core::election::{Input, Output};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::AckEcho;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::reconcile::{Answered, ReconcileTerm};
use kabudachi_core::time::{Clock, Duration, Instant};

const SUSPECT_TIMEOUT: u64 = 10;

/// Every abort deadline `outputs` reports, in order.
fn abort_deadlines(outputs: &[Output]) -> Vec<Option<Instant>> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::AbortDeadline(deadline) => Some(*deadline),
            _ => None,
        })
        .collect()
}

/// A node of three voters, last heard by a leader at tick 0, that has since
/// won term 2 and is reconciling. Returns it with the outputs of its win.
fn reconciling_winner_last_heard_long_ago(
    clock: &FakeClock,
) -> (TestNode, WorkerId, WorkerId, Vec<Output>) {
    let (me, a, b) = (worker("me"), worker("a"), worker("b"));
    let mut node = voter_node(clock, &me, 3, SUSPECT_TIMEOUT);
    let mut ack = leader_ack(&b, 1, &configuration_of(3), None);
    ack.heartbeat_token = Some(0);
    deliver(&mut node, &b, ack_message(ack));
    let call = stand_as_candidate(&mut node, clock, SUSPECT_TIMEOUT, &[a.clone(), b.clone()]);
    let won = deliver(
        &mut node,
        &a,
        vote_grant_message(vote_grant(me.clone(), a.clone(), call.term)),
    );
    assert_eq!(node.state(), WorkerState::LeaderReconciling);
    (node, a, b, won)
}

/// A node of three voters that has won term 1 and is reconciling, with the
/// two workers it won among.
fn reconciling_leader(clock: &FakeClock) -> (TestNode, WorkerId, WorkerId, WorkerId) {
    let (me, a, b) = (worker("me"), worker("a"), worker("b"));
    let mut node = voter_node(clock, &me, 3, SUSPECT_TIMEOUT);
    let call = stand_as_candidate(&mut node, clock, SUSPECT_TIMEOUT, &[a.clone(), b.clone()]);
    deliver(
        &mut node,
        &a,
        vote_grant_message(vote_grant(me.clone(), a.clone(), call.term)),
    );
    assert_eq!(node.state(), WorkerState::LeaderReconciling);
    (node, me, a, b)
}

#[test]
fn a_winner_reconciles_before_it_reports_a_grant() {
    let clock = FakeClock::new();
    let solo = worker("solo");
    let mut node = voter_node(&clock, &solo, 1, SUSPECT_TIMEOUT);
    start_roll_call(&mut node, &clock, SUSPECT_TIMEOUT);

    let won = close_roll_call(&mut node, &clock, SUSPECT_TIMEOUT);

    assert_eq!(
        state_changes(&won),
        vec![WorkerState::Candidate, WorkerState::LeaderReconciling]
    );
    let office = node.office_term().expect("it holds office");
    assert!(won.contains(&Output::Reconcile(office)));
    assert!(
        grants(&won).iter().all(Option::is_none),
        "no grant while reconciling: {won:?}"
    );
    assert_eq!(node.known_leader(), Some((solo.clone(), office.term)));

    let stale = node.step(Input::Reconciled(ReconcileTerm {
        term: office.term + 1,
        ..office
    }));
    assert!(
        stale.outputs.is_empty(),
        "another office's reconciliation changes nothing"
    );

    let led = finish_reconciling(&mut node);
    assert_eq!(state_changes(&led), vec![WorkerState::Leader]);
    assert!(matches!(grants(&led).as_slice(), [Some(grant)] if grant.term == office.term));
}

#[test]
fn a_reconciling_leader_acks_heartbeats_and_loses_its_lease_like_a_leader() {
    let clock = FakeClock::new();
    let (mut node, _me, a, _b) = reconciling_leader(&clock);
    let sent_at = clock.now();
    let call_term = node.term();

    let beat = heartbeat(
        &a,
        Some(AckEcho {
            term: call_term,
            send_token: sent_at.as_ticks(),
        }),
    );
    let acked = node.step(message_input(&a, heartbeat_message(beat)));

    assert_eq!(
        sent_to(&acked.outputs, &a).len(),
        1,
        "it answers heartbeats while reconciling"
    );
    assert!(
        grants(&acked.outputs).iter().all(Option::is_none),
        "the quorum's confirmation gives it a lease, not a grant: {acked:?}"
    );
    let lease_end = acked.next_deadline.expect("its lease has an end");
    clock.advance(lease_end - clock.now());
    let lost = node.step(Input::Tick).outputs;
    assert_eq!(
        state_changes(&lost),
        vec![WorkerState::NoQuorum],
        "its lease runs out like a leader's"
    );
    assert!(
        grants(&lost).contains(&None),
        "leaving office reports no grant"
    );
    assert_eq!(node.office_term(), None);
}

#[test]
fn a_reconciling_leader_steps_down_to_a_later_terms_leader() {
    let clock = FakeClock::new();
    let (mut node, _me, _a, b) = reconciling_leader(&clock);
    let office = node.office_term().expect("it holds office");

    let outpaced = deliver(
        &mut node,
        &b,
        ack_message(leader_ack(&b, office.term + 1, &configuration_of(3), None)),
    );

    assert_eq!(state_changes(&outpaced), vec![WorkerState::Active]);
    assert!(grants(&outpaced).contains(&None));
    assert_eq!(node.office_term(), None);
    assert!(
        node.step(Input::Reconciled(office)).outputs.is_empty(),
        "a reconciliation finished after the office ended changes nothing"
    );
}

#[test]
fn a_drain_asked_while_reconciling_leaves_office_without_ever_leading() {
    let clock = FakeClock::new();
    let solo = worker("solo");
    let mut node = voter_node(&clock, &solo, 1, SUSPECT_TIMEOUT);
    start_roll_call(&mut node, &clock, SUSPECT_TIMEOUT);
    close_roll_call(&mut node, &clock, SUSPECT_TIMEOUT);

    let drained = node.step(Input::Drain).outputs;

    assert_eq!(
        state_changes(&drained),
        vec![WorkerState::Draining, WorkerState::Stopped]
    );
    assert!(
        !grants(&drained).iter().any(Option::is_some),
        "it never held a grant"
    );
}

#[test]
fn a_reconciling_leader_asks_its_roster_and_counts_only_voters_answers() {
    let clock = FakeClock::new();
    let (node, me, a, b) = reconciling_leader(&clock);
    let stranger = worker("stranger");

    assert_eq!(
        node.reconcilees().into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([me.clone(), a.clone(), b.clone()])
    );
    assert!(node.is_member(&a));
    assert!(!node.is_member(&stranger));
    assert_eq!(
        node.voters_answered(&BTreeSet::from([me.clone()])),
        Answered::Short
    );
    assert_eq!(
        node.voters_answered(&BTreeSet::from([me.clone(), a.clone(), stranger.clone()])),
        Answered::Quorum,
        "a worker its roster does not hold counts for nothing"
    );
    assert_eq!(
        node.voters_answered(&BTreeSet::from([me, a, b])),
        Answered::All
    );
}

#[test]
fn a_reconciling_leader_with_a_confirmed_quorum_withdraws_its_stale_abort_deadline() {
    let clock = FakeClock::new();
    let (mut node, a, _b, mut outputs) = reconciling_winner_last_heard_long_ago(&clock);
    assert!(clock.now() - Instant::at(0) > Duration::from_ticks(SUSPECT_TIMEOUT));
    let term = node.term();

    let confirm = heartbeat(
        &a,
        Some(AckEcho {
            term,
            send_token: clock.now().as_ticks(),
        }),
    );
    outputs.extend(node.step(message_input(&a, heartbeat_message(confirm))).outputs);

    assert_eq!(
        abort_deadlines(&outputs).last(),
        Some(&None),
        "a quorum that heard it lifts its contact floor, so its runs are not aborted: {outputs:?}"
    );
}

#[test]
fn a_reconciling_leader_that_loses_office_raises_its_contact_floor_like_a_leader() {
    let clock = FakeClock::new();
    let (mut node, a, b, _won) = reconciling_winner_last_heard_long_ago(&clock);
    let term = node.term();
    let confirm = heartbeat(
        &a,
        Some(AckEcho {
            term,
            send_token: clock.now().as_ticks(),
        }),
    );
    let _ = node.step(message_input(&a, heartbeat_message(confirm)));
    let left_at = clock.now();

    let mut outputs = deliver(
        &mut node,
        &b,
        ack_message(leader_ack(&b, term + 1, &configuration_of(3), None)),
    );
    for _ in 0..40 {
        clock.advance(Duration::from_ticks(1));
        outputs.extend(node.step(Input::Tick).outputs);
    }

    let deadlines: Vec<Instant> = abort_deadlines(&outputs).into_iter().flatten().collect();
    assert!(!deadlines.is_empty(), "its contact goes stale again: {outputs:?}");
    assert!(
        deadlines.iter().all(|deadline| *deadline > left_at),
        "its floor rose to the end of its lease, not back to its follower days: {deadlines:?}"
    );
}

#[test]
fn a_reconciling_leader_still_reports_a_worker_it_stops_hearing_from_lost() {
    let clock = FakeClock::new();
    let (mut node, _me, a, b) = reconciling_leader(&clock);
    let term = node.term();
    let mut lost = Vec::new();

    for _ in 0..10_000 {
        clock.advance(Duration::from_ticks(5));
        let beat = heartbeat(
            &a,
            Some(AckEcho {
                term,
                send_token: clock.now().as_ticks(),
            }),
        );
        let mut outputs = node.step(message_input(&a, heartbeat_message(beat))).outputs;
        outputs.extend(node.step(Input::Tick).outputs);
        lost.extend(outputs.into_iter().filter_map(|output| match output {
            Output::WorkerLost(worker) => Some(worker),
            _ => None,
        }));
        if !lost.is_empty() {
            break;
        }
    }

    assert_eq!(lost, vec![b], "only the silent worker is lost");
    assert_eq!(node.state(), WorkerState::LeaderReconciling);
}

/// Heartbeats from `from` and ticks, `Duration::from_ticks(5)` apart, until
/// some worker is reported lost; returns those reported.
fn lost_while_hearing_from(
    node: &mut TestNode,
    clock: &FakeClock,
    from: &WorkerId,
) -> Vec<WorkerId> {
    let term = node.term();
    let mut lost = Vec::new();
    for _ in 0..10_000 {
        clock.advance(Duration::from_ticks(5));
        let beat = heartbeat(
            from,
            Some(AckEcho {
                term,
                send_token: clock.now().as_ticks(),
            }),
        );
        let mut outputs = node.step(message_input(from, heartbeat_message(beat))).outputs;
        outputs.extend(node.step(Input::Tick).outputs);
        lost.extend(outputs.into_iter().filter_map(|output| match output {
            Output::WorkerLost(worker) => Some(worker),
            _ => None,
        }));
        if !lost.is_empty() {
            break;
        }
    }
    lost
}

#[test]
fn a_worker_lost_while_reconciling_is_watched_again_once_the_leader_leads() {
    let clock = FakeClock::new();
    let (mut node, _me, a, b) = reconciling_leader(&clock);
    assert_eq!(lost_while_hearing_from(&mut node, &clock, &a), vec![b.clone()]);

    finish_reconciling(&mut node);

    assert_eq!(
        lost_while_hearing_from(&mut node, &clock, &a),
        vec![b],
        "the reconciliation may have heard from it before it died, so it is lost again while leading"
    );
}

#[test]
fn a_reconciling_leader_that_suspects_the_leader_it_lost_to_withdraws_its_grant() {
    let clock = FakeClock::new();
    let (mut node, _me, _a, b) = reconciling_leader(&clock);
    let office = node.office_term().expect("it holds office");

    let mut beat = heartbeat(&b, None);
    beat.term_seen = office.term + 1;
    let outputs = deliver(&mut node, &b, heartbeat_message(beat));

    assert_eq!(state_changes(&outputs), vec![WorkerState::LeaderSuspect]);
    assert_eq!(grants(&outputs), vec![None]);
    assert_eq!(node.office_term(), None);
}
