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
    TestNode, deliver, finish_reconciling, grants, stand_as_candidate, state_changes, voter_node,
};
use kabudachi_core::election::{Input, Output};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::AckEcho;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::reconcile::Answered;
use kabudachi_core::time::{Clock, Duration};

const SUSPECT_TIMEOUT: u64 = 10;

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
