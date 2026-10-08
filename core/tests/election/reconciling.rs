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
    TestNode, deliver, grants, stand_as_candidate, state_changes, voter_node,
};
use kabudachi_core::election::{Input, Output};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::AckEcho;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::reconcile::Answered;
use kabudachi_core::time::{Clock, Duration, Instant};

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
/// some worker is reported silent; returns those reported, each with the
/// instant its runs' reconnect timeouts count from.
fn silent_while_hearing_from(
    node: &mut TestNode,
    clock: &FakeClock,
    from: &WorkerId,
) -> Vec<(WorkerId, Instant)> {
    let term = node.term();
    let mut silent = Vec::new();
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
        silent.extend(outputs.into_iter().filter_map(|output| match output {
            Output::WorkerSilence {
                worker,
                reconnect_from: Some(from),
            } => Some((worker, from)),
            _ => None,
        }));
        if !silent.is_empty() {
            break;
        }
    }
    silent
}

// A worker that answers the new leader's rebuild has been heard: the silence
// its leader reported for it ends, and if it stays quiet it is reported
// silent afresh, a suspicion timeout after its answer.
#[test]
fn a_worker_that_answered_the_rebuild_counts_as_heard_and_falls_silent_afresh() {
    let clock = FakeClock::new();
    let (mut node, _me, a, b) = reconciling_leader(&clock);
    let first = silent_while_hearing_from(&mut node, &clock, &a);
    assert_eq!(first.iter().map(|(worker, _)| worker).collect::<Vec<_>>(), [&b]);

    let answered_at = clock.now();
    let _ = node.step(Input::WatchWorkers {
        silent_holders: BTreeSet::new(),
        answered: BTreeSet::from([b.clone()]),
    });

    let again = silent_while_hearing_from(&mut node, &clock, &a);
    assert_eq!(
        again,
        [(b, answered_at + Duration::from_ticks(SUSPECT_TIMEOUT))],
        "silent afresh, counted from its answer"
    );
}

/// Heartbeats from `confirming`, echoing a fresh ack, and from `unconfirming`,
/// echoing none, then a tick, `Duration::from_ticks(5)` apart, at most
/// `rounds` times, until an output satisfies `until`; returns it, or `None`
/// if none did.
fn beat_until(
    node: &mut TestNode,
    clock: &FakeClock,
    confirming: &WorkerId,
    unconfirming: &WorkerId,
    rounds: usize,
    until: impl Fn(&Output) -> bool,
) -> Option<Output> {
    let term = node.term();
    for _ in 0..rounds {
        clock.advance(Duration::from_ticks(5));
        let echo = AckEcho {
            term,
            send_token: clock.now().as_ticks(),
        };
        let mut outputs = node
            .step(message_input(confirming, heartbeat_message(heartbeat(confirming, Some(echo)))))
            .outputs;
        outputs.extend(
            node.step(message_input(unconfirming, heartbeat_message(heartbeat(unconfirming, None))))
                .outputs,
        );
        outputs.extend(node.step(Input::Tick).outputs);
        if let Some(found) = outputs.into_iter().find(|output| until(output)) {
            return Some(found);
        }
    }
    None
}

// A member whose heartbeats arrive but which confirms no ack is lost to its
// leader. Its answer to the rebuild ends the silence reported for it, but it
// still confirms nothing, so it is reported silent again, within a suspicion
// timeout, rather than keeping its runs for good.
#[test]
fn a_lost_member_that_answered_the_rebuild_but_confirms_no_ack_is_reported_silent_again() {
    let clock = FakeClock::new();
    let (mut node, _me, a, b) = reconciling_leader(&clock);
    beat_until(&mut node, &clock, &a, &b, 10_000, |output| {
        *output == Output::WorkerLost(b.clone())
    })
    .expect("a member that confirms no ack is lost");
    let still_silent = beat_until(&mut node, &clock, &a, &b, 10, |output| {
        matches!(output, Output::WorkerSilence { worker, reconnect_from: None } if *worker == b)
    });
    assert_eq!(still_silent, None, "its heartbeats alone do not end its silence");

    let answered_at = clock.now();
    let silent_again = |output: &Output| {
        matches!(output, Output::WorkerSilence { worker, reconnect_from: Some(_) } if *worker == b)
    };
    let answered = node.step(Input::WatchWorkers {
        silent_holders: BTreeSet::new(),
        answered: BTreeSet::from([b.clone()]),
    });
    let silent = answered
        .outputs
        .into_iter()
        .find(silent_again)
        .or_else(|| beat_until(&mut node, &clock, &a, &b, 10_000, silent_again));
    assert!(silent.is_some(), "its silence is reported again");
    assert!(
        clock.now() <= answered_at + Duration::from_ticks(SUSPECT_TIMEOUT + 5),
        "reported silent at {:?}, not within a suspicion timeout of its answer at \
         {answered_at:?}",
        clock.now()
    );
}
