//! Step-down (ADR-0001 decision 14): a node that holds or contests a term
//! gives it up once it has seen a later one (from a vote it grants, an ack,
//! a refusal or an election certificate), to follow the leader of that
//! term if an ack from it is what told it, and otherwise to suspect its
//! leader again.

use crate::support::builders::{
    ack_message, configuration_of, election_certificate, election_certificate_message,
    election_reject, founded_from_g0, g0, leader_ack, past_any_suspicion, roll_call,
    roll_call_message, timings, vote_request, vote_request_message, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{
    TestNode, deliver, elect, grants, published_roll_calls, rejects_sent_to, sent_to,
    stand_as_candidate, start_roll_call, state_changes, voter_node,
};
use kabudachi_core::configuration::{Admission, Generation};
use kabudachi_core::election::Input;
use kabudachi_core::protocol::messages::ElectionRejectReason;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Clock, Duration};

/// Every node here suspects its leader after this many ticks.
const SUSPECT: u64 = 10;

/// `w1`, elected leader of a configuration of 3 in term 1 with `p1` and
/// `p2`.
fn leader_of_three(clock: &FakeClock) -> TestNode {
    let mut node = voter_node(clock, &worker("w1"), 3, SUSPECT);
    elect(&mut node, clock, SUSPECT, &[worker("p1"), worker("p2")]);
    assert_eq!(node.term(), 1, "setup invariant");
    node
}

/// `w1`, standing as the candidate for term 1 of a configuration of 3.
fn candidate_of_three(clock: &FakeClock) -> TestNode {
    let mut node = voter_node(clock, &worker("w1"), 3, SUSPECT);
    stand_as_candidate(&mut node, clock, SUSPECT, &[worker("p1")]);
    node
}

/// `w1`, a voter of 5, in `RollCall` with its own call for term 1.
fn initiator_of_five(clock: &FakeClock) -> TestNode {
    let mut node = voter_node(clock, &worker("w1"), 5, SUSPECT);
    start_roll_call(&mut node, clock, SUSPECT);
    assert_eq!(node.state(), WorkerState::RollCall, "setup invariant");
    node
}

/// An ack from `leader-2`, elected in `term`.
fn ack_from_new_leader(term: u64) -> kabudachi_core::protocol::messages::ElectionMessage {
    ack_message(leader_ack(
        &worker("leader-2"),
        term,
        &configuration_of(3),
        Some(g0()),
    ))
}

/// A refusal to `w1` of its call or request for term 1, by `p1`, which has
/// seen `highest_term_seen`.
fn refusal_naming(highest_term_seen: u64) -> kabudachi_core::protocol::messages::ElectionMessage {
    election_reject(
        &worker("w1"),
        1,
        &worker("p1"),
        ElectionRejectReason::StaleTerm,
        highest_term_seen,
        None,
    )
}

// ---- To Active, on an ack from a later term's leader ----

#[test]
fn a_leader_acked_by_a_later_terms_leader_follows_it_and_withdraws_its_grant_first() {
    let clock = FakeClock::new();
    let mut node = leader_of_three(&clock);

    let outputs = deliver(&mut node, &worker("leader-2"), ack_from_new_leader(2));

    assert_eq!(state_changes(&outputs), vec![WorkerState::Active]);
    assert_eq!(grants(&outputs), vec![None]);
    let grant_withdrawn = outputs
        .iter()
        .position(|output| matches!(output, kabudachi_core::election::Output::Grant(None)));
    let stepped_down = outputs.iter().position(|output| {
        matches!(
            output,
            kabudachi_core::election::Output::StateChanged(WorkerState::Active)
        )
    });
    assert!(grant_withdrawn < stepped_down, "{outputs:?}");
    assert_eq!(node.known_leader(), Some((worker("leader-2"), 2)));
}

#[test]
fn a_candidate_acked_by_a_later_terms_leader_follows_it() {
    let clock = FakeClock::new();
    let mut node = candidate_of_three(&clock);

    let outputs = deliver(&mut node, &worker("leader-2"), ack_from_new_leader(2));

    assert_eq!(state_changes(&outputs), vec![WorkerState::Active]);
    assert_eq!(node.known_leader(), Some((worker("leader-2"), 2)));
}

#[test]
fn an_ack_from_a_leader_of_the_same_term_moves_no_candidate() {
    let clock = FakeClock::new();
    let mut node = candidate_of_three(&clock);

    let outputs = deliver(&mut node, &worker("leader-2"), ack_from_new_leader(1));

    assert!(state_changes(&outputs).is_empty(), "{outputs:?}");
    assert_eq!(node.state(), WorkerState::Candidate);
}

// ---- To LeaderSuspect, on anything else ----

#[test]
fn a_leader_refused_naming_a_later_term_steps_down_to_leader_suspect() {
    let clock = FakeClock::new();
    let mut node = leader_of_three(&clock);

    let outputs = deliver(&mut node, &worker("p1"), refusal_naming(3));

    assert_eq!(grants(&outputs), vec![None]);
    assert_eq!(state_changes(&outputs), vec![WorkerState::LeaderSuspect]);
    assert_eq!(node.known_leader(), None);
}

#[test]
fn a_candidate_certified_a_later_terms_win_steps_down_to_leader_suspect() {
    let clock = FakeClock::new();
    let mut node = candidate_of_three(&clock);
    let winner = worker("leader-2");

    let outputs = deliver(
        &mut node,
        &winner,
        election_certificate_message(election_certificate(
            &winner,
            2,
            &founded_from_g0(2, 3, 3),
            Admission::from(Some(Generation::new(0, 2, 1))),
        )),
    );

    assert_eq!(state_changes(&outputs), vec![WorkerState::LeaderSuspect]);
}

#[test]
fn a_refusal_naming_the_contested_term_itself_deposes_no_one() {
    let clock = FakeClock::new();
    let mut candidate = candidate_of_three(&clock);
    let mut initiator = initiator_of_five(&clock);

    let to_candidate = deliver(&mut candidate, &worker("p1"), refusal_naming(1));
    let to_initiator = deliver(&mut initiator, &worker("p1"), refusal_naming(1));

    assert!(state_changes(&to_candidate).is_empty());
    assert!(state_changes(&to_initiator).is_empty());
}

#[test]
fn an_initiator_that_grants_a_vote_in_a_later_term_steps_down_to_leader_suspect() {
    let clock = FakeClock::new();
    let mut node = initiator_of_five(&clock);
    let rival = worker("w0");
    deliver(
        &mut node,
        &rival,
        roll_call_message(roll_call(&rival, 2, &configuration_of(5), 0)),
    );

    let outputs = deliver(
        &mut node,
        &rival,
        vote_request_message(vote_request(rival.clone(), 0, 2)),
    );

    assert_eq!(state_changes(&outputs), vec![WorkerState::LeaderSuspect]);
}

#[test]
fn a_roll_call_for_a_later_term_deposes_no_leader() {
    let clock = FakeClock::new();
    let mut node = leader_of_three(&clock);
    let rival = worker("w0");

    let outputs = deliver(
        &mut node,
        &rival,
        roll_call_message(roll_call(&rival, 9, &configuration_of(3), 0)),
    );

    assert_eq!(
        rejects_sent_to(&outputs, &rival)[0].reason(),
        ElectionRejectReason::NotEligible
    );
    assert_eq!(node.state(), WorkerState::Leader);
}

// ---- What a node that stepped down does next ----

#[test]
fn a_node_that_stepped_down_contests_again_only_after_a_fresh_suspicion_timeout() {
    let clock = FakeClock::new();
    let mut node = leader_of_three(&clock);
    let step = node.step(Input::Message {
        from: worker("p1"),
        message: refusal_naming(3),
    });
    assert_eq!(node.state(), WorkerState::LeaderSuspect, "setup invariant");

    let retry_at = step.next_deadline.expect("due to contest again");
    assert!(
        retry_at > clock.now() + Duration::from_ticks(SUSPECT)
            && retry_at <= clock.now() + past_any_suspicion(SUSPECT),
        "{retry_at:?}"
    );
    let early = node.step(Input::Tick);
    assert!(published_roll_calls(&early.outputs).is_empty());

    clock.advance(retry_at - clock.now());
    let retried = node.step(Input::Tick);

    let calls = published_roll_calls(&retried.outputs);
    assert_eq!(calls.len(), 1, "{retried:?}");
    assert_eq!(calls[0].term, 4, "the term after the latest it has seen");
}

// ---- After a lost race, the leader that outlasted it ----

#[test]
fn a_failed_candidate_follows_the_leader_that_outlasted_its_candidacy() {
    let clock = FakeClock::new();
    let leader = worker("leader-1");
    let term_one_ack = || ack_message(leader_ack(&leader, 1, &configuration_of(3), Some(g0())));
    let mut node = voter_node(&clock, &worker("w1"), 3, SUSPECT);
    deliver(&mut node, &leader, term_one_ack());
    // Its contact lapses, and p1 (stale too) answers its call for term 2,
    // so it stands; the other voters hear the leader again and refuse it.
    let call = stand_as_candidate(&mut node, &clock, SUSPECT, &[worker("p1")]);
    assert_eq!(call.term, 2, "setup invariant");

    // While it stands, the term-1 leader's ack moves nothing.
    deliver(&mut node, &leader, term_one_ack());
    assert_eq!(node.state(), WorkerState::Candidate);

    // Its candidacy lapses unwon; that term was never won, so the term-1
    // leader, still holding, is the one to follow.
    clock.advance(timings(Duration::from_ticks(SUSPECT)).roll_call_deadline);
    let _ = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::LeaderSuspect, "setup invariant");
    let outputs = deliver(&mut node, &leader, term_one_ack());

    assert_eq!(state_changes(&outputs), vec![WorkerState::Active]);
    assert_eq!(node.known_leader(), Some((leader, 1)));
}

#[test]
fn a_voter_whose_candidate_lost_brings_down_the_leader_it_can_no_longer_follow() {
    let leader_clock = FakeClock::new();
    let mut leader = leader_of_three(&leader_clock);
    let w1 = worker("w1");
    let clock = FakeClock::new();
    let (p1, p2, rival) = (worker("p1"), worker("p2"), worker("w0"));
    let mut voter = voter_node(&clock, &p1, 3, SUSPECT);
    // Stale, it answers the rival's call for term 2 and grants its vote,
    // which the rival never turns into a win.
    clock.advance(past_any_suspicion(SUSPECT));
    deliver(
        &mut voter,
        &rival,
        roll_call_message(roll_call(&rival, 2, &configuration_of(3), 0)),
    );
    deliver(
        &mut voter,
        &rival,
        vote_request_message(vote_request(rival.clone(), 0, 2)),
    );
    assert_eq!(voter.highest_term_seen(), 2, "setup invariant");
    // Its own call for term 3 is refused by a voter still hearing w1, the
    // term-1 leader: w1 is who it heartbeats now, telling it of term 2.
    clock.advance(past_any_suspicion(SUSPECT));
    let _ = voter.step(Input::Tick);
    let started = voter.step(Input::Tick).outputs;
    let call = published_roll_calls(&started).remove(0);
    let refused = deliver(
        &mut voter,
        &p2,
        election_reject(
            &p1,
            call.term,
            &p2,
            ElectionRejectReason::LeaderStillValid,
            1,
            Some((&w1, 1)),
        ),
    );
    let heartbeat = sent_to(&refused, &w1)
        .into_iter()
        .next()
        .expect("it heartbeats the leader it was named");

    let outputs = deliver(&mut leader, &p1, heartbeat);

    assert_eq!(state_changes(&outputs), vec![WorkerState::LeaderSuspect]);
    assert_eq!(grants(&outputs), vec![None]);
}

#[test]
fn a_failed_candidate_grants_no_vote_in_a_term_below_the_one_it_stood_in() {
    let clock = FakeClock::new();
    let (me, rival, p1) = (worker("w1"), worker("w0"), worker("p1"));
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    clock.advance(past_any_suspicion(SUSPECT));
    // It answers the rival's call for term 1, which raises no term seen...
    deliver(
        &mut node,
        &rival,
        roll_call_message(roll_call(&rival, 1, &configuration_of(3), 0)),
    );
    // ...then stands for term 2 itself and loses.
    let call = stand_as_candidate(&mut node, &clock, SUSPECT, &[p1]);
    assert_eq!(call.term, 2, "setup invariant");
    clock.advance(timings(Duration::from_ticks(SUSPECT)).roll_call_deadline);
    let _ = node.step(Input::Tick);
    assert_eq!(node.state(), WorkerState::LeaderSuspect, "setup invariant");
    assert_eq!(node.highest_term_seen(), 0, "setup invariant");

    let outputs = deliver(
        &mut node,
        &rival,
        vote_request_message(vote_request(rival.clone(), 0, 1)),
    );

    assert_eq!(
        rejects_sent_to(&outputs, &rival)[0].reason(),
        ElectionRejectReason::StaleTerm
    );
}
