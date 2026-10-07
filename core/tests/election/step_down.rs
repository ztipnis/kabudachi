//! Step-down: a node that holds or contests a term
//! gives it up once it has seen a later one (from a vote it grants, an ack,
//! a refusal or an election certificate), to follow the leader of that
//! term if an ack from it is what told it, and otherwise to suspect its
//! leader again.

use crate::support::builders::{
    ack_message, configuration_of, election_reject, g0, leader_ack,
    past_any_suspicion, roll_call, roll_call_message, timings, vote_request,
    vote_request_message, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{
    TestNode, deliver, elect, grants, published_roll_calls, rejects_sent_to, sent_to,
    stand_as_candidate, state_changes, voter_node,
};
use kabudachi_core::election::Input;
use kabudachi_core::protocol::messages::ElectionRejectReason;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;

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
    assert_eq!(node.known_leader(), Some((leader.clone(), 1)));

    // Its vote for itself in term 2 stands: it grants no other candidate
    // that term's vote.
    clock.advance(past_any_suspicion(SUSPECT));
    let rival = worker("w0");
    let refused = deliver(
        &mut node,
        &rival,
        vote_request_message(vote_request(rival.clone(), 0, 2)),
    );
    assert_eq!(
        rejects_sent_to(&refused, &rival)[0].reason(),
        ElectionRejectReason::AlreadyVoted
    );

    // The term it stood in stays taken: losing contact again, its next roll
    // call contests the term after it, not term 2 again.
    clock.advance(past_any_suspicion(SUSPECT));
    let _ = node.step(Input::Tick);
    let restarted = node.step(Input::Tick).outputs;
    let calls = published_roll_calls(&restarted);
    assert_eq!(calls.len(), 1, "{restarted:?}");
    assert_eq!(calls[0].term, 3);
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
