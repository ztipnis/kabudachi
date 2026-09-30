//! The vote (ADR-0001 decisions 6 to 8), from both sides: a voter grants
//! at most one vote per term, only to the initiator of the best roll call
//! it answered, and refuses every other request with the reason; a
//! candidate wins once its granters are a majority of that call's
//! respondents and the voters of the call's configuration among them a
//! quorum of it, and leads the configuration the respondents found, every
//! one of them admitted at its generation.

use crate::support::builders::{
    ack_message, configuration_of, founded_from_g0, g0, heartbeat, heartbeat_message, leader_ack,
    past_any_suspicion, roll_call, roll_call_message, roll_call_reply, shard, vote_grant,
    vote_grant_message, vote_request, vote_request_message, worker,
};
use crate::support::builders::checked;
use crate::support::clock::FakeClock;
use kabudachi_core::protocol::checked::{Checked, CheckedPayload};
use crate::support::node::{
    TestNode, close_roll_call, connect, deliver, published_roll_calls, rejects_sent_to, sent_to,
    stand_as_candidate, start_roll_call, voter_node,
};
use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::Output;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    ElectionCertificate, ElectionRejectReason, LeaderHeartbeatAck, RollCall, VoteGrant,
    VoteRequest, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;

/// Every node here suspects its leader after this many ticks.
const SUSPECT: u64 = 10;

// ---- The voter ----

/// `me`, a voter of 3 whose leader contact has gone stale.
fn stale_voter(clock: &FakeClock, me: &WorkerId) -> TestNode {
    let node = voter_node(clock, me, 3, SUSPECT);
    clock.advance(past_any_suspicion(SUSPECT));
    node
}

fn answer(node: &mut TestNode, initiator: &WorkerId, term: u64, timestamp_millis: u64) {
    let outputs = deliver(
        node,
        initiator,
        roll_call_message(roll_call(
            initiator,
            term,
            &configuration_of(3),
            timestamp_millis,
        )),
    );
    assert_eq!(
        sent_to(&outputs, initiator).len(),
        1,
        "setup invariant: answered"
    );
}

fn request(node: &mut TestNode, request: VoteRequest) -> Vec<Output> {
    let candidate: WorkerId = request
        .candidate_id
        .clone()
        .expect("the request names its candidate")
        .into();
    deliver(node, &candidate, vote_request_message(request))
}

fn the_grant(outputs: &[Output], candidate: &WorkerId) -> Checked<VoteGrant> {
    let grants: Vec<Checked<VoteGrant>> = sent_to(outputs, candidate)
        .into_iter()
        .filter_map(|message| match checked(message).into_payload() {
            Some(CheckedPayload::VoteGrant(grant)) => Some(grant),
            _ => None,
        })
        .collect();
    assert_eq!(
        grants.len(),
        1,
        "expected exactly one grant to {candidate:?}"
    );
    grants.into_iter().next().unwrap()
}

fn the_refusal(outputs: &[Output], candidate: &WorkerId) -> ElectionRejectReason {
    let rejects = rejects_sent_to(outputs, candidate);
    assert_eq!(
        rejects.len(),
        1,
        "expected exactly one refusal to {candidate:?}"
    );
    assert_eq!(
        sent_to(outputs, candidate).len(),
        1,
        "and nothing else to {candidate:?}"
    );
    rejects[0].reason()
}

#[test]
fn a_voter_grants_the_initiator_of_the_call_it_answered() {
    let clock = FakeClock::new();
    let me = worker("voter");
    let mut node = stale_voter(&clock, &me);
    let candidate = worker("w1");
    answer(&mut node, &candidate, 1, 0);

    let outputs = request(&mut node, vote_request(candidate.clone(), 0, 1));

    let grant = the_grant(&outputs, &candidate);
    assert_eq!(grant.voter_id(), me);
    assert_eq!(grant.candidate_id(), candidate);
    assert_eq!(grant.term, 1);
    assert_eq!(grant.shard_id(), shard("shard-1"));
}

#[test]
fn granting_raises_the_highest_term_seen_to_the_requests_term() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"));
    let candidate = worker("w1");
    answer(&mut node, &candidate, 2, 0);
    request(&mut node, vote_request(candidate, 0, 2));

    let late = worker("w2");
    let outputs = deliver(
        &mut node,
        &late,
        roll_call_message(roll_call(&late, 2, &configuration_of(3), 0)),
    );

    assert_eq!(
        rejects_sent_to(&outputs, &late)[0].reason(),
        ElectionRejectReason::StaleTerm
    );
}

#[test]
fn a_voter_grants_only_the_best_call_it_answered() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"));
    let (first, better) = (worker("w5"), worker("w1"));
    answer(&mut node, &first, 1, 0);
    answer(&mut node, &better, 1, 0);

    let to_first = request(&mut node, vote_request(first.clone(), 0, 1));
    let to_better = request(&mut node, vote_request(better.clone(), 0, 1));

    assert_eq!(
        the_refusal(&to_first, &first),
        ElectionRejectReason::NotBestRollCall
    );
    the_grant(&to_better, &better);
}

#[test]
fn a_granted_vote_never_switches_even_to_a_better_call() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"));
    let (granted, better) = (worker("w5"), worker("w1"));
    answer(&mut node, &granted, 1, 0);
    request(&mut node, vote_request(granted.clone(), 0, 1));
    deliver(
        &mut node,
        &better,
        roll_call_message(roll_call(&better, 1, &configuration_of(3), 0)),
    );

    let outputs = request(&mut node, vote_request(better.clone(), 0, 1));

    assert_eq!(
        the_refusal(&outputs, &better),
        ElectionRejectReason::AlreadyVoted
    );
}

#[test]
fn a_repeat_request_from_the_granted_candidate_is_refused_as_already_voted() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"));
    let candidate = worker("w1");
    answer(&mut node, &candidate, 1, 0);
    request(&mut node, vote_request(candidate.clone(), 0, 1));

    let outputs = request(&mut node, vote_request(candidate.clone(), 0, 1));

    assert_eq!(
        the_refusal(&outputs, &candidate),
        ElectionRejectReason::AlreadyVoted,
        "already voted is checked before stale term"
    );
}

#[test]
fn a_request_at_another_recovery_epoch_is_refused_first() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"));
    let candidate = worker("w1");
    answer(&mut node, &candidate, 1, 0);

    let outputs = request(&mut node, vote_request(candidate.clone(), 1, 1));

    assert_eq!(
        the_refusal(&outputs, &candidate),
        ElectionRejectReason::WrongRecoveryEpoch
    );
    let outputs = request(&mut node, vote_request(candidate.clone(), 0, 1));
    assert_eq!(
        the_grant(&outputs, &candidate).candidate_id(),
        candidate,
        "a current-epoch request for the same term and answered call is granted: the refused \
         one left no trace in the ballot"
    );
}

#[test]
fn a_request_for_a_term_already_seen_is_refused_as_stale() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"));
    let (earlier, later) = (worker("w1"), worker("w2"));
    answer(&mut node, &later, 3, 0);
    request(&mut node, vote_request(later, 0, 3));

    let outputs = request(&mut node, vote_request(earlier.clone(), 0, 2));

    assert_eq!(
        the_refusal(&outputs, &earlier),
        ElectionRejectReason::StaleTerm
    );
}

#[test]
fn a_request_for_a_call_under_a_configuration_older_than_the_voters_own_is_refused() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"));
    let candidate = worker("w1");
    answer(&mut node, &candidate, 1, 0);
    // Between answering and the request, a leader's ack gives it a newer
    // configuration; its contact with that leader then goes stale too.
    let newer = Configuration::single(Single {
        generation: Generation::new(0, 0, 1),
        base: g0(),
        voter_count: 3,
    }).expect("valid");
    let leader = worker("leader");
    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 0, &newer, Some(g0()))),
    );
    clock.advance(past_any_suspicion(SUSPECT));

    let outputs = request(&mut node, vote_request(candidate.clone(), 0, 1));

    assert_eq!(
        the_refusal(&outputs, &candidate),
        ElectionRejectReason::StaleGeneration
    );
}

#[test]
fn a_voter_with_fresh_leader_contact_grants_nothing() {
    let clock = FakeClock::new();
    let mut node = voter_node(&clock, &worker("voter"), 3, SUSPECT);
    let candidate = worker("w1");

    let outputs = request(&mut node, vote_request(candidate.clone(), 0, 1));

    assert_eq!(
        the_refusal(&outputs, &candidate),
        ElectionRejectReason::LeaderStillValid
    );
}

#[test]
fn a_candidate_asked_by_another_candidate_refuses_as_not_eligible() {
    let clock = FakeClock::new();
    let (mut node, _) = candidate_of_five(&clock);
    let rival = worker("rival");

    let outputs = request(&mut node, vote_request(rival.clone(), 0, 1));

    assert_eq!(
        the_refusal(&outputs, &rival),
        ElectionRejectReason::NotEligible
    );
}

#[test]
fn a_request_for_another_shard_or_not_from_its_candidate_is_ignored() {
    let clock = FakeClock::new();
    let mut node = stale_voter(&clock, &worker("voter"));
    let candidate = worker("w1");
    answer(&mut node, &candidate, 1, 0);

    let mut other_shard = vote_request(candidate.clone(), 0, 1);
    other_shard.shard_id = Some(shard("shard-2").into());
    let ignored_shard = request(&mut node, other_shard);
    let relayed = deliver(
        &mut node,
        &worker("relay"),
        vote_request_message(vote_request(candidate.clone(), 0, 1)),
    );
    assert!(sent_to(&ignored_shard, &candidate).is_empty());
    assert!(
        relayed
            .iter()
            .all(|output| !matches!(output, Output::Send { .. }))
    );

    // The vote is still there for the real request.
    let outputs = request(&mut node, vote_request(candidate.clone(), 0, 1));
    the_grant(&outputs, &candidate);
}

// ---- The candidate ----

/// `w1`, a voter of 5, standing as `Candidate` for term 1 once `p1` and `p2`
/// answered its roll call; connected to both. Returns the node and its call.
fn candidate_of_five(clock: &FakeClock) -> (TestNode, Checked<RollCall>) {
    let me = worker("w1");
    let mut node = voter_node(clock, &me, 5, SUSPECT);
    connect(&mut node, &[worker("p1"), worker("p2")]);
    let call = stand_as_candidate(&mut node, clock, SUSPECT, &[worker("p1"), worker("p2")]);
    (node, call)
}

fn grant_from(node: &mut TestNode, voter: &WorkerId, term: u64) -> Vec<Output> {
    deliver(
        node,
        voter,
        vote_grant_message(vote_grant(worker("w1"), voter.clone(), term)),
    )
}

fn certificates_to(outputs: &[Output], voter: &WorkerId) -> Vec<Checked<ElectionCertificate>> {
    sent_to(outputs, voter)
        .into_iter()
        .filter_map(|message| match checked(message).into_payload() {
            Some(CheckedPayload::ElectionCertificate(certificate)) => Some(certificate),
            _ => None,
        })
        .collect()
}

#[test]
fn a_candidate_wins_once_its_returning_granters_are_a_quorum() {
    let clock = FakeClock::new();
    let (mut node, call) = candidate_of_five(&clock);

    grant_from(&mut node, &worker("p1"), call.term);
    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "two of five, itself included, are no quorum"
    );
    grant_from(&mut node, &worker("p2"), call.term);

    assert_eq!(node.state(), WorkerState::Leader);
    assert_eq!(node.term(), 1);
}

/// What an election for term 1 under `configuration_of(old_voter_count)`
/// founds with `respondents` respondents: the joint configuration of the
/// respondents, at (0, 1, 1), and that configuration.
fn founded_in_term_1(old_voter_count: usize, respondents: usize) -> Configuration {
    founded_from_g0(1, old_voter_count, respondents)
}

#[test]
fn the_winner_certifies_what_its_respondents_founded_to_each_of_them() {
    let clock = FakeClock::new();
    let (granting, silent, pending) = (worker("w2"), worker("w3"), worker("p1"));
    // Four respondents, itself included: three grants, two of them from
    // voters of three, win.
    let (mut node, call) = candidate_of_three_with(
        &clock,
        &[granting.clone(), silent.clone()],
        std::slice::from_ref(&pending),
    );
    grant_from(&mut node, &granting, call.term);

    let won = grant_from(&mut node, &pending, call.term);

    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");
    for respondent in [&granting, &silent, &pending] {
        let certificates = certificates_to(&won, respondent);
        assert_eq!(certificates.len(), 1, "one certificate to {respondent:?}");
        assert_eq!(certificates[0].term, 1);
        assert_eq!(certificates[0].leader_id(), worker("w1"));
        assert_eq!(certificates[0].configuration(), founded_in_term_1(3, 4));
        assert_eq!(
            certificates[0].recipient_admission(),
            Some(Generation::new(0, 1, 1))
        );
    }
    let prior_of =
        |respondent: &WorkerId| certificates_to(&won, respondent)[0].recipient_prior_admission();
    assert_eq!(prior_of(&granting), Some(g0()));
    assert_eq!(prior_of(&silent), Some(g0()));
    assert_eq!(prior_of(&pending), None, "a pending respondent had none");
    assert!(
        certificates_to(&won, &worker("w1")).is_empty(),
        "none to itself"
    );
}

#[test]
fn a_grant_from_a_worker_that_did_not_answer_the_roll_call_is_ignored() {
    let clock = FakeClock::new();
    let (mut node, call) = candidate_of_five(&clock);

    grant_from(&mut node, &worker("p1"), call.term);
    grant_from(&mut node, &worker("stranger"), call.term);

    assert_eq!(node.state(), WorkerState::Candidate);
}

#[test]
fn a_repeated_grant_counts_once() {
    let clock = FakeClock::new();
    let (mut node, call) = candidate_of_five(&clock);

    grant_from(&mut node, &worker("p1"), call.term);
    grant_from(&mut node, &worker("p1"), call.term);

    assert_eq!(node.state(), WorkerState::Candidate);
}

#[test]
fn a_grant_for_another_term_candidate_shard_or_epoch_or_from_another_sender_is_ignored() {
    let clock = FakeClock::new();
    let (mut node, call) = candidate_of_five(&clock);
    grant_from(&mut node, &worker("p1"), call.term);
    let p2 = worker("p2");

    let mut other_candidate = vote_grant(worker("w1"), p2.clone(), call.term);
    other_candidate.candidate_id = Some(worker("someone").into());
    let mut other_shard = vote_grant(worker("w1"), p2.clone(), call.term);
    other_shard.shard_id = Some(shard("shard-2").into());
    let mut other_epoch = vote_grant(worker("w1"), p2.clone(), call.term);
    other_epoch.recovery_epoch = 1;
    let other_term = vote_grant(worker("w1"), p2.clone(), call.term + 1);
    for grant in [other_candidate, other_shard, other_epoch, other_term] {
        deliver(&mut node, &p2, vote_grant_message(grant));
    }
    deliver(
        &mut node,
        &worker("relay"),
        vote_grant_message(vote_grant(worker("w1"), p2.clone(), call.term)),
    );

    assert_eq!(node.state(), WorkerState::Candidate);
}

/// `w1`, a voter of 3, standing as `Candidate` once each of `returning`,
/// admitted at `g0`, and each of `pending`, with no admission, answered its
/// roll call. Returns the node and its call.
fn candidate_of_three_with(
    clock: &FakeClock,
    returning: &[WorkerId],
    pending: &[WorkerId],
) -> (TestNode, Checked<RollCall>) {
    let me = worker("w1");
    let mut node = voter_node(clock, &me, 3, SUSPECT);
    let call = published_roll_calls(&start_roll_call(&mut node, clock, SUSPECT)).remove(0);
    for respondent in returning {
        deliver(
            &mut node,
            respondent,
            roll_call_reply(&me, call.term, respondent, Some(g0())),
        );
    }
    for respondent in pending {
        deliver(
            &mut node,
            respondent,
            roll_call_reply(&me, call.term, respondent, None),
        );
    }
    close_roll_call(&mut node, clock, SUSPECT);
    assert_eq!(node.state(), WorkerState::Candidate, "setup invariant");
    (node, call)
}

#[test]
fn a_win_needs_a_majority_of_the_respondents_even_once_the_returning_voters_are_a_quorum() {
    let clock = FakeClock::new();
    let pending = [worker("p1"), worker("p2"), worker("p3"), worker("p4")];
    // Six respondents, itself included: a majority is four.
    let (mut node, call) = candidate_of_three_with(&clock, &[worker("w2")], &pending);

    grant_from(&mut node, &worker("w2"), call.term);
    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "two of three returning voters, but two of six respondents"
    );
    grant_from(&mut node, &pending[0], call.term);
    assert_eq!(node.state(), WorkerState::Candidate, "three of six");
    grant_from(&mut node, &pending[1], call.term);

    assert_eq!(node.state(), WorkerState::Leader);
}

#[test]
fn a_win_needs_a_returning_quorum_even_once_a_majority_of_the_respondents_granted() {
    let clock = FakeClock::new();
    let returning = worker("w2");
    let pending = [worker("p1"), worker("p2")];
    // Four respondents, itself included: a majority is three.
    let (mut node, call) =
        candidate_of_three_with(&clock, std::slice::from_ref(&returning), &pending);

    grant_from(&mut node, &pending[0], call.term);
    grant_from(&mut node, &pending[1], call.term);
    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "three of four respondents, but one of three returning voters"
    );
    grant_from(&mut node, &returning, call.term);

    assert_eq!(node.state(), WorkerState::Leader);
}

#[test]
fn a_refusal_of_a_vote_request_changes_nothing() {
    let clock = FakeClock::new();
    let (mut node, call) = candidate_of_five(&clock);
    let p1 = worker("p1");
    let reject = kabudachi_core::protocol::messages::ElectionReject {
        shard_id: Some(shard("shard-1").into()),
        term: call.term,
        initiator_id: Some(worker("w1").into()),
        rejecter_id: Some(p1.clone().into()),
        reason: ElectionRejectReason::AlreadyVoted as i32,
        highest_term_seen: call.term,
        configuration: Some((&configuration_of(5)).into()),
        leader: None,
    };

    let outputs = deliver(
        &mut node,
        &p1,
        crate::support::builders::message(election_message::Payload::ElectionReject(reject)),
    );

    assert_eq!(node.state(), WorkerState::Candidate);
    assert!(outputs.is_empty());
}

// ---- The winner's roster ----

fn ack_to(outputs: &[Output], worker: &WorkerId) -> Checked<LeaderHeartbeatAck> {
    let acks: Vec<Checked<LeaderHeartbeatAck>> = sent_to(outputs, worker)
        .into_iter()
        .filter_map(|message| match checked(message).into_payload() {
            Some(CheckedPayload::HeartbeatAck(ack)) => Some(ack),
            _ => None,
        })
        .collect();
    assert_eq!(acks.len(), 1, "expected one ack to {worker:?}");
    acks.into_iter().next().unwrap()
}

#[test]
fn the_winner_leads_the_joint_configuration_its_respondents_found_each_admitted_at_its_generation()
{
    let clock = FakeClock::new();
    let me = worker("w1");
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    let (pending, returning) = (worker("pending"), worker("returning"));
    connect(&mut node, &[pending.clone(), returning.clone()]);
    let call = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
    deliver(
        &mut node,
        &pending,
        roll_call_reply(&me, call.term, &pending, None),
    );
    deliver(
        &mut node,
        &returning,
        roll_call_reply(&me, call.term, &returning, Some(g0())),
    );
    close_roll_call(&mut node, &clock, SUSPECT);

    let won = grant_from(&mut node, &returning, call.term);
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");

    let founded = founded_in_term_1(3, 3);
    assert_eq!(node.configuration(), Some(&founded));
    assert_eq!(node.admission(), Some(founded.generation()));
    assert_eq!(node.prior_admission(), Some(g0()));
    for respondent in [&returning, &pending] {
        let ack = ack_to(&won, respondent);
        assert_eq!(ack.configuration(), founded, "to {respondent:?}");
        assert_eq!(
            ack.recipient_admission(),
            Some(founded.generation()),
            "a pending respondent is admitted too: {respondent:?}"
        );
    }
    assert_eq!(
        ack_to(&won, &returning).recipient_prior_admission(),
        Some(g0())
    );
    assert_eq!(ack_to(&won, &pending).recipient_prior_admission(), None);
}

#[test]
fn a_worker_that_heartbeats_the_leader_without_being_in_its_roster_is_acked_with_no_admission() {
    let clock = FakeClock::new();
    let (mut node, call) = candidate_of_five(&clock);
    grant_from(&mut node, &worker("p1"), call.term);
    grant_from(&mut node, &worker("p2"), call.term);
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");
    let missed = worker("missed-the-call");

    let outputs = deliver(
        &mut node,
        &missed,
        heartbeat_message(heartbeat(&missed, None)),
    );

    let ack = ack_to(&outputs, &missed);
    assert_eq!(ack.recipient_admission(), None);
    assert_eq!(ack.configuration(), founded_in_term_1(5, 3));
}
