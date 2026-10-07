//! The vote, from both sides: a voter refuses every request it must not
//! grant with the reason, and ignores one that is not addressed to it; a
//! candidate wins once its granters are a majority of its call's respondents
//! and the voters of the call's configuration among them a quorum of it.

use crate::support::builders::{
    ack_message, configuration_of, g0, leader_ack,
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
use kabudachi_core::protocol::messages::{
    ElectionRejectReason, RollCall, VoteGrant,
    VoteRequest,
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
fn a_voter_refuses_a_request_it_must_not_grant_and_says_why() {
    let rows: [(&str, ElectionRejectReason, fn(&FakeClock) -> (TestNode, VoteRequest)); 7] = [
        (
            "a granted vote never switches, even to a better call",
            ElectionRejectReason::AlreadyVoted,
            |clock| {
                let mut node = stale_voter(clock, &worker("voter"));
                let (granted, better) = (worker("w5"), worker("w1"));
                answer(&mut node, &granted, 1, 0);
                request(&mut node, vote_request(granted, 0, 1));
                deliver(
                    &mut node,
                    &better,
                    roll_call_message(roll_call(&better, 1, &configuration_of(3), 0)),
                );
                (node, vote_request(better, 0, 1))
            },
        ),
        (
            "a term below one already voted in",
            ElectionRejectReason::StaleTerm,
            |clock| {
                let mut node = stale_voter(clock, &worker("voter"));
                let later = worker("w2");
                answer(&mut node, &later, 3, 0);
                request(&mut node, vote_request(later, 0, 3));
                (node, vote_request(worker("w1"), 0, 2))
            },
        ),
        (
            "a call under a configuration older than the voter's own",
            ElectionRejectReason::StaleGeneration,
            |clock| {
                let mut node = stale_voter(clock, &worker("voter"));
                let candidate = worker("w1");
                answer(&mut node, &candidate, 1, 0);
                // Between answering and the request, a leader's ack gives it
                // a newer configuration; its contact with that leader then
                // goes stale too.
                let newer = Configuration::single(Single {
                    generation: Generation::new(0, 0, 1),
                    base: g0(),
                    voter_count: 3,
                })
                .expect("valid");
                let leader = worker("leader");
                deliver(
                    &mut node,
                    &leader,
                    ack_message(leader_ack(&leader, 0, &newer, Some(g0()))),
                );
                clock.advance(past_any_suspicion(SUSPECT));
                (node, vote_request(candidate, 0, 1))
            },
        ),
        (
            "fresh leader contact",
            ElectionRejectReason::LeaderStillValid,
            |clock| {
                let node = voter_node(clock, &worker("voter"), 3, SUSPECT);
                (node, vote_request(worker("w1"), 0, 1))
            },
        ),
        (
            "a candidate asked by another candidate",
            ElectionRejectReason::NotEligible,
            |clock| (candidate_of_five(clock).0, vote_request(worker("rival"), 0, 1)),
        ),
        (
            "no roll call answered before the request",
            ElectionRejectReason::NotBestRollCall,
            |clock| {
                let node = stale_voter(clock, &worker("voter"));
                (node, vote_request(worker("w1"), 0, 1))
            },
        ),
        (
            "another recovery epoch",
            ElectionRejectReason::WrongRecoveryEpoch,
            |clock| {
                let mut node = stale_voter(clock, &worker("voter"));
                let candidate = worker("w1");
                answer(&mut node, &candidate, 1, 0);
                (node, vote_request(candidate, 1, 1))
            },
        ),
    ];

    for (name, reason, setup) in rows {
        let clock = FakeClock::new();
        let (mut node, asked) = setup(&clock);
        let candidate: WorkerId = asked.candidate_id.clone().expect("it names its candidate").into();

        let outputs = request(&mut node, asked);

        assert_eq!(the_refusal(&outputs, &candidate), reason, "{name}");
    }
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

/// How a roll call and the votes after it end for the node that called it.
#[derive(Debug, PartialEq)]
enum Outcome {
    /// No returning quorum answered the call.
    NoQuorum,
    /// The last grant wins it the term, and none before it did.
    Elected,
    /// Every grant arrives and the candidate never wins.
    StillCandidate,
}

// A win needs a majority of the respondents and a quorum of the returning
// voters among them.
#[test]
fn a_win_needs_a_respondent_majority_and_a_returning_quorum() {
    // (voters of the configuration, returning replies, pending replies,
    // grants in order, outcome)
    let rows: [(usize, &[&str], &[&str], &[&str], Outcome); 3] = [
        (3, &["w2"], &["p1", "p2", "p3", "p4"], &["w2", "p1", "p2"], Outcome::Elected),
        (3, &["w2"], &["p1", "p2"], &["p1", "p2", "w2"], Outcome::Elected),
        // The grants are a majority of the five respondents, but only the
        // candidate among them is a returning voter: one of three.
        (3, &["w2"], &["p1", "p2", "p3"], &["p1", "p2", "p3"], Outcome::StillCandidate),
    ];

    for (voters, returning, pending, grants, outcome) in rows {
        let clock = FakeClock::new();
        let me = worker("w1");
        let mut node = voter_node(&clock, &me, voters, SUSPECT);
        let call = published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT)).remove(0);
        for (names, admission) in [(returning, Some(g0())), (pending, None)] {
            for name in names {
                let respondent = worker(name);
                deliver(
                    &mut node,
                    &respondent,
                    roll_call_reply(&me, call.term, &respondent, admission),
                );
            }
        }
        close_roll_call(&mut node, &clock, SUSPECT);
        if outcome == Outcome::NoQuorum {
            assert_eq!(node.state(), WorkerState::NoQuorum, "{returning:?}");
            continue;
        }
        assert_eq!(node.state(), WorkerState::Candidate, "setup invariant");

        for (index, name) in grants.iter().enumerate() {
            grant_from(&mut node, &worker(name), call.term);
            let last = index + 1 == grants.len();
            let expected = if last && outcome == Outcome::Elected {
                WorkerState::LeaderReconciling
            } else {
                WorkerState::Candidate
            };
            assert_eq!(node.state(), expected, "{grants:?} after {name}");
        }
    }
}
