//! What a win does, from the respondents' side (ADR-0001 decision 8): a
//! respondent that accepts the winner's election certificate adopts the
//! joint configuration its roll call founded, admitted at its generation
//! with the admission it answered with kept as its prior one; the leader's
//! acks repair a lost certificate; and a worker adopts admission generations
//! only together with the configuration they belong to.

mod support;

use kabudachi_core::configuration::{Admission, Configuration, Generation};
use kabudachi_core::election::{KnownConfiguration, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{RollCallReply, election_message};
use kabudachi_core::time::Duration;
use support::builders::{
    ack_message, committed_from_g0, configuration_of, election_certificate,
    election_certificate_message, founded_from_g0, g0, leader_ack, past_any_suspicion, roll_call,
    roll_call_message, shard, timings, vote_request, vote_request_message, worker,
};
use support::clock::FakeClock;
use support::node::{TestNode, deliver, sent_to, voter_node};

/// Every node here suspects its leader after this many ticks.
const SUSPECT: u64 = 10;

/// What an election for `term` under `configuration_of(3)` founds with
/// `respondents` respondents.
fn founded_in(term: u64, respondents: usize) -> Configuration {
    founded_from_g0(term, 3, respondents)
}

/// Admitted at `founded`'s generation, having held `prior` before.
fn admitted_at(founded: &Configuration, prior: Option<Generation>) -> Admission {
    Admission {
        current: Some(founded.generation()),
        prior,
    }
}

/// `me`, whose leader contact has gone stale, knowing `known`.
fn stale_node(clock: &FakeClock, me: &WorkerId, known: KnownConfiguration) -> TestNode {
    let node = WorkerNode::new(
        me.clone(),
        IncarnationId::new("incarnation-1"),
        shard("shard-1"),
        clock.clone(),
        known,
        None,
        timings(Duration::from_ticks(SUSPECT)),
    );
    clock.advance(past_any_suspicion(SUSPECT));
    node
}

/// A voter of `configuration_of(3)`, admitted at `g0`, with stale leader
/// contact.
fn stale_voter(clock: &FakeClock, me: &WorkerId) -> TestNode {
    let node = voter_node(clock, me, 3, SUSPECT);
    clock.advance(past_any_suspicion(SUSPECT));
    node
}

/// A pending member of `configuration_of(3)`, with stale leader contact.
fn stale_pending_member(clock: &FakeClock, me: &WorkerId) -> TestNode {
    stale_node(
        clock,
        me,
        KnownConfiguration {
            configuration: configuration_of(3),
            admission: None,
        },
    )
}

/// Has `node` answer `initiator`'s roll call for `term` under `g0`'s
/// configuration of 3, and returns its reply.
fn answer(node: &mut TestNode, initiator: &WorkerId, term: u64) -> RollCallReply {
    let outputs = deliver(
        node,
        initiator,
        roll_call_message(roll_call(initiator, term, &configuration_of(3), 0)),
    );
    let replies: Vec<RollCallReply> = sent_to(&outputs, initiator)
        .into_iter()
        .filter_map(|message| match message.payload {
            Some(election_message::Payload::RollCallReply(reply)) => Some(reply),
            _ => None,
        })
        .collect();
    assert_eq!(replies.len(), 1, "setup invariant: answered");
    replies.into_iter().next().unwrap()
}

/// Has `node` answer `candidate`'s roll call for `term` and grant it its
/// vote.
fn answer_and_grant(node: &mut TestNode, candidate: &WorkerId, term: u64) {
    answer(node, candidate, term);
    let outputs = deliver(
        node,
        candidate,
        vote_request_message(vote_request(candidate.clone(), 0, term)),
    );
    assert!(
        sent_to(&outputs, candidate).iter().any(|message| matches!(
            message.payload,
            Some(election_message::Payload::VoteGrant(_))
        )),
        "setup invariant: granted"
    );
}

/// Hands `node` `leader`'s certificate that its election in `term` founded
/// `founded_in(term, respondents)`, where `node` was admitted having held
/// `prior` before.
fn certify(
    node: &mut TestNode,
    leader: &WorkerId,
    term: u64,
    respondents: usize,
    prior: Option<Generation>,
) {
    let founded = founded_in(term, respondents);
    deliver(
        node,
        leader,
        election_certificate_message(election_certificate(
            leader,
            term,
            &founded,
            admitted_at(&founded, prior),
        )),
    );
}

/// The one roll-call reply among what `node` sent `initiator`.
fn reply_to(outputs: &[kabudachi_core::election::Output], initiator: &WorkerId) -> RollCallReply {
    sent_to(outputs, initiator)
        .into_iter()
        .find_map(|message| match message.payload {
            Some(election_message::Payload::RollCallReply(reply)) => Some(reply),
            _ => None,
        })
        .expect("it answers the roll call")
}

// ---- The certificate ----

#[test]
fn a_respondent_that_granted_the_winner_adopts_what_its_roll_call_founded() {
    let clock = FakeClock::new();
    let (me, winner) = (worker("w2"), worker("w1"));
    let mut node = stale_voter(&clock, &me);
    answer_and_grant(&mut node, &winner, 1);
    let founded = founded_in(1, 3);

    certify(&mut node, &winner, 1, 3, Some(g0()));

    assert_eq!(node.configuration(), Some(&founded));
    assert_eq!(node.admission(), Some(founded.generation()));
    assert_eq!(node.prior_admission(), Some(g0()));
}

#[test]
fn a_pending_respondent_is_admitted_and_answers_the_next_roll_call_as_a_voter() {
    let clock = FakeClock::new();
    let (me, winner) = (worker("p1"), worker("w1"));
    let mut node = stale_pending_member(&clock, &me);
    // It answered, but its vote request was lost: it granted nothing.
    answer(&mut node, &winner, 1);
    let founded = founded_in(1, 3);

    certify(&mut node, &winner, 1, 3, None);
    assert!(!node.is_pending_member());
    let next_initiator = worker("w3");
    let outputs = deliver(
        &mut node,
        &next_initiator,
        roll_call_message(roll_call(&next_initiator, 2, &founded, 0)),
    );

    let reply = reply_to(&outputs, &next_initiator);
    assert_eq!(reply.admission(), Some(founded.generation()));
    assert_eq!(reply.prior_admission(), None);
    assert!(founded.is_voter(reply.admission()), "a returning voter");
}

#[test]
fn a_certificate_for_a_term_the_node_has_moved_past_is_refused_unless_it_granted_that_winner() {
    let clock = FakeClock::new();
    let (winner, later) = (worker("w1"), worker("w5"));
    let founded = founded_in(1, 3);

    let mut granted = stale_voter(&clock, &worker("w2"));
    answer_and_grant(&mut granted, &winner, 1);
    answer_and_grant(&mut granted, &later, 3);
    certify(&mut granted, &winner, 1, 3, Some(g0()));
    assert_eq!(
        granted.admission(),
        Some(founded.generation()),
        "it granted that winner"
    );

    let mut passed_over = stale_voter(&clock, &worker("w4"));
    answer(&mut passed_over, &winner, 1);
    answer_and_grant(&mut passed_over, &later, 3);
    certify(&mut passed_over, &winner, 1, 3, Some(g0()));
    assert_eq!(passed_over.configuration(), Some(&configuration_of(3)));
    assert_eq!(passed_over.admission(), Some(g0()));
}

#[test]
fn a_certificate_for_another_shard_or_recovery_epoch_is_ignored() {
    let clock = FakeClock::new();
    let winner = worker("w1");
    let mut node = stale_voter(&clock, &worker("w2"));
    answer_and_grant(&mut node, &winner, 1);
    let founded = founded_in(1, 3);
    let certificate = || election_certificate(&winner, 1, &founded, admitted_at(&founded, None));

    let mut other_shard = certificate();
    other_shard.shard_id = Some(shard("shard-2").into());
    let mut other_epoch = certificate();
    other_epoch.recovery_epoch = 1;
    for certificate in [other_shard, other_epoch] {
        deliver(
            &mut node,
            &winner,
            election_certificate_message(certificate),
        );
    }
    deliver(
        &mut node,
        &worker("relay"),
        election_certificate_message(certificate()),
    );

    assert_eq!(node.configuration(), Some(&configuration_of(3)));
    assert_eq!(node.admission(), Some(g0()));
}

#[test]
fn a_node_still_joining_ignores_a_certificate() {
    let clock = FakeClock::new();
    let winner = worker("w1");
    let mut node: TestNode = WorkerNode::bootstrapping(
        worker("joiner"),
        IncarnationId::new("incarnation-1"),
        shard("shard-1"),
        clock.clone(),
        None,
        timings(Duration::from_ticks(SUSPECT)),
    );

    deliver(
        &mut node,
        &winner,
        election_certificate_message(election_certificate(
            &winner,
            1,
            &founded_in(1, 3),
            admitted_at(&founded_in(1, 3), None),
        )),
    );

    assert_eq!(node.configuration(), None);
    assert!(node.is_pending_member());
}

// ---- Acks after a win ----

#[test]
fn a_lost_certificate_is_repaired_by_the_winners_ack() {
    let clock = FakeClock::new();
    let (me, winner) = (worker("w2"), worker("w1"));
    let mut node = stale_voter(&clock, &me);
    answer_and_grant(&mut node, &winner, 1);
    let founded = founded_in(1, 3);

    deliver(
        &mut node,
        &winner,
        ack_message(ack_admitting(&winner, &founded, Some(g0()))),
    );

    assert_eq!(node.configuration(), Some(&founded));
    assert_eq!(node.admission(), Some(founded.generation()));
    assert_eq!(node.prior_admission(), Some(g0()));
}

/// An ack from `leader`, elected in term 1, carrying `configuration` and
/// admitting the recipient at its generation, having held `prior` before.
fn ack_admitting(
    leader: &WorkerId,
    configuration: &Configuration,
    prior: Option<Generation>,
) -> kabudachi_core::protocol::messages::LeaderHeartbeatAck {
    kabudachi_core::protocol::messages::LeaderHeartbeatAck {
        recipient_prior_admission: prior.map(Into::into),
        ..leader_ack(leader, 1, configuration, Some(configuration.generation()))
    }
}

#[test]
fn a_worker_outside_the_winning_roll_call_counts_on_the_old_side_only_until_the_commit() {
    let clock = FakeClock::new();
    let (me, winner) = (worker("w9"), worker("w1"));
    let mut node = stale_voter(&clock, &me);
    let founded = founded_in(1, 3);

    deliver(
        &mut node,
        &winner,
        ack_message(leader_ack(&winner, 1, &founded, None)),
    );
    assert_eq!(node.configuration(), Some(&founded));
    assert_eq!(node.admission(), Some(g0()), "it keeps its old admission");
    assert!(
        founded.is_voter(node.admission()),
        "still a voter of the old side: the configuration it moved from"
    );
    let committed = committed_from_g0(1, 1, 3);
    deliver(
        &mut node,
        &winner,
        ack_message(leader_ack(&winner, 1, &committed, None)),
    );
    // Its leader's contact goes stale, and a roll call under the committed
    // configuration follows.
    clock.advance(past_any_suspicion(SUSPECT));
    let next_initiator = worker("w3");
    let outputs = deliver(
        &mut node,
        &next_initiator,
        roll_call_message(roll_call(&next_initiator, 2, &committed, 0)),
    );

    let reply = reply_to(&outputs, &next_initiator);
    assert_eq!(reply.admission(), Some(g0()));
    assert!(
        !committed.is_voter(reply.admission()),
        "older than the new base: a new voter, not a returning one"
    );
}

#[test]
fn an_ack_carrying_an_older_configuration_than_the_nodes_own_leaves_its_admission_alone() {
    let clock = FakeClock::new();
    let later = founded_in(2, 3);
    let mut node = stale_node(
        &clock,
        &worker("w2"),
        KnownConfiguration {
            configuration: later.clone(),
            admission: Some(later.generation()),
        },
    );
    let older = founded_in(1, 3);
    let stale_leader = worker("w1");

    deliver(
        &mut node,
        &stale_leader,
        ack_message(leader_ack(
            &stale_leader,
            2,
            &older,
            Some(older.generation()),
        )),
    );

    assert_eq!(node.configuration(), Some(&later));
    assert_eq!(node.admission(), Some(later.generation()));
}

#[test]
fn an_ack_carrying_the_nodes_own_configuration_repairs_its_admission() {
    let clock = FakeClock::new();
    let founded = founded_in(1, 3);
    let mut node = stale_node(
        &clock,
        &worker("w2"),
        KnownConfiguration {
            configuration: founded.clone(),
            admission: Some(g0()),
        },
    );
    let leader = worker("w1");

    deliver(
        &mut node,
        &leader,
        ack_message(ack_admitting(&leader, &founded, Some(g0()))),
    );

    assert_eq!(node.admission(), Some(founded.generation()));
    assert_eq!(node.prior_admission(), Some(g0()));
}

#[test]
fn a_committed_configuration_drops_the_prior_admission() {
    let clock = FakeClock::new();
    let founded = founded_in(1, 3);
    let mut node = stale_voter(&clock, &worker("w2"));
    let leader = worker("w1");
    deliver(
        &mut node,
        &leader,
        ack_message(ack_admitting(&leader, &founded, Some(g0()))),
    );

    let committed = committed_from_g0(1, 1, 3);
    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(
            &leader,
            1,
            &committed,
            Some(founded.generation()),
        )),
    );

    assert_eq!(node.configuration(), Some(&committed));
    assert_eq!(node.admission(), Some(founded.generation()));
    assert_eq!(node.prior_admission(), None);
}
