//! What a win does, from the respondents' side: a
//! respondent that accepts the winner's election certificate adopts the
//! joint configuration its roll call founded, admitted at its generation
//! with the admission it answered with kept as its prior one; the leader's
//! acks repair a lost certificate; and a worker adopts admission generations
//! only together with the configuration they belong to.

use crate::support::builders::{epoch, 
    ack_message, configuration_of, election_certificate,
    election_certificate_message, founded_from_g0, g0, leader_ack,
    past_any_suspicion, roll_call, roll_call_message, shard, timings, vote_request,
    vote_request_message, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{TestNode, deliver, sent_to, voter_node};
use kabudachi_core::configuration::{Admission, Configuration, Generation, Single};
use kabudachi_core::election::{Entry, Identity, KnownConfiguration, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::{RollCallReply, election_message};
use kabudachi_core::time::Duration;

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
    let node = WorkerNode::start(
        Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(SUSPECT)),
        },
        Entry::Known(known),
        clock.clone(),
        None,
    )
    .0;
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

// ---- The certificate ----

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
    // A certificate of epoch 1 carries a configuration of epoch 1.
    let founded_in_epoch_1 = Configuration::single(Single {
        generation: Generation::new(epoch(1), 1, 1),
        base: Generation::new(epoch(1), 1, 1),
        voter_count: 3,
    })
    .expect("valid");
    let mut other_epoch = election_certificate(
        &winner,
        1,
        &founded_in_epoch_1,
        admitted_at(&founded_in_epoch_1, None),
    );
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

// ---- Acks after a win ----

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

// A member that missed the certificate, or whose admission lagged its
// configuration, is repaired by the leader's next ack.
#[test]
fn the_winners_ack_repairs_the_admission_of_a_node_that_missed_the_certificate() {
    let clock = FakeClock::new();
    let (me, winner) = (worker("w2"), worker("w1"));
    let founded = founded_in(1, 3);
    let mut lost_certificate = stale_voter(&clock, &me);
    answer_and_grant(&mut lost_certificate, &winner, 1);
    let mut lagging_admission = stale_node(
        &clock,
        &me,
        KnownConfiguration {
            configuration: founded.clone(),
            admission: Some(g0()),
        },
    );

    for node in [&mut lost_certificate, &mut lagging_admission] {
        deliver(
            node,
            &winner,
            ack_message(ack_admitting(&winner, &founded, Some(g0()))),
        );

        assert_eq!(node.configuration(), Some(&founded));
        assert_eq!(node.admission(), Some(founded.generation()));
        assert_eq!(node.prior_admission(), Some(g0()));
    }
}
