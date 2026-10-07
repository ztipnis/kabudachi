//! Who a worker answers when a leader asks what it holds before it schedules:
//! the leader it follows, or a requester that proves a leader's office with
//! an election certificate for a term no earlier than the worker's highest.

use crate::support::builders::{
    ack_message, configuration_of, election_certificate_message, leader_ack, past_any_suspicion,
    roll_call, roll_call_message, shard, timings, vote_grant, vote_grant_message, vote_request,
    vote_request_message, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{TestNode, deliver, stand_as_candidate, voter_node};
use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{Entry, Identity, KnownConfiguration, WorkerNode};
use kabudachi_core::protocol::generated::ElectionCertificate;
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::time::Duration;

const SUSPECT: u64 = 10;

/// A node of three voters that follows `old` in term 1.
fn follower_of(clock: &FakeClock, me: &WorkerId, old: &WorkerId) -> TestNode {
    let mut node = voter_node(clock, me, 3, SUSPECT);
    deliver(
        &mut node,
        old,
        ack_message(leader_ack(
            old,
            1,
            &configuration_of(3),
            Some(Generation::genesis(0)),
        )),
    );
    node
}

/// The certificate `me` holds office on, having won the term after the latest
/// `me` knows of among `peers`, still reconciling.
fn proof_of_office(
    clock: &FakeClock,
    me: &WorkerId,
    followed: Option<&WorkerId>,
    peers: &[WorkerId],
) -> ElectionCertificate {
    let mut node = match followed {
        Some(old) => follower_of(clock, me, old),
        None => voter_node(clock, me, 3, SUSPECT),
    };
    let call = stand_as_candidate(&mut node, clock, SUSPECT, peers);
    for peer in peers {
        deliver(
            &mut node,
            peer,
            vote_grant_message(vote_grant(me.clone(), peer.clone(), call.term)),
        );
    }
    node.reconcile_proof()
        .expect("a leader that holds office has a proof of it")
}

struct Fixture {
    worker: TestNode,
    old: WorkerId,
    new: WorkerId,
    old_proof: ElectionCertificate,
    new_proof: ElectionCertificate,
}

/// A worker following `old`, which won term 1, while `new` has won term 2.
fn fixture() -> Fixture {
    let (w, old, new, other) = (worker("w"), worker("old"), worker("new"), worker("other"));
    let peers = [w.clone(), other.clone()];
    let old_proof = proof_of_office(&FakeClock::new(), &old, None, &peers);
    let new_proof = proof_of_office(&FakeClock::new(), &new, Some(&old), &peers);
    assert_eq!((old_proof.term, new_proof.term), (1, 2), "setup invariant");
    let clock = FakeClock::new();
    Fixture {
        worker: follower_of(&clock, &w, &old),
        old,
        new,
        old_proof,
        new_proof,
    }
}

#[test]
fn a_worker_answers_the_leader_it_follows_and_no_one_who_proves_nothing() {
    let f = fixture();
    assert!(f.worker.may_answer_reconcile(&f.old, None));
    assert!(!f.worker.may_answer_reconcile(&f.new, None));
    assert!(!f.worker.may_answer_reconcile(&worker("stranger"), None));
}

#[test]
fn a_deposed_leader_is_refused_once_the_worker_has_seen_a_later_term() {
    let mut f = fixture();
    // Before the worker follows it, the new leader is answered on its proof.
    assert!(f.worker.may_answer_reconcile(&f.new, Some(&f.new_proof)));
    deliver(
        &mut f.worker,
        &f.new,
        election_certificate_message(ElectionCertificate {
            recipient_admission: Some(Generation::genesis(0).into()),
            ..f.new_proof.clone()
        }),
    );
    assert!(!f.worker.may_answer_reconcile(&f.old, None));
    assert!(!f.worker.may_answer_reconcile(&f.old, Some(&f.old_proof)));
    assert!(f.worker.may_answer_reconcile(&f.new, Some(&f.new_proof)));
}

#[test]
fn a_certificate_for_an_earlier_term_than_the_worker_has_seen_is_refused() {
    let f = fixture();
    // A worker that follows no one, and granted its vote in term 2.
    let clock = FakeClock::new();
    let mut worker_of_term_2 = voter_node(&clock, &worker("v"), 3, SUSPECT);
    clock.advance(past_any_suspicion(SUSPECT));
    deliver(
        &mut worker_of_term_2,
        &f.new,
        roll_call_message(roll_call(&f.new, 2, &configuration_of(3), 0)),
    );
    deliver(
        &mut worker_of_term_2,
        &f.new,
        vote_request_message(vote_request(f.new.clone(), 0, 2)),
    );
    assert!(!worker_of_term_2.may_answer_reconcile(&f.old, Some(&f.old_proof)));
    assert!(worker_of_term_2.may_answer_reconcile(&f.new, Some(&f.new_proof)));
}

#[test]
fn a_certificate_presented_by_a_worker_it_does_not_name_is_refused() {
    let f = fixture();
    assert!(
        !f.worker
            .may_answer_reconcile(&worker("thief"), Some(&f.new_proof))
    );
}

#[test]
fn a_certificate_for_another_shard_is_refused() {
    let f = fixture();
    let elsewhere = ElectionCertificate {
        shard_id: Some(shard("shard-2").into()),
        ..f.new_proof.clone()
    };
    assert!(!f.worker.may_answer_reconcile(&f.new, Some(&elsewhere)));
}

#[test]
fn a_certificate_of_a_later_recovery_epoch_is_answered_and_of_an_earlier_one_refused() {
    let f = fixture();
    let later = Configuration::single(Single {
        generation: Generation::genesis(1),
        base: Generation::genesis(1),
        voter_count: 3,
    })
    .expect("valid");
    let of_epoch_1 = ElectionCertificate {
        recovery_epoch: 1,
        term: 1,
        configuration: Some((&later).into()),
        ..f.new_proof.clone()
    };
    // The worker, at epoch 0, has seen term 5: a later epoch's term does not compare.
    let clock = FakeClock::new();
    let mut at_epoch_0 = voter_node(&clock, &worker("w0"), 3, SUSPECT);
    deliver(
        &mut at_epoch_0,
        &f.old,
        ack_message(leader_ack(
            &f.old,
            5,
            &configuration_of(3),
            Some(Generation::genesis(0)),
        )),
    );
    assert!(at_epoch_0.may_answer_reconcile(&f.new, Some(&of_epoch_1)));

    // A worker at epoch 1 refuses epoch 0's certificate whatever its term.
    let at_epoch_1 = WorkerNode::start(
        Identity {
            id: worker("w1"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(SUSPECT)),
        },
        Entry::Known(KnownConfiguration {
            configuration: later,
            admission: Some(Generation::genesis(1)),
        }),
        clock.clone(),
        None,
    )
    .0;
    let of_epoch_0 = ElectionCertificate {
        term: 1_000,
        ..f.new_proof.clone()
    };
    assert!(!at_epoch_1.may_answer_reconcile(&f.new, Some(&of_epoch_0)));
}

#[test]
fn a_certificate_older_than_the_configuration_the_worker_holds_is_refused() {
    let f = fixture();
    // A worker restarted holding a configuration stamped in term 5, with no
    // term seen yet: a certificate for term 3 passes the term rule, so only
    // the configuration can refuse it.
    let held = Configuration::single(Single {
        generation: Generation::new(0, 5, 1),
        base: Generation::new(0, 5, 1),
        voter_count: 3,
    })
    .expect("valid");
    let clock = FakeClock::new();
    let node = WorkerNode::start(
        Identity {
            id: worker("w2"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(SUSPECT)),
        },
        Entry::Known(KnownConfiguration {
            configuration: held,
            admission: Some(Generation::new(0, 5, 1)),
        }),
        clock,
        None,
    )
    .0;
    assert_eq!(node.highest_term_seen(), 0, "setup invariant");
    let of_term_3 = ElectionCertificate {
        term: 3,
        ..f.new_proof.clone()
    };
    assert!(!node.may_answer_reconcile(&f.new, Some(&of_term_3)));
    let of_term_5 = ElectionCertificate {
        term: 5,
        ..f.new_proof.clone()
    };
    assert!(node.may_answer_reconcile(&f.new, Some(&of_term_5)));
}
