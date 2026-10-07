//! A worker answers a reconcile request only from the leader it follows or a
//! requester that proves an office with an election certificate.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::reconcile::ReconcileTerm;

use crate::support::deadline::within_deadline;
use crate::support::records::{ThreeVoters, proof_of_office};

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_refuses_a_requester_that_is_not_its_leader_unless_its_certificate_is_current() {
    within_deadline(async {
        let (mut shard, claimant) = ThreeVoters::start().await;
        let leader = shard.drive_until_a_leader().await;
        let worker = shard.id(shard.others(leader)[0]);
        let outsider = claimant.local_worker_id();
        let ask = |term: u64| {
            let term_of = ReconcileTerm {
                recovery_epoch: RecoveryEpoch::new(0, 0),
                term,
            };
            claimant.ask_reconcile(
                worker.clone(),
                term_of,
                proof_of_office(&outsider, term),
                None,
                false,
            )
        };

        let stale = shard.drive_until(ask(0)).await;
        assert!(
            stale.is_none(),
            "a certificate of a term the worker has passed gets no answer"
        );

        let new_leader = shard.drive_until(ask(1_000)).await;
        assert!(
            new_leader.is_some(),
            "a certificate of a later term is answered before the worker follows it"
        );
    })
    .await;
}
