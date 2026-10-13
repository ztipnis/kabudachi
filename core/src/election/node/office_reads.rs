use super::super::EpochOrder;
use super::super::WorkerNode;
use super::super::lease::Office;
use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::checked::{CheckedMessage, CheckedPayload, decode};
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{ElectionCertificate, ElectionMessage, election_message};
use crate::protocol::worker_state::WorkerState;
use crate::queues::Subscription;
use crate::reconcile::{Answered, ReconcileTerm};
use crate::scheduler::LeaseEnd;
use crate::time::Clock;
use std::collections::{BTreeMap, BTreeSet};

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Whether it holds office: reconciling or leading.
    pub(super) fn holds_office(&self) -> bool {
        matches!(
            self.state,
            WorkerState::LeaderReconciling | WorkerState::Leader
        )
    }

    /// The voters of the configuration this node leads, by id, itself
    /// included; empty unless it holds office, reconciling or leading.
    /// Count-based configurations may hold fewer ids than voters (a voter the
    /// roster does not know is not named); only the leader's own roster is
    /// read, never the routing table.
    pub fn voters(&self) -> Vec<WorkerId> {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.voter_ids(&self.my_id),
            _ => Vec::new(),
        }
    }

    /// The voters this leader places Task records on: those of its
    /// configuration, minus every one it has reported lost and not heard
    /// from since (one reported for confirming no ack, until it confirms
    /// one). Empty unless it holds office. Read from its roster alone.
    pub fn placeable_voters(&self) -> Vec<WorkerId> {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.placeable_voter_ids(&self.my_id),
            _ => Vec::new(),
        }
    }

    /// Whether this node holds office and its roster holds `worker` as a
    /// voter of the configuration it leads or as a pending member. `false`
    /// for every worker while this node holds none. Only the leader's own
    /// roster is read, never the routing table.
    pub fn is_voter_or_pending(&self, worker: &WorkerId) -> bool {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.is_voter_or_pending(worker),
            _ => false,
        }
    }

    /// The office this node holds, reconciling or leading: its recovery epoch
    /// and term. `None` while it holds none.
    pub fn office_term(&self) -> Option<ReconcileTerm> {
        if !self.holds_office() {
            return None;
        }
        Some(ReconcileTerm {
            recovery_epoch: self.standing.epoch()?,
            term: self.term,
        })
    }

    /// The certificate this leader presents to a worker it asks what it holds
    /// (see [`Self::may_answer_reconcile`]): for the term and recovery epoch
    /// it took office in, the configuration it leads. `None` unless it holds
    /// office.
    pub fn reconcile_proof(&self) -> Option<ElectionCertificate> {
        let office = self.office.as_ref().filter(|_| self.holds_office())?;
        Some(ElectionCertificate {
            shard_id: Some(self.shard_id.clone().into()),
            recovery_epoch: self.standing.epoch_number(),
            recovery_epoch_lineage: self.standing.epoch().map_or(0, |epoch| epoch.lineage),
            term: self.term,
            leader_id: Some(self.my_id.clone().into()),
            configuration: Some(office.configuration().into()),
            recipient_admission: None,
            recipient_prior_admission: None,
        })
    }

    /// Whether this worker may answer `from`, which asks it what it holds
    /// before it schedules, presenting `proof`. It answers the leader it
    /// follows, and a requester that proves an office: a certificate it
    /// names itself as the leader of, for this node's shard, no earlier in
    /// term than the highest this node has seen and not stamped before the
    /// configuration this node holds (a later recovery epoch's terms do not
    /// compare with this node's, so one is accepted as a newer office). A
    /// requester that is neither, such as a deposed leader, is refused, so
    /// what a worker holds is told only to the office that may act on it.
    /// The certificate is checked as one received over the wire is. A node
    /// that never joined a shard answers no one.
    pub fn may_answer_reconcile(
        &self,
        from: &WorkerId,
        proof: Option<&ElectionCertificate>,
    ) -> bool {
        if self
            .known_leader()
            .is_some_and(|(leader, _)| &leader == from)
        {
            return true;
        }
        let Some(proof) = proof else {
            return false;
        };
        let wrapped = ElectionMessage {
            payload: Some(election_message::Payload::ElectionCertificate(
                proof.clone(),
            )),
        };
        let Some(CheckedPayload::ElectionCertificate(certificate)) =
            decode(wrapped).ok().and_then(CheckedMessage::into_payload)
        else {
            return false;
        };
        if certificate.leader_id() != *from || certificate.shard_id() != self.shard_id {
            return false;
        }
        let named = RecoveryEpoch::new(
            certificate.recovery_epoch,
            certificate.recovery_epoch_lineage,
        );
        match self.standing.order(named) {
            Some(EpochOrder::Later) => true,
            Some(EpochOrder::Stale) | None => false,
            Some(EpochOrder::Mine) => {
                certificate.term >= self.standing.highest_term_seen()
                    && self
                        .led_or_followed_configuration()
                        .is_none_or(|held| held.generation().term() <= certificate.term)
            }
        }
    }

    /// Whom its reconciliation asks: the voters of the configuration it leads
    /// and its pending members, by id, itself included. Empty unless it holds
    /// office. Read from its roster alone, never the routing table.
    pub fn reconcilees(&self) -> Vec<WorkerId> {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.reconcilees(&self.my_id),
            _ => Vec::new(),
        }
    }

    /// What `answered`, the workers that answered its reconciliation, amount
    /// to among its voters (both sides of a joint configuration): all, a
    /// quorum, or short of one. Pending members count for nothing. `Short`
    /// unless it holds office.
    pub fn voters_answered(&self, answered: &BTreeSet<WorkerId>) -> Answered {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.voters_answered(&self.my_id, answered),
            _ => Answered::Short,
        }
    }

    /// Whether its roster holds `worker`, admitted or pending. `false`
    /// unless it holds office.
    pub fn is_member(&self, worker: &WorkerId) -> bool {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.is_member(worker),
            _ => false,
        }
    }

    /// The members that said in their latest heartbeat that they run
    /// compaction, this node itself if it does. Empty unless it holds office:
    /// only the leader's own roster is read.
    pub fn compaction_runners(&self) -> BTreeSet<WorkerId> {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => {
                let mut runners = office.compaction_runners();
                if self.report.runs_compaction {
                    runners.insert(self.my_id.clone());
                }
                runners
            }
            _ => BTreeSet::new(),
        }
    }

    /// Each member's free places as its latest heartbeat said, this node's
    /// own included. Empty unless it holds office.
    pub fn member_capacities(&self) -> BTreeMap<WorkerId, u32> {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => {
                let mut capacities = office.member_capacities();
                capacities.insert(self.my_id.clone(), self.report.available_capacity);
                capacities
            }
            _ => BTreeMap::new(),
        }
    }

    /// The queues each member and pending member said in its latest heartbeat
    /// that it takes, this node's own included. Empty unless it holds office:
    /// only the leader's own roster is read.
    pub fn queue_subscriptions(&self) -> BTreeMap<WorkerId, Subscription> {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => {
                let mut subscriptions = office.subscriptions();
                subscriptions.insert(self.my_id.clone(), self.report.queues.clone());
                subscriptions
            }
            _ => BTreeMap::new(),
        }
    }

    /// What the lease reads of this node while it leads: `None` unless it
    /// is `Leader`, and, with an authority, holds the recovery fence (design
    /// 4.5), whose end also ends its grant.
    pub(super) fn office(&self) -> Option<Office<'_>> {
        if !self.holds_office() {
            return None;
        }
        let fence_end = match &self.authority {
            None => LeaseEnd::Unbounded,
            Some(authority) => LeaseEnd::At(authority.fence_valid_until()?),
        };
        Some(Office {
            me: &self.my_id,
            roster: self.office.as_ref()?.roster(),
            term: self.term,
            recovery_epoch: self.standing.epoch()?,
            fence_end,
        })
    }
}
