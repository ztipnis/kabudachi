use crate::configuration::Configuration;
use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::checked::Checked;
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{
    ElectionCertificate, ElectionReject, ElectionRejectReason, KnownLeader, election_message,
};
use crate::protocol::worker_state::WorkerState;
use crate::time::Clock;

use super::super::{EpochOrder, WorkerNode};

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Refuses `initiator`'s roll call or vote request for `term`, naming
    /// this node's highest term seen, its configuration, and the leader it
    /// follows, if any, when `name_leader` asks for it, when this node leads,
    /// or when its contact with that leader is fresh: a node refused a roll
    /// call for any reason learns who leads, or one cut off from its leader's
    /// acks while another is elected never finds it again. A candidate refused
    /// a vote ignores the name.
    pub(super) fn send_reject(
        &mut self,
        initiator: WorkerId,
        term: u64,
        reason: ElectionRejectReason,
        name_leader: bool,
    ) {
        let leader = self
            .leader
            .as_ref()
            .filter(|_| {
                name_leader || self.holds_office() || self.current_leader_still_valid()
            })
            .map(|(leader, term)| KnownLeader {
                leader_id: Some(leader.clone().into()),
                term: *term,
            });
        let reject = ElectionReject {
            shard_id: Some(self.shard_id.clone().into()),
            term,
            initiator_id: Some(initiator.clone().into()),
            rejecter_id: Some(self.my_id.clone().into()),
            reason: reason as i32,
            // Not the vote floor the ballot refused against: a term this node
            // only stood in, unwon, would lock the initiator out of the
            // leader that outlasted it, as it once locked this node out.
            highest_term_seen: self.standing.highest_term_seen(),
            leader,
            configuration: self.led_or_followed_configuration().map(Into::into),
            recovery_epoch: self.standing.epoch().map(|epoch| epoch.number),
            recovery_epoch_lineage: self.standing.epoch().map_or(0, |epoch| epoch.lineage),
        };
        self.send(initiator, election_message::Payload::ElectionReject(reject));
    }

    /// Learns what a refusal of this node's roll call or vote request tells
    /// it: a higher term raises its highest term seen,
    /// and while `RollCall` a leader named at a term no older than that, or
    /// named as still valid at any term, becomes its leader, which it
    /// heartbeats until that leader's ack returns it to `Active` (or, if
    /// that leader's term is below its own term seen, until the heartbeat
    /// makes that leader step down). A refusal carrying the commit of the
    /// joint configuration this node holds, or a batch started on its single
    /// one, hands it that configuration (see
    /// [`Self::adopt_relayed_configuration`]). A term later than the one it
    /// holds or contests makes it step down (see
    /// [`Self::step_down_if_outpaced`]).
    /// A refusal from a newer epoch raises nothing (the two epochs' terms do
    /// not compare) but, while `RollCall`, makes the leader it names this
    /// node's, whose ack then moves it to that epoch. A refusal from an older
    /// epoch is dropped (see [`EpochOrder`]). A refusal names its refuser's
    /// epoch and lineage itself; one that names no epoch (its refuser has
    /// joined no shard) is read as this node's own epoch's. The refusal
    /// itself is not counted.
    pub(super) fn on_election_reject(&mut self, reject: &Checked<ElectionReject>) {
        if reject.shard_id() != self.shard_id || reject.initiator_id() != self.my_id {
            return;
        }
        let offered = reject.configuration();
        let refuser_epoch = reject
            .recovery_epoch
            .map(|number| RecoveryEpoch::new(number, reject.recovery_epoch_lineage));
        match refuser_epoch.and_then(|heard| self.standing.order(heard)) {
            // A newer epoch's terms are not this epoch's, so they raise
            // nothing here; the named leader's ack moves this node.
            Some(EpochOrder::Later) => {
                if let Some(leader) = self.leader_named_by(reject, true) {
                    self.leader = Some(leader);
                }
                return;
            }
            // An older epoch's terms, leader and configuration mean nothing
            // here, and its configuration must not be relayed.
            Some(EpochOrder::Stale) => return,
            Some(EpochOrder::Mine) | None => {}
        }
        self.standing.saw_term(reject.highest_term_seen);
        // A leader named as still valid is heartbeated whatever its term:
        // if this node's term seen is above that leader's, its acks stay
        // ignored, but the heartbeat tells it of the later term, and it
        // steps down (see `Self::on_heartbeat`).
        if let Some(leader) = self.leader_named_by(reject, false) {
            self.leader = Some(leader);
        }
        if let Some(offered) = offered {
            self.adopt_relayed_configuration(offered);
        }
        self.step_down_if_outpaced();
    }

    /// The leader `reject` names for this node to follow while it is
    /// `RollCall`: never itself, and, unless `any_term`, only one named at a
    /// term no older than the highest this node has seen, or named as still
    /// valid.
    fn leader_named_by(
        &self,
        reject: &Checked<ElectionReject>,
        any_term: bool,
    ) -> Option<(WorkerId, u64)> {
        if self.state != WorkerState::RollCall {
            return None;
        }
        let (leader, term) = reject.named_leader()?;
        let current = any_term
            || term >= self.standing.highest_term_seen()
            || reject.reason() == ElectionRejectReason::LeaderStillValid;
        (current && leader != self.my_id).then_some((leader, term))
    }

    /// Adopts `offered`, a refuser's configuration, when it is the commit of
    /// the joint configuration this node holds, or the batch its leader
    /// started on the single one it holds, and this node is a voter there, or
    /// the batch it was promised admission in, which it is no voter of in what
    /// it holds, only in the batch itself: the ack that carried it never reached this node, say because its
    /// leader stopped soon after (see
    /// [`ShardStanding::adopt_relayed_configuration`]). Without this, a
    /// survivor holding the older configuration and one holding the newer
    /// refuse each other's roll calls term after term. Only a node that takes
    /// part in elections without standing or leading adopts it.
    fn adopt_relayed_configuration(&mut self, offered: Configuration) {
        if self.takes_part_in_elections() {
            self.standing.adopt_relayed_configuration(offered);
        }
    }

    /// Accepts `leader`'s certificate of what its election's winner leads,
    /// sent to every respondent of its winning roll
    /// call: this node adopts that configuration and its admission
    /// generations there, as [`ShardStanding::adopt_certificate`] allows, and the
    /// certificate's term raises its highest term seen, which makes a node
    /// holding or contesting an earlier one step down (see
    /// [`Self::step_down_if_outpaced`]).
    ///
    /// It accepts a certificate for its own shard and recovery epoch that is
    /// from the leader it granted its vote in that term, or for a term no
    /// earlier than the highest it has seen: a respondent whose vote request
    /// never came granted nothing but is admitted too. A certificate for an
    /// earlier term from a leader it did not vote for is ignored, as is one
    /// reaching a node still joining, which answered no roll call.
    pub(super) fn on_election_certificate(
        &mut self,
        leader: &WorkerId,
        certificate: &Checked<ElectionCertificate>,
    ) {
        if !self.is_joined_to(&certificate.shard_id())
            || self.standing.order(RecoveryEpoch::new(
                certificate.recovery_epoch,
                certificate.recovery_epoch_lineage,
            )) != Some(EpochOrder::Mine)
        {
            return;
        }
        let voted_for_it = self.round.granted_in(certificate.term) == Some(leader);
        if certificate.term < self.ack_floor() && !voted_for_it {
            return;
        }
        if self.holds_office() && certificate.term > self.term {
            self.leave_office();
        }
        self.standing.adopt_certificate(
            certificate.term,
            certificate.configuration(),
            certificate.recipient_admission(),
            certificate.recipient_prior_admission(),
        );
        self.step_down_if_outpaced();
    }
}
