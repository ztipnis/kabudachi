use crate::protocol::checked::Checked;
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{TransferLeadership, election_message};
use crate::protocol::worker_state::WorkerState;
use crate::time::Clock;

use super::super::standing::EpochOrder;
use super::super::transfer::Transfer;
use super::super::{Output, WorkerNode};
use crate::coordination_authority::RecoveryEpoch;

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// A `Leader` starts a hand-over to its successor (see
    /// [`Input::TransferLeadership`](crate::election::Input::TransferLeadership)).
    pub(super) fn transfer_leadership(&mut self) {
        if self.state != WorkerState::Leader || self.transfer.is_some() {
            return;
        }
        let Some(to) = self
            .office
            .as_ref()
            .and_then(|office| office.successor(&self.my_id))
        else {
            return;
        };
        self.begin_transfer(to);
    }

    /// Hands leadership to `to`: from this step on [`Self::office`] is `None`,
    /// so the grant is withdrawn (reported in this step, and carried out
    /// before any message it sends) and no ack vouches for a heartbeat. Every
    /// connected peer is acked saying so, then `to` is asked to stand.
    fn begin_transfer(&mut self, to: WorkerId) {
        let now = self.clock.now();
        self.transfer = Some(Transfer {
            to: to.clone(),
            until: now + self.timings.suspect_timeout,
        });
        for peer in self.connected.clone() {
            self.send_ack(peer, None);
        }
        let asked = TransferLeadership {
            shard_id: Some(self.shard_id.clone().into()),
            leader_id: Some(self.my_id.clone().into()),
            target_id: Some(to.clone().into()),
            recovery_epoch: self.standing.epoch_number(),
            recovery_epoch_lineage: self.standing.epoch().map_or(0, |epoch| epoch.lineage),
            term: self.term,
        };
        self.send(to.clone(), election_message::Payload::TransferLeadership(asked));
        self.outputs.push(Output::TransferLeadership { to });
    }

    /// Ends a hand-over no successor took up in time: the node leads again.
    pub(super) fn end_transfer_if_due(&mut self) {
        let now = self.clock.now();
        if self
            .transfer
            .as_ref()
            .is_some_and(|transfer| now >= transfer.until)
        {
            self.transfer = None;
        }
    }

    /// The leader this node follows asks it to stand for election at once:
    /// an `Active` voter of the same shard and recovery epoch, that may
    /// stand, starts its roll call now, without waiting out a suspicion.
    pub(super) fn on_transfer_leadership(
        &mut self,
        from: &WorkerId,
        asked: &Checked<TransferLeadership>,
    ) {
        let named = RecoveryEpoch::new(asked.recovery_epoch, asked.recovery_epoch_lineage);
        if asked.shard_id() != self.shard_id
            || asked.target_id() != self.my_id
            || self.standing.order(named) != Some(EpochOrder::Mine)
            || self.leader.as_ref() != Some(&(from.clone(), asked.term))
            || self.state != WorkerState::Active
            || !self.may_stand()
        {
            return;
        }
        let now = self.clock.now();
        self.leader_draining = true;
        self.round.may_call_from(now);
        self.transition_to(WorkerState::LeaderSuspect);
        let timestamp_millis = self.clock.wall_clock_millis();
        self.decide(|round, view| round.begin_roll_call(view, timestamp_millis, now));
    }
}
