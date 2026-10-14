//! The roll calls and vote requests of other workers this node answers or
//! refuses.

use super::ballot::{RollCallVerdict, VoteVerdict, Voter};
use super::roll_call::CallRank;
use super::{ElectionRound, Verdict, View};
use crate::configuration::Configuration;
use crate::coordination_authority::RecoveryEpoch;
use crate::election::standing::{EpochOrder, order};
use crate::protocol::checked::Checked;
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{
    ElectionRejectReason, RollCall, RollCallReply, VoteGrant, VoteRequest,
};
use crate::time::Instant;

impl ElectionRound {
    /// Decides on a roll call published by `initiator` (see the `ballot`
    /// module for the rules): answers it with the node's admission
    /// generations, refuses it with the reason, or passes over a repeat of
    /// a call it answered. A call for another shard, or its own, is
    /// dropped. A call it answers is electing someone, so the node starts
    /// no roll call of its own until that call resolves (see
    /// [`Self::roll_call_due`]): a repeat of it, passed over, changes
    /// nothing, and answering a better call moves the hold to that answer,
    /// within the limits given at [`Self::roll_call_due`].
    ///
    /// An initiator that answers a better call for its own term abandons
    /// its own call for it: it stays `RollCall` as that call's respondent,
    /// and never stands as its own call's candidate.
    pub(crate) fn on_roll_call(
        &mut self,
        view: &View,
        initiator: WorkerId,
        call: &Checked<RollCall>,
        now: Instant,
    ) -> Vec<Verdict> {
        if call.shard_id() != *view.shard || initiator == *view.me {
            return Vec::new();
        }
        let voter = self.voter(view);
        let verdict = self.ballot.on_roll_call(
            &voter,
            call.term,
            call.configuration().generation(),
            CallRank::of(call),
        );
        match verdict {
            RollCallVerdict::Answer => self.answer_roll_call(view, initiator, call, now),
            RollCallVerdict::Reject(reason) => vec![refusal_of(view, initiator, call, reason)],
            RollCallVerdict::Pass | RollCallVerdict::Drop => Vec::new(),
        }
    }

    /// Answers `initiator`'s roll call: the answer holds this node's own roll
    /// calls back, and an own call for the same term is abandoned to it.
    fn answer_roll_call(
        &mut self,
        view: &View,
        initiator: WorkerId,
        call: &Checked<RollCall>,
        now: Instant,
    ) -> Vec<Verdict> {
        self.hold
            .answered_at(now, view.roll_call_deadline, view.suspect_timeout);
        if let Some(own) = self.roll_call.as_mut()
            && own.term() == call.term
        {
            own.abandon();
        }
        let reply = reply_to(view, &initiator, call.term);
        vec![Verdict::Answer { initiator, reply }]
    }

    /// Decides on `candidate`'s request for this node's vote
    /// (see the `ballot` module for the rules). A request
    /// for another shard is ignored; every refusal is answered with the
    /// reason.
    pub(crate) fn on_vote_request(
        &mut self,
        view: &View,
        candidate: WorkerId,
        req: &Checked<VoteRequest>,
    ) -> Vec<Verdict> {
        if req.shard_id() != *view.shard {
            return Vec::new();
        }
        let voter = self.voter(view);
        let verdict = self.ballot.on_vote_request(
            &voter,
            RecoveryEpoch::new(req.recovery_epoch, req.recovery_epoch_lineage),
            req.term,
            req.roll_call_generation(),
            &candidate,
        );
        match verdict {
            VoteVerdict::Grant => {
                let grant = grant_to(view, &candidate, req.term);
                vec![Verdict::Grant { candidate, grant }]
            }
            VoteVerdict::Reject(reason) => vec![Verdict::Reject {
                to: candidate,
                term: req.term,
                reason,
                name_leader: reason == ElectionRejectReason::LeaderStillValid,
            }],
        }
    }

    /// What the ballot needs to know of the node to decide on a roll call
    /// or a vote request.
    fn voter(&self, view: &View) -> Voter {
        Voter {
            takes_part: view.takes_part,
            recovery_epoch: view.recovery_epoch,
            // Its own candidacies count here though they raise no term
            // seen: it votes in no term at or below one it stood in.
            highest_term_seen: view
                .highest_term_seen
                .max(self.ballot.highest_granted_term().unwrap_or(0)),
            configuration_generation: view.configuration.map(Configuration::generation),
            leader_contact_is_fresh: view.leader_contact_is_fresh,
        }
    }
}

/// What this node answers `initiator`'s roll call for `term` with.
fn reply_to(view: &View, initiator: &WorkerId, term: u64) -> RollCallReply {
    RollCallReply {
        shard_id: Some(view.shard.clone().into()),
        term,
        initiator_id: Some(initiator.clone().into()),
        responder_id: Some(view.me.clone().into()),
        responder_address: String::new(),
        admission: view.admission.current.map(Into::into),
        prior_admission: view.admission.prior.map(Into::into),
        configuration_generation: view
            .configuration
            .map(|configuration| configuration.generation().into()),
    }
}

/// This node's refusal of `initiator`'s roll call, for `reason`.
fn refusal_of(
    view: &View,
    initiator: WorkerId,
    call: &Checked<RollCall>,
    reason: ElectionRejectReason,
) -> Verdict {
    // A caller left on an earlier recovery epoch learns who leads
    // the current one, whose ack then moves it on.
    let name_leader = reason == ElectionRejectReason::LeaderStillValid
        || view
            .recovery_epoch
            .map(|own| order(own, call.configuration().generation().recovery_epoch()))
            == Some(EpochOrder::Stale);
    Verdict::Reject {
        to: initiator,
        term: call.term,
        reason,
        name_leader,
    }
}

/// The vote this node grants `candidate` for `term`.
fn grant_to(view: &View, candidate: &WorkerId, term: u64) -> VoteGrant {
    VoteGrant {
        shard_id: Some(view.shard.clone().into()),
        recovery_epoch: view.epoch_number(),
        recovery_epoch_lineage: view.epoch().lineage,
        term,
        candidate_id: Some(candidate.clone().into()),
        voter_id: Some(view.me.clone().into()),
    }
}
