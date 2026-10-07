//! The voter's side of an election: for each
//! term, the best roll call this node answered and the candidate it granted
//! its vote to. It is the one place that decides whether this node answers a
//! roll call or grants a vote.
//!
//! A node answers the first roll call it accepts for a term, and any later
//! one that ranks better (see [`CallRank`]); it refuses a worse one, so its
//! initiator learns why it is short. It grants at most one vote per
//! term, only to the initiator of the best call it answered, and never
//! switches a vote it granted. A node whose leader contact is fresh answers
//! and grants nothing (leader stickiness), and one that holds a newer
//! configuration than a call's refuses both the call and its candidate's
//! request.

use std::collections::BTreeMap;

use super::roll_call::CallRank;
use crate::configuration::Generation;
use crate::election::standing::{EpochOrder, order_numbers};
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::ElectionRejectReason;

/// This node's history as a voter.
///
/// The per-term history grows for the life of the node, one entry per term
/// it answered a call or voted in; bounding it is deferred.
#[derive(Debug, Clone, Default)]
pub(crate) struct Ballot {
    terms: BTreeMap<u64, TermBallot>,
    /// The highest term of any roll call this node accepted, its own
    /// included.
    highest_roll_call_term: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct TermBallot {
    best_answered: Option<CallRank>,
    granted: Option<WorkerId>,
}

/// What the ballot needs to know of the node deciding.
pub(crate) struct Voter {
    /// Whether the node's state takes part in elections at all.
    pub(crate) takes_part: bool,
    pub(crate) recovery_epoch: u64,
    pub(crate) highest_term_seen: u64,
    /// The generation of the node's configuration; `None` with none.
    pub(crate) configuration_generation: Option<Generation>,
    /// Whether the node heard from its leader within its suspicion timeout.
    pub(crate) leader_contact_is_fresh: bool,
}

/// What a node does with a roll call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RollCallVerdict {
    /// Reply to the initiator: this is now the best call it answered for
    /// the term.
    Answer,
    /// Refuse it, telling the initiator why: among other reasons, that it
    /// ranks below a call the node already answered for the term.
    Reject(ElectionRejectReason),
    /// It is a call the node already answered, arriving again: the node
    /// neither answers it twice nor refuses it.
    Pass,
    /// It is from a recovery epoch the node cannot move to: the node takes
    /// no notice of it.
    Drop,
}

/// What a node does with a vote request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VoteVerdict {
    Grant,
    Reject(ElectionRejectReason),
}

impl Ballot {
    /// Decides on a roll call for `term`, ranking `rank`, counted against a
    /// configuration at `configuration_generation`. Answering records the
    /// call as the node's best for the term, and never raises the node's
    /// highest term seen.
    ///
    /// A call the node accepts counts toward the highest roll-call term it
    /// has accepted (see [`Self::highest_roll_call_term`]); a call it
    /// refuses does not, except one ranking below a call it already
    /// answered for the same term, which is counted already.
    pub(crate) fn on_roll_call(
        &mut self,
        voter: &Voter,
        term: u64,
        configuration_generation: Generation,
        rank: CallRank,
    ) -> RollCallVerdict {
        let epoch = order_numbers(
            voter.recovery_epoch,
            configuration_generation.recovery_epoch(),
        );
        // A node cannot adopt a newer recovery epoch.
        if epoch == EpochOrder::Later {
            return RollCallVerdict::Drop;
        }
        if !voter.takes_part {
            return RollCallVerdict::Reject(ElectionRejectReason::NotEligible);
        }
        if epoch == EpochOrder::Stale {
            return RollCallVerdict::Reject(ElectionRejectReason::StaleGeneration);
        }
        if term <= voter.highest_term_seen {
            return RollCallVerdict::Reject(ElectionRejectReason::StaleTerm);
        }
        if voter
            .configuration_generation
            .is_some_and(|own| configuration_generation < own)
        {
            return RollCallVerdict::Reject(ElectionRejectReason::StaleGeneration);
        }
        if voter.leader_contact_is_fresh {
            return RollCallVerdict::Reject(ElectionRejectReason::LeaderStillValid);
        }

        self.accept_roll_call_term(term);
        let ballot = self.terms.entry(term).or_default();
        match &ballot.best_answered {
            Some(best) if *best == rank => return RollCallVerdict::Pass,
            Some(best) if *best < rank => {
                return RollCallVerdict::Reject(ElectionRejectReason::NotBestRollCall);
            }
            _ => {}
        }
        ballot.best_answered = Some(rank);
        RollCallVerdict::Answer
    }

    /// Decides on `candidate`'s request for this node's vote in `term`, at
    /// `recovery_epoch`, for a roll call run under a configuration at
    /// `roll_call_generation`. Granting records the vote; the caller raises
    /// its highest term seen to `term`.
    ///
    /// "Already voted" is checked before "stale term": granting raises the
    /// highest term seen to the request's term, so a repeat request for that
    /// term would otherwise always be refused as stale.
    ///
    /// A node that took on a newer configuration after answering the call
    /// refuses: the call counts it as a voter of a configuration it has
    /// moved past, so its vote must not help a candidate win under that one.
    pub(crate) fn on_vote_request(
        &mut self,
        voter: &Voter,
        recovery_epoch: u64,
        term: u64,
        roll_call_generation: Generation,
        candidate: &WorkerId,
    ) -> VoteVerdict {
        if !voter.takes_part {
            return VoteVerdict::Reject(ElectionRejectReason::NotEligible);
        }
        if order_numbers(voter.recovery_epoch, recovery_epoch) != EpochOrder::Mine {
            return VoteVerdict::Reject(ElectionRejectReason::WrongRecoveryEpoch);
        }
        // Read only: a refused request leaves no entry for its term.
        let ballot = self.terms.get(&term);
        // Applies even when the request comes from the candidate already
        // voted for.
        if ballot.is_some_and(|ballot| ballot.granted.is_some()) {
            return VoteVerdict::Reject(ElectionRejectReason::AlreadyVoted);
        }
        if term <= voter.highest_term_seen {
            return VoteVerdict::Reject(ElectionRejectReason::StaleTerm);
        }
        if voter
            .configuration_generation
            .is_some_and(|own| roll_call_generation < own)
        {
            return VoteVerdict::Reject(ElectionRejectReason::StaleGeneration);
        }
        if voter.leader_contact_is_fresh {
            return VoteVerdict::Reject(ElectionRejectReason::LeaderStillValid);
        }
        if ballot
            .and_then(|ballot| ballot.best_answered.as_ref())
            .is_none_or(|best| best.initiator() != candidate)
        {
            return VoteVerdict::Reject(ElectionRejectReason::NotBestRollCall);
        }
        self.terms.entry(term).or_default().granted = Some(candidate.clone());
        VoteVerdict::Grant
    }

    /// Records this node's own roll call for `term`, ranking `rank`, as the
    /// best it answered: the initiator is its own call's first respondent.
    pub(crate) fn record_own_roll_call(&mut self, term: u64, rank: CallRank) {
        self.accept_roll_call_term(term);
        self.terms.entry(term).or_default().best_answered = Some(rank);
    }

    /// The candidate this node granted its vote in `term`, if any.
    pub(crate) fn granted_in(&self, term: u64) -> Option<&WorkerId> {
        self.terms.get(&term)?.granted.as_ref()
    }

    /// Records the vote this node grants itself as the candidate in `term`.
    pub(crate) fn record_own_grant(&mut self, term: u64, me: &WorkerId) {
        self.terms.entry(term).or_default().granted = Some(me.clone());
    }

    /// The highest term in which this node granted a vote, itself as a
    /// candidate included; `None` before it grants one.
    pub(crate) fn highest_granted_term(&self) -> Option<u64> {
        self.terms
            .iter()
            .rev()
            .find_map(|(term, ballot)| ballot.granted.as_ref().map(|_| *term))
    }

    /// The highest term of any roll call this node accepted, its own
    /// included; `None` before it accepts one. Its next roll call contests
    /// a later term.
    pub(crate) fn highest_roll_call_term(&self) -> Option<u64> {
        self.highest_roll_call_term
    }

    fn accept_roll_call_term(&mut self, term: u64) {
        self.highest_roll_call_term = Some(
            self.highest_roll_call_term
                .map_or(term, |seen| seen.max(term)),
        );
    }
}
