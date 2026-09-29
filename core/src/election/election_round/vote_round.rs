//! The initiator's side of a vote (ADR-0001 decisions 6 and 7): once its
//! roll call stands it as the candidate, the initiator asks the call's
//! respondents for their votes and counts the grants.
//!
//! The vote runs on the census its roll call took, until its own deadline.
//! A respondent whose reply arrives after the initiator stood joins the
//! census and is asked for its vote too. Only a respondent's grant counts,
//! each at the admission generation it answered the roll call with, and the
//! initiator grants itself its own vote first. A respondent that replies
//! late also raises the number of respondents a win needs a majority of.

use std::collections::BTreeSet;

use super::roll_call::RollCallRound;
use crate::configuration::{Admission, Tally};
use crate::protocol::ids::{ShardId, WorkerId};
use crate::protocol::messages::VoteRequest;
use crate::time::Instant;

/// The vote a candidate is running: the census it stands on and the
/// respondents that granted it so far.
#[derive(Debug, Clone)]
pub(crate) struct VoteRound {
    census: RollCallRound,
    grants: BTreeSet<WorkerId>,
    deadline: Instant,
}

impl VoteRound {
    /// Stands the initiator of `census` as the candidate for its term, with
    /// its own vote as the first grant, until `deadline`.
    pub(crate) fn stand(census: RollCallRound, deadline: Instant) -> Self {
        let candidate = census.rank().initiator().clone();
        VoteRound {
            census,
            grants: BTreeSet::from([candidate]),
            deadline,
        }
    }

    /// When the candidate gives up if it has not won.
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    pub(crate) fn term(&self) -> u64 {
        self.census.term()
    }

    /// The roll call the candidate stands on, with every respondent so far.
    pub(crate) fn census(&self) -> &RollCallRound {
        &self.census
    }

    /// Every respondent but the candidate itself: the workers it asks for
    /// their votes when it stands.
    pub(crate) fn voters_to_ask(&self) -> Vec<WorkerId> {
        let candidate = self.census.rank().initiator();
        self.census
            .respondents()
            .keys()
            .filter(|respondent| *respondent != candidate)
            .cloned()
            .collect()
    }

    /// Records a reply to the roll call that arrived after the candidate
    /// stood. Returns whether `respondent` is new, and so is to be asked for
    /// its vote.
    pub(crate) fn record_respondent(&mut self, respondent: WorkerId, admission: Admission) -> bool {
        self.census.record(respondent, admission)
    }

    /// Records `voter`'s grant. A worker that did not answer the roll call
    /// grants nothing that counts, and is ignored.
    pub(crate) fn record_grant(&mut self, voter: WorkerId) {
        if self.census.respondents().contains_key(&voter) {
            self.grants.insert(voter);
        }
    }

    /// Whether the candidate has won (ADR-0001 decision 7): the returning
    /// voters among the granting workers are a quorum of the roll call's
    /// configuration, on both sides of a joint one, and the granting
    /// workers, new voters included, are a majority of every respondent so
    /// far, the candidate included.
    pub(crate) fn has_won(&self) -> bool {
        let respondents = self.census.respondents();
        let mut tally = Tally::against(self.census.configuration())
            .and(Tally::against_count(respondents.len()));
        for (granter, admission) in respondents
            .iter()
            .filter(|(respondent, _)| self.grants.contains(*respondent))
        {
            tally.record(granter.clone(), *admission);
        }
        tally.has_quorum()
    }

    /// The message that asks a respondent for its vote in `shard_id` at
    /// `recovery_epoch`.
    pub(crate) fn request(&self, shard_id: &ShardId, recovery_epoch: u64) -> VoteRequest {
        VoteRequest {
            shard_id: Some(shard_id.clone().into()),
            recovery_epoch,
            term: self.census.term(),
            candidate_id: Some(self.census.rank().initiator().clone().into()),
            roll_call_generation: Some(self.census.configuration().generation().into()),
        }
    }
}
