//! The initiator's side of a roll call (ADR-0001 decisions 3 to 6): the
//! census a worker that suspects its leader publishes to its shard, and the
//! replies it collects.
//!
//! A roll call contests one term and is counted against the configuration
//! the initiator knows. Every worker that answers it is a respondent, the
//! initiator included. A respondent whose admission generations make it a
//! voter in that configuration, on both sides of a joint one, is a
//! returning voter; any other respondent, a pending member among them, is a
//! new voter. A roll call collects
//! replies until its deadline; if the returning voters are a quorum of the
//! configuration by then, the initiator stands as the candidate.

use std::collections::BTreeMap;

use crate::configuration::{Admission, Configuration, Tally};
use crate::protocol::checked::Checked;
use crate::protocol::ids::{ShardId, WorkerId};
use crate::protocol::messages::RollCall;
use crate::protocol::messages::prelude::*;
use crate::time::Instant;

/// How a roll call ranks against the other calls for its term: the lower
/// the better. Calls are ordered by the initiator's wall-clock timestamp,
/// then by the initiator's `WorkerId` (ADR-0001 decision 5). The timestamp
/// only breaks ties, so clocks that disagree bias who wins a tie but never
/// let two calls rank equal.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CallRank {
    // The derived `Ord` compares fields in declaration order, so this order
    // is the ranking order.
    timestamp_millis: u64,
    initiator: WorkerId,
}

impl CallRank {
    pub(crate) fn of(call: &Checked<RollCall>) -> Self {
        CallRank {
            timestamp_millis: call.timestamp_millis,
            initiator: call.initiator_id(),
        }
    }

    pub(crate) fn initiator(&self) -> &WorkerId {
        &self.initiator
    }
}

/// One roll call this node started: what it publishes, and who has
/// answered so far with which admission generation.
#[derive(Debug, Clone)]
pub(crate) struct RollCallRound {
    term: u64,
    configuration: Configuration,
    rank: CallRank,
    respondents: BTreeMap<WorkerId, Admission>,
    abandoned: bool,
    deadline: Instant,
}

impl RollCallRound {
    /// A roll call for `term` under `configuration`, started by `initiator`
    /// at `timestamp_millis` on its wall clock, that runs until `deadline`
    /// on its monotonic one. The initiator is its first respondent, at its
    /// own `admission`.
    pub(crate) fn start(
        term: u64,
        configuration: Configuration,
        timestamp_millis: u64,
        initiator: WorkerId,
        admission: Admission,
        deadline: Instant,
    ) -> Self {
        RollCallRound {
            term,
            configuration,
            respondents: BTreeMap::from([(initiator.clone(), admission)]),
            rank: CallRank {
                timestamp_millis,
                initiator,
            },
            abandoned: false,
            deadline,
        }
    }

    /// When the initiator decides on the call.
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    pub(crate) fn term(&self) -> u64 {
        self.term
    }

    pub(crate) fn configuration(&self) -> &Configuration {
        &self.configuration
    }

    pub(crate) fn rank(&self) -> &CallRank {
        &self.rank
    }

    /// The message that publishes this call to `shard_id`.
    pub(crate) fn call(&self, shard_id: &ShardId) -> RollCall {
        RollCall {
            shard_id: Some(shard_id.clone().into()),
            term: self.term,
            configuration: Some((&self.configuration).into()),
            timestamp_millis: self.rank.timestamp_millis,
            initiator_id: Some(self.rank.initiator.clone().into()),
            initiator_address: String::new(),
        }
    }

    /// Records a reply from `respondent`, admitted at `admission`. Returns
    /// whether it is a new respondent: a worker that answers twice counts
    /// once, with the admission it first gave, and an abandoned call records
    /// no one.
    pub(crate) fn record(&mut self, respondent: WorkerId, admission: Admission) -> bool {
        if self.abandoned || self.respondents.contains_key(&respondent) {
            return false;
        }
        self.respondents.insert(respondent, admission);
        true
    }

    /// Gives this call up for a better one for the same term: it collects no
    /// more replies, and its initiator never stands as its candidate.
    pub(crate) fn abandon(&mut self) {
        self.abandoned = true;
    }

    pub(crate) fn is_abandoned(&self) -> bool {
        self.abandoned
    }

    /// Every respondent so far, the initiator included, with the admission
    /// it answered with.
    pub(crate) fn respondents(&self) -> &BTreeMap<WorkerId, Admission> {
        &self.respondents
    }

    /// Whether the returning voters among the respondents are a quorum of
    /// the call's configuration.
    pub(crate) fn has_returning_quorum(&self) -> bool {
        let mut tally = Tally::against(&self.configuration);
        for (respondent, admission) in &self.respondents {
            tally.record(respondent.clone(), *admission);
        }
        tally.has_quorum()
    }
}
