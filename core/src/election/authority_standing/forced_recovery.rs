//! The authority path: what a node with a
//! coordination authority does once a roll call of its own has closed
//! without its returning quorum.
//!
//! It asks the authority for the shard's live registrations, and counts the
//! call's respondents that are live there, itself included, against the
//! number of live registrations: a majority of them lets it go on. It then
//! reads the recovery epoch, swaps it for the next one, and waits out the
//! recovery fence before it leads a configuration founded at that new
//! epoch. Each step is one authority call; the node's driver performs it
//! and hands back the reply, which moves the recovery on or ends it.
//!
//! The swap is what lets only one worker recover the shard from a given
//! epoch, and the fence is what keeps a leader of the old epoch and the
//! new leader from acting at the same time. A respondent count that is
//! still warming up is never trusted: until every live worker has had a
//! TTL to register, a handful of registrations could pass for the whole
//! shard.

use std::collections::BTreeMap;

use crate::configuration::{Admission, Configuration, Generation, Roster, Single, Tally};
use crate::coordination_authority::{LiveRegistrations, RecoveryEpoch};
use crate::election::standing::{EpochOrder, order};
use crate::protocol::ids::WorkerId;

/// One attempt at the authority path, from the census of the roll call that
/// fell short.
#[derive(Debug, Clone)]
pub(crate) struct ForcedRecovery {
    term: u64,
    roll_call_configuration: Configuration,
    respondents: BTreeMap<WorkerId, Admission>,
    phase: Phase,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    ReadingLiveSet,
    ReadingEpoch { counted: Vec<WorkerId> },
    Swapping {
        counted: Vec<WorkerId>,
        from: RecoveryEpoch,
        to: RecoveryEpoch,
    },
    AwaitingFence {
        counted: Vec<WorkerId>,
        epoch: RecoveryEpoch,
    },
}

/// What the recovery asks of its node after a reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Next {
    /// Read the shard's recovery epoch.
    ReadEpoch,
    /// Swap the recovery epoch from `from` to `to`.
    Swap {
        from: RecoveryEpoch,
        to: RecoveryEpoch,
    },
    /// The swap succeeded: adopt `epoch`, and wait out its fence.
    AwaitFence { epoch: RecoveryEpoch },
    /// The shard's recovery epoch is gone: the shard is abandoned.
    Abandon,
    /// The authority holds this epoch, which the node cannot recover from:
    /// it rejoins the shard at it, as a fenced node that reconnects to find
    /// another epoch does.
    Rejoin(RecoveryEpoch),
    /// This attempt is over; the node stays `NoQuorum` and tries again at
    /// its next roll call.
    GiveUp,
}

/// Whether a node at `own_epoch` (`None` if it never learned its lineage)
/// can recover its shard from `held`, the epoch the authority holds, or
/// must rejoin the shard at it: `held` is of another lineage (founded
/// afresh), or lower in its own (lost data, or put back by a republish),
/// so a recovery from it would swap an epoch number its shard may already
/// have used. A node that never learned its lineage cannot tell.
pub(crate) fn cannot_recover_from(
    own_epoch: Option<RecoveryEpoch>,
    held: RecoveryEpoch,
) -> bool {
    match own_epoch {
        None => true,
        Some(own) => {
            own.lineage != held.lineage || order(&own, held.into()) == EpochOrder::Stale
        }
    }
}

impl ForcedRecovery {
    /// Starts the authority path for the roll call of `term`, counted
    /// against `roll_call_configuration`, which drew `respondents` (its
    /// initiator among them).
    pub(crate) fn start(
        term: u64,
        roll_call_configuration: Configuration,
        respondents: BTreeMap<WorkerId, Admission>,
    ) -> Self {
        ForcedRecovery {
            term,
            roll_call_configuration,
            respondents,
            phase: Phase::ReadingLiveSet,
        }
    }

    pub(crate) fn term(&self) -> u64 {
        self.term
    }

    pub(crate) fn is_awaiting_fence(&self) -> bool {
        matches!(self.phase, Phase::AwaitingFence { .. })
    }

    /// The live set came back. Goes on to read the epoch when the
    /// respondents live there are a majority of an authoritative count.
    pub(crate) fn on_live_registrations(&mut self, live: &LiveRegistrations) -> Next {
        if self.phase != Phase::ReadingLiveSet {
            return Next::GiveUp;
        }
        let Some(live_count) = live.authoritative_count() else {
            return Next::GiveUp;
        };
        let counted: Vec<WorkerId> = self
            .respondents
            .keys()
            .filter(|respondent| live.addresses().contains_key(*respondent))
            .cloned()
            .collect();
        let mut tally = Tally::against_count(live_count);
        for respondent in &counted {
            tally.record(respondent.clone(), None);
        }
        if !tally.has_quorum() {
            return Next::GiveUp;
        }
        self.phase = Phase::ReadingEpoch { counted };
        Next::ReadEpoch
    }

    /// The epoch came back. The swap starts from the authority's epoch
    /// whenever it is at or above `own_epoch`, so a shard some other worker
    /// already moved on, or left at a swapped epoch with no leader, is
    /// recovered from where it is, into the next epoch of its lineage. An
    /// epoch numbered below the node's own in its lineage (data lost, or
    /// put back by a leader that republished it after a flush), or one of
    /// another lineage (a shard founded afresh), is one this node's shard
    /// cannot recover into: a recovery from it would swap an epoch number
    /// this shard may already have used. The node rejoins the shard at it
    /// instead, as a fenced node that reconnects to another epoch does
    /// (see `authority_standing::authority_lease::Reconnect`), rather than stay `NoQuorum`
    /// beside it for good. A node that never learned its lineage cannot tell
    /// its own epoch from another, and rejoins too. An epoch at `u64::MAX`
    /// cannot be swapped.
    pub(crate) fn on_recovery_epoch(
        &mut self,
        epoch: Option<RecoveryEpoch>,
        own_epoch: Option<RecoveryEpoch>,
    ) -> Next {
        let Phase::ReadingEpoch { counted } = &self.phase else {
            return Next::GiveUp;
        };
        let Some(from) = epoch else {
            return Next::Abandon;
        };
        if cannot_recover_from(own_epoch, from) {
            return Next::Rejoin(from);
        }
        let Some(to) = from.next() else {
            return Next::GiveUp;
        };
        self.phase = Phase::Swapping {
            counted: counted.clone(),
            from,
            to,
        };
        Next::Swap { from, to }
    }

    /// The swap from `expected` to `new` came back.
    pub(crate) fn on_swapped(
        &mut self,
        expected: Option<RecoveryEpoch>,
        new: RecoveryEpoch,
        succeeded: bool,
    ) -> Next {
        let Phase::Swapping { counted, from, to } = &self.phase else {
            return Next::GiveUp;
        };
        let asked_for_this_swap = expected
            .is_some_and(|expected| order(from, expected.into()) == EpochOrder::Mine)
            && order(to, new.into()) == EpochOrder::Mine;
        if !asked_for_this_swap || !succeeded {
            return Next::GiveUp;
        }
        let epoch = *to;
        self.phase = Phase::AwaitingFence {
            counted: counted.clone(),
            epoch,
        };
        Next::AwaitFence { epoch }
    }

    /// The configuration this recovery founds at `epoch`, and the roster of
    /// its leader: one voter per counted respondent, every one admitted at
    /// the founded generation; every other respondent is a pending member.
    ///
    /// The generation is (`epoch`, the roll call's term, the roll call
    /// configuration's counter + 1). It leads the old epoch's every
    /// generation, so it needs no joint configuration: no worker at the new
    /// epoch counts a quorum of the old one.
    pub(crate) fn founded_roster(&self) -> Option<Roster> {
        let Phase::AwaitingFence { counted, epoch } = &self.phase else {
            return None;
        };
        let founded = Generation::founded_by_election(
            epoch.number,
            self.term,
            self.roll_call_configuration.generation(),
        );
        let members = counted
            .iter()
            .map(|respondent| (respondent.clone(), founded))
            .collect();
        let pending = self
            .respondents
            .keys()
            .filter(|respondent| !counted.contains(respondent))
            .cloned()
            .collect();
        Some(Roster::new(
            Configuration::single(Single {
                generation: founded,
                base: founded,
                voter_count: counted.len(),
            })
            .expect("a forced recovery's configuration has a voter, and its base is its generation"),
            members,
            pending,
        ))
    }
}
