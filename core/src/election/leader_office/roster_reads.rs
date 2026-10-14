use std::collections::{BTreeMap, BTreeSet};

use crate::configuration::Tally;
use crate::protocol::ids::WorkerId;
use crate::queues::Subscription;
use crate::reconcile::Answered;

use super::LeaderOffice;

impl LeaderOffice {
    /// Whether every voter of the committed configuration other than `me`
    /// is known to the roster and has reported a routing crawl at the
    /// admission the roster counts it by now. Never while the configuration
    /// is joint.
    pub(crate) fn remaining_voters_have_crawled(&mut self, me: &WorkerId) -> bool {
        self.apply_pending_removals();
        let configuration = self.roster.configuration();
        let Some(voter_count) = configuration.voter_count() else {
            return false;
        };
        let others: Vec<&WorkerId> = self
            .roster
            .members()
            .keys()
            .filter(|worker| {
                *worker != me && configuration.is_voter(self.roster.counted_admission_of(worker))
            })
            .collect();
        let leader_votes =
            usize::from(configuration.is_voter(self.roster.counted_admission_of(me)));
        others.len() + leader_votes == voter_count
            && others.iter().all(|worker| {
                self.reports
                    .crawled_at(worker, self.roster.admission_of(worker))
            })
    }

    /// The members whose latest heartbeat said they run compaction.
    pub(crate) fn compaction_runners(&self) -> BTreeSet<WorkerId> {
        self.reports.compaction_runners(&self.roster)
    }

    /// The free places each member and pending joiner said it had in its
    /// latest heartbeat.
    pub(crate) fn member_capacities(&self) -> BTreeMap<WorkerId, u32> {
        self.reports.capacities(&self.roster)
    }

    /// The queues each member and pending member said in its latest heartbeat
    /// that it takes.
    pub(crate) fn subscriptions(&self) -> BTreeMap<WorkerId, Subscription> {
        self.reports.subscriptions(&self.roster)
    }

    /// The voters of the committed configuration this office knows by id,
    /// `me` included when it votes. A voter the roster does not know is not
    /// named.
    pub(crate) fn voter_ids(&self, me: &WorkerId) -> Vec<WorkerId> {
        let configuration = self.roster.configuration();
        self.roster
            .members()
            .keys()
            .chain(std::iter::once(me))
            .filter(|worker| configuration.is_voter(self.roster.counted_admission_of(worker)))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .cloned()
            .collect()
    }

    /// The voters of the committed configuration records may be placed on:
    /// those of [`Self::voter_ids`] that this office has not reported lost
    /// since it last heard them (`me` is never lost).
    pub(crate) fn placeable_voter_ids(&self, me: &WorkerId) -> Vec<WorkerId> {
        self.voter_ids(me)
            .into_iter()
            .filter(|voter| voter == me || !self.liveness.is_lost(voter))
            .collect()
    }

    /// Whether the roster holds `worker` as a voter of the committed
    /// configuration or as a pending member. Removals pending are not applied
    /// first: a worker that has just asked to leave may be counted once more,
    /// and nothing its claim decides outlives its removal.
    pub(crate) fn is_voter_or_pending(&self, worker: &WorkerId) -> bool {
        self.roster.pending().contains(worker)
            || self
                .roster
                .configuration()
                .is_voter(self.roster.counted_admission_of(worker))
    }

    /// Whom a reconciliation asks: the voters of the committed configuration
    /// (either side of a joint one), `me` among them, then the pending
    /// members. Read from the roster alone.
    pub(crate) fn reconcilees(&self, me: &WorkerId) -> Vec<WorkerId> {
        let mut asked = self.voter_ids(me);
        asked.extend(
            self.roster
                .pending()
                .iter()
                .filter(|worker| !asked.contains(*worker))
                .cloned()
                .collect::<Vec<_>>(),
        );
        asked
    }

    /// What `answered` amounts to among the voters: all of them (of both
    /// sides of a joint configuration), a quorum, or short of one. Pending
    /// members and workers the roster does not hold count for nothing; `me`
    /// counts as the lease counts it, whether or not the roster lists it.
    pub(crate) fn voters_answered(&self, me: &WorkerId, answered: &BTreeSet<WorkerId>) -> Answered {
        let mut tally = Tally::against(self.roster.configuration());
        for worker in answered
            .iter()
            .filter(|worker| *worker == me || self.roster.members().contains_key(*worker))
        {
            tally.record(worker.clone(), self.roster.counted_admission_of(worker));
        }
        if tally.is_unanimous() {
            Answered::All
        } else if tally.has_quorum() {
            Answered::Quorum
        } else {
            Answered::Short
        }
    }

    /// Whether the roster holds `worker`, admitted or pending. Removals
    /// pending are not applied first, as for [`Self::is_voter_or_pending`].
    pub(crate) fn is_member(&self, worker: &WorkerId) -> bool {
        self.roster.holds(worker)
    }
}
