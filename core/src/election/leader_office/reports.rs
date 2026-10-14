use std::collections::{BTreeMap, BTreeSet};

use crate::configuration::{Generation, Roster};
use crate::protocol::ids::WorkerId;
use crate::queues::Subscription;

use super::MemberReport;

/// What each worker's latest heartbeat said of it: the admission it
/// reported a routing crawl at, whether it runs compaction, and its free
/// places.
#[derive(Default)]
pub(super) struct MemberReports {
    /// The workers whose latest heartbeat reported a routing crawl, with the
    /// admission each crawled at.
    crawled: BTreeMap<WorkerId, Generation>,
    /// The workers whose latest heartbeat said they run compaction.
    compaction_runners: BTreeSet<WorkerId>,
    /// Each worker's free places, by its latest heartbeat.
    capacities: BTreeMap<WorkerId, u32>,
    /// The queues each worker's latest heartbeat said it takes.
    subscriptions: BTreeMap<WorkerId, Subscription>,
}

impl MemberReports {
    /// Records `from`'s `report`. A crawl counts only when the admission it
    /// names is `counted_by`, the one the roster counts `from` by now, so a
    /// delayed heartbeat from before a re-admission does not count.
    pub(super) fn record(
        &mut self,
        from: &WorkerId,
        report: &MemberReport,
        counted_by: Option<Generation>,
    ) {
        if report.runs_compaction {
            self.compaction_runners.insert(from.clone());
        } else {
            self.compaction_runners.remove(from);
        }
        self.capacities.insert(from.clone(), report.available_capacity);
        self.subscriptions
            .insert(from.clone(), report.queues.clone());
        match report.admission.filter(|_| report.routing_crawled) {
            Some(admission) if Some(admission) == counted_by => {
                self.crawled.insert(from.clone(), admission);
            }
            _ => {
                self.crawled.remove(from);
            }
        }
    }

    /// Whether `worker` reported a crawl at `admission`.
    pub(super) fn crawled_at(&self, worker: &WorkerId, admission: Option<Generation>) -> bool {
        self.crawled
            .get(worker)
            .is_some_and(|crawled_at| Some(*crawled_at) == admission)
    }

    /// The free places each worker `roster` holds said it had in its latest
    /// heartbeat.
    pub(super) fn capacities(&self, roster: &Roster) -> BTreeMap<WorkerId, u32> {
        self.capacities
            .iter()
            .filter(|(worker, _)| roster.holds(worker))
            .map(|(worker, places)| (worker.clone(), *places))
            .collect()
    }

    /// The workers `roster` holds whose latest heartbeat said they run compaction.
    pub(super) fn compaction_runners(&self, roster: &Roster) -> BTreeSet<WorkerId> {
        self.compaction_runners
            .iter()
            .filter(|worker| roster.holds(worker))
            .cloned()
            .collect()
    }

    /// The queues each worker `roster` holds said in its latest heartbeat that
    /// it takes.
    pub(super) fn subscriptions(&self, roster: &Roster) -> BTreeMap<WorkerId, Subscription> {
        self.subscriptions
            .iter()
            .filter(|(worker, _)| roster.holds(worker))
            .map(|(worker, queues)| (worker.clone(), queues.clone()))
            .collect()
    }
}
