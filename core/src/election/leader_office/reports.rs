use std::collections::{BTreeMap, BTreeSet};

use crate::configuration::{Generation, Roster};
use crate::protocol::ids::WorkerId;

use super::MemberReport;

/// What each worker's latest heartbeat said of it: the admission it
/// reported a routing crawl at, and whether it runs compaction.
#[derive(Default)]
pub(super) struct MemberReports {
    /// The workers whose latest heartbeat reported a routing crawl, with the
    /// admission each crawled at.
    crawled: BTreeMap<WorkerId, Generation>,
    /// The workers whose latest heartbeat said they run compaction.
    compaction_runners: BTreeSet<WorkerId>,
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

    /// The workers `roster` holds whose latest heartbeat said they run compaction.
    pub(super) fn compaction_runners(&self, roster: &Roster) -> BTreeSet<WorkerId> {
        self.compaction_runners
            .iter()
            .filter(|worker| roster.holds(worker))
            .cloned()
            .collect()
    }
}
