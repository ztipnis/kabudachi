//! The runs this worker claimed and has not yet heard the end of, as it
//! last knew each: what it reports to a new leader, and what its heartbeats
//! summarise. Kept from the answers its own claim and lifecycle calls got,
//! never from anyone else's view.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex, PoisonError};

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::TaskRunId;
use kabudachi_core::protocol::messages::Claim as WireClaim;
use kabudachi_core::reconcile::{ReportedRun, ReportedState};
use kabudachi_core::scheduler::Claim;

/// The most bytes of a failure kind a reconciliation reports. A failure kind
/// is an exception class name in practice; the cap keeps a pathological one
/// from growing a report page's single item past the message limit.
const MAX_FAILURE_KIND_BYTES: usize = 256;

/// What this worker last did to a run it claimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldRun {
    Claimed,
    Running,
    /// It succeeded with this digest, and no leader has certified it yet.
    Completed { result_digest: Digest },
    /// It failed, and no leader has recorded the failure yet.
    Failed { failure_kind: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedRun {
    /// The claim as the leader granted it.
    pub claim: WireClaim,
    pub state: HeldRun,
}

/// Shared by a `Net` and whoever reads it; cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct ClaimedRuns(Arc<Mutex<BTreeMap<TaskRunId, ClaimedRun>>>);

impl ClaimedRuns {
    pub fn get(&self, run: &TaskRunId) -> Option<ClaimedRun> {
        self.runs().get(run).cloned()
    }

    /// Every run held, by run id.
    pub fn snapshot(&self) -> Vec<(TaskRunId, ClaimedRun)> {
        self.runs()
            .iter()
            .map(|(run, held)| (run.clone(), held.clone()))
            .collect()
    }

    /// Hands `take` the runs held after `after` (all of them with `None`), as
    /// a reconciliation reports them, in run id order, converting one at a
    /// time and stopping once `take` returns false, so a page costs only the
    /// runs it holds. A held claim that does not read as one a leader grants
    /// is left out.
    pub fn reported_runs_after(
        &self,
        after: Option<&TaskRunId>,
        mut take: impl FnMut(ReportedRun) -> bool,
    ) {
        let runs = self.runs();
        let from = after.map_or(Bound::Unbounded, Bound::Excluded);
        for (_, held) in runs.range::<TaskRunId, _>((from, Bound::Unbounded)) {
            let claim = match Claim::try_from(&held.claim) {
                Ok(claim) => claim,
                Err(malformed) => {
                    tracing::warn!(
                        task_run_id = ?held.claim.task_run_id,
                        %malformed,
                        "a held claim does not read as a granted one; left out of the report"
                    );
                    continue;
                }
            };
            let state = match &held.state {
                HeldRun::Claimed => ReportedState::Claimed,
                HeldRun::Running => ReportedState::Running,
                HeldRun::Completed { result_digest } => ReportedState::Succeeded {
                    result_digest: result_digest.clone(),
                },
                HeldRun::Failed { failure_kind } => ReportedState::Failed {
                    failure_kind: truncated(failure_kind, MAX_FAILURE_KIND_BYTES),
                },
            };
            // The chain's generations have records of their own, and a
            // claim alone can fill a message.
            let run = ReportedRun {
                claim: Claim { chain: Vec::new(), ..claim },
                state,
            };
            if !take(run) {
                return;
            }
        }
    }

    /// The runs held as claimed or running: what heartbeats digest.
    pub fn active_ids(&self) -> Vec<TaskRunId> {
        self.runs()
            .iter()
            .filter(|(_, held)| matches!(held.state, HeldRun::Claimed | HeldRun::Running))
            .map(|(run, _)| run.clone())
            .collect()
    }

    /// Records a granted claim. A claim that names no run is not one a leader
    /// grants, so it is not kept.
    pub(crate) fn claimed(&self, claim: WireClaim) {
        if let Some(run) = claim.task_run_id.clone() {
            self.runs().insert(
                TaskRunId::from(run),
                ClaimedRun {
                    claim,
                    state: HeldRun::Claimed,
                },
            );
        }
    }

    /// Notes what this worker did to `run`, if it still holds it. A run
    /// already reported completed or failed stays so: a start answered late
    /// does not set it running again.
    pub(crate) fn set(&self, run: &TaskRunId, state: HeldRun) {
        if let Some(held) = self.runs().get_mut(run) {
            let ended = matches!(held.state, HeldRun::Completed { .. } | HeldRun::Failed { .. });
            if !(ended && state == HeldRun::Running) {
                held.state = state;
            }
        }
    }

    pub(crate) fn forget(&self, run: &TaskRunId) {
        self.runs().remove(run);
    }

    fn runs(&self) -> std::sync::MutexGuard<'_, BTreeMap<TaskRunId, ClaimedRun>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// `text` cut to at most `max` bytes, at a char boundary.
fn truncated(text: &str, max: usize) -> String {
    let mut end = max.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}
