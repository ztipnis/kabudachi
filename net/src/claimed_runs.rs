//! The runs this worker claimed and has not yet heard the end of, as it
//! last knew each: what it reports to a new leader, and what its heartbeats
//! summarise. Kept from the answers its own claim and lifecycle calls got,
//! never from anyone else's view.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::TaskRunId;
use kabudachi_core::protocol::messages::Claim;

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
    pub claim: Claim,
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

    /// Records a granted claim. A claim that names no run is not one a leader
    /// grants, so it is not kept.
    pub(crate) fn claimed(&self, claim: Claim) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn held_run() -> (ClaimedRuns, TaskRunId) {
        let run = TaskRunId::new("run-1");
        let ledger = ClaimedRuns::default();
        ledger.claimed(Claim {
            task_run_id: Some(run.clone().into()),
            ..Claim::default()
        });
        (ledger, run)
    }

    #[test]
    fn a_late_start_answer_does_not_undo_a_report_that_the_run_ended() {
        let (ledger, run) = held_run();
        let completed = HeldRun::Completed {
            result_digest: Digest::blake3(b"result"),
        };
        ledger.set(&run, completed.clone());

        ledger.set(&run, HeldRun::Running);

        assert_eq!(ledger.get(&run).map(|held| held.state), Some(completed));

        let failed = HeldRun::Failed {
            failure_kind: "ValueError".into(),
        };
        ledger.set(&run, failed.clone());
        ledger.set(&run, HeldRun::Running);
        assert_eq!(ledger.get(&run).map(|held| held.state), Some(failed));
    }
}
