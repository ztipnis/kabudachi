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

#[cfg(test)]
mod tests {
    use super::*;

    fn held_run() -> (ClaimedRuns, TaskRunId) {
        let run = TaskRunId::new("run-1");
        let ledger = ClaimedRuns::default();
        ledger.claimed(WireClaim {
            task: Some(Default::default()),
            task_run_id: Some(run.clone().into()),
            ..WireClaim::default()
        });
        (ledger, run)
    }

    #[test]
    fn a_reported_failure_kind_is_cut_to_a_bound_at_a_char_boundary() {
        let (ledger, run) = held_run();
        ledger.set(&run, HeldRun::Failed { failure_kind: "é".repeat(MAX_FAILURE_KIND_BYTES) });

        let mut reported = Vec::new();
        ledger.reported_runs_after(None, |run| {
            reported.push(run);
            true
        });

        let ReportedState::Failed { failure_kind } = &reported[0].state else {
            panic!("a failed run is reported failed");
        };
        assert_eq!(failure_kind.len(), MAX_FAILURE_KIND_BYTES);
        assert!(failure_kind.chars().all(|c| c == 'é'));
    }

    #[test]
    fn reported_runs_after_a_cursor_are_visited_in_order_until_the_visitor_stops() {
        let ledger = ClaimedRuns::default();
        for n in 0..5 {
            ledger.claimed(WireClaim {
                task: Some(Default::default()),
                task_run_id: Some(TaskRunId::new(format!("run-{n}")).into()),
                ..WireClaim::default()
            });
        }
        let mut visited = Vec::new();

        ledger.reported_runs_after(Some(&TaskRunId::new("run-1")), |run| {
            visited.push(run.claim.task_run_id);
            visited.len() < 2
        });

        assert_eq!(
            visited,
            [TaskRunId::new("run-2"), TaskRunId::new("run-3")],
            "the runs after the cursor, up to the one the visitor refused"
        );
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
