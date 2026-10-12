//! What the workers that answered report about their runs, applied run by run.

use std::collections::{BTreeMap, BTreeSet};

use crate::coalescing;
use crate::protocol::digest::Digest;
use crate::protocol::generated;
use crate::protocol::ids::{IdGenerator, TaskRunId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{TaskRun, TaskRunIdentity};
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::reconcile::{ANSWER_IN_FLIGHT, ReportedRun, ReportedState, WorkerRuns};
use crate::time::{Clock, WallTime};

use super::super::{Claim, Completion, HeldCancel, Observer, Scheduler};
use super::Adopted;

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Applies what the workers that answered reported, run by run: a run
    /// its worker reports is adopted at the state it reports (a success is
    /// certified, a failure applied), one the leader holds for a worker that
    /// answered without it is lost, and one whose task has no record is
    /// rebuilt from its claim. A run decided shortly before the worker was
    /// asked may not have reached it, so it is not lost for being missing.
    /// What a worker's earlier answer left held for tasks still uncertain is
    /// dropped when its newer answer leaves those runs out.
    pub(super) fn apply_reports(&mut self, reports: BTreeMap<WorkerId, WorkerRuns>, out: &mut Adopted) {
        for (worker, answer) in reports {
            let now = self.clock.now();
            let stamped_at = WallTime::now(&self.clock);
            let reported: BTreeSet<TaskRunId> = answer
                .runs
                .iter()
                .map(|run| run.claim.task_run_id.clone())
                .collect();
            self.reconciliation.drop_unreported(&worker, &reported);
            for task_id in self.held_by(&worker) {
                let run_id = &self.current_run[&task_id];
                let in_flight = answer.asked_at.is_some_and(|asked_at| {
                    self.run_decided_at
                        .get(run_id)
                        .is_some_and(|decided| *decided + ANSWER_IN_FLIGHT > asked_at)
                });
                if !reported.contains(run_id) && !in_flight {
                    out.lost.push(self.lose_run(&task_id, now, stamped_at));
                }
            }
            for run in answer.runs {
                self.apply_reported(&worker, run, out);
            }
        }
    }

    /// Applies one run `worker` reported (see [`Self::apply_reports`]).
    pub(super) fn apply_reported(&mut self, worker: &WorkerId, reported: ReportedRun, out: &mut Adopted) {
        let Some(reported) = self.reconciliation.hold_report(worker, reported) else {
            return;
        };
        let task_id = reported.claim.task.task_id();
        let run_id = reported.claim.task_run_id.clone();
        if !self.tasks.contains_key(&task_id) && !self.rebuild_from_claim(worker, &reported.claim) {
            return;
        }
        if self.current_run.get(&task_id) != Some(&run_id) {
            return;
        }
        let recorded = self.runs[&run_id].current_state();
        if self.runs[&run_id].selected_worker().as_ref() != Some(worker) {
            return;
        }
        if recorded == TaskRunState::Cancelled
            && matches!(reported.state, ReportedState::Claimed | ReportedState::Running)
        {
            self.held_cancels.push(HeldCancel {
                task_id,
                task_run_id: run_id,
                worker: worker.clone(),
            });
            return;
        }
        if !matches!(recorded, TaskRunState::Claimed | TaskRunState::Running) {
            return;
        }
        let beyond_claimed = !matches!(reported.state, ReportedState::Claimed);
        if recorded == TaskRunState::Claimed && beyond_claimed {
            let _ = self.start_owned(worker, &run_id);
        }
        match reported.state {
            ReportedState::Claimed | ReportedState::Running => {}
            ReportedState::Succeeded { result_digest } => {
                if let Ok(certified) =
                    self.certify_owned(worker, &run_id, result_digest, Completion::Final)
                {
                    out.certified.push(certified);
                }
            }
            ReportedState::Failed { failure_kind } => {
                if let Ok(failed) = self.fail_owned(worker, &run_id, failure_kind) {
                    out.failed.push(failed);
                }
            }
        }
    }

    /// Holds the task `claim` was granted for, which no record is known of, as
    /// its worker's claimed run: the key it holds is occupied with an empty
    /// chain (the worker folded the chain it was given). Says whether it did;
    /// it does not when another generation already holds the key.
    fn rebuild_from_claim(&mut self, worker: &WorkerId, claim: &Claim) -> bool {
        let task = claim.task.clone();
        let task_id = task.task_id();
        let key = task
            .coalescing_key
            .as_deref()
            .map(|key| coalescing::key(&task.task_definition_id(), key));
        if key
            .as_ref()
            .is_some_and(|key| self.occupancy.is_blocked(key, &task_id))
        {
            return false;
        }
        let stamped_at = WallTime::now(&self.clock);
        let mut run = TaskRun {
            identity: Some(TaskRunIdentity {
                task_run_id: Some(claim.task_run_id.clone().into()),
                task_id: Some(task_id.clone().into()),
                attempt_number: claim.attempt_number,
                parent_task_run_id: None,
            }),
            source_version: task.source_version,
            execution_version: task.source_version,
            created_at: Some(stamped_at.into()),
            state: generated::TaskRunState::Queued as i32,
            updated_at: Some(stamped_at.into()),
            selected_worker: None,
            result_digest: None,
            failure_kind: String::new(),
        };
        run.transition_to(TaskRunState::Claimed, stamped_at)
            .expect("a queued run can be claimed");
        run.selected_worker = Some(worker.clone().into());
        let needed = task.serialized_input.len() as u64;
        let compaction_key = task.compacts.as_ref().map(|prefix| {
            coalescing::key(&task.task_definition_id(), &prefix.coalescing_key)
        });
        if compaction_key.as_ref().is_some_and(|key| {
            self.occupancy
                .compaction_of(key)
                .is_some_and(|other| *other != task_id)
        }) {
            return false;
        }
        self.input_digests
            .insert(task_id.clone(), Digest::blake3(&task.serialized_input));
        self.current_run
            .insert(task_id.clone(), claim.task_run_id.clone());
        self.runs_of_task
            .insert(task_id.clone(), vec![claim.task_run_id.clone()]);
        self.runs.insert(claim.task_run_id.clone(), run);
        self.tasks.insert(task_id.clone(), task);
        self.mark_current_decided(&task_id);
        self.budget.take(needed);
        if let Some(key) = key {
            self.occupancy.start(&key, &task_id);
        }
        if let Some(key) = compaction_key {
            self.occupancy.restore_compaction(&key, &task_id, true);
        }
        self.update_pressure();
        true
    }
}
