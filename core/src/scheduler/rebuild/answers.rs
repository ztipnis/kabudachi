//! What the workers that answered report about their runs, applied run by run.

use std::collections::{BTreeMap, BTreeSet};

use crate::coalescing::{self, Key};
use crate::manifest;
use crate::protocol::generated;
use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{Task, TaskRun, TaskRunIdentity};
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::reconcile::{ANSWER_IN_FLIGHT, ReportedRun, ReportedState, WorkerRuns};
use crate::time::{Clock, WallTime};

use super::super::composite::Step;
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
    pub(super) fn apply_reports(
        &mut self,
        reports: BTreeMap<WorkerId, WorkerRuns>,
        out: &mut Adopted,
    ) {
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
    pub(super) fn apply_reported(
        &mut self,
        worker: &WorkerId,
        reported: ReportedRun,
        out: &mut Adopted,
    ) {
        let Some(reported) = self.reconciliation.hold_report(worker, reported) else {
            return;
        };
        let task_id = reported.claim.task.task_id();
        let run_id = reported.claim.task_run_id.clone();
        if !self.tasks.contains_key(&task_id) && !self.rebuild_from_claim(worker, &reported.claim) {
            return;
        }
        match self.recorded_state_of(worker, &task_id, &run_id) {
            Some(TaskRunState::Cancelled) => {
                self.hold_cancel(worker, task_id, run_id, &reported.state)
            }
            Some(recorded @ (TaskRunState::Claimed | TaskRunState::Running)) => {
                self.adopt_reported(
                    worker,
                    &run_id,
                    recorded,
                    reported.state,
                    reported.continuation,
                    reported.execution_version,
                    out,
                );
            }
            _ => {}
        }
    }

    /// The state this leader records for `run_id`, if it is `task_id`'s
    /// current run and selected for `worker`.
    fn recorded_state_of(
        &self,
        worker: &WorkerId,
        task_id: &TaskId,
        run_id: &TaskRunId,
    ) -> Option<TaskRunState> {
        if self.current_run.get(task_id) != Some(run_id) {
            return None;
        }
        let run = &self.runs[run_id];
        (run.selected_worker().as_ref() == Some(worker)).then(|| run.current_state())
    }

    /// A run stored as cancelled that `worker` reports it still holds: whichever
    /// leader stored the cancel, the worker must still be told to stop the body.
    fn hold_cancel(
        &mut self,
        worker: &WorkerId,
        task_id: TaskId,
        run_id: TaskRunId,
        state: &ReportedState,
    ) {
        if matches!(state, ReportedState::Claimed | ReportedState::Running) {
            self.held_cancels.push(HeldCancel {
                task_id,
                task_run_id: run_id,
                worker: worker.clone(),
            });
        }
    }

    /// Takes what `worker` reports of its claimed or running run: a claimed run
    /// it reports beyond its claim is started first.
    fn adopt_reported(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        recorded: TaskRunState,
        state: ReportedState,
        continuation: Option<Step>,
        execution_version: Option<u32>,
        out: &mut Adopted,
    ) {
        if recorded == TaskRunState::Claimed && !matches!(state, ReportedState::Claimed) {
            let _ = self.start_owned(worker, run_id, execution_version);
        }
        self.apply_reported_outcome(worker, run_id, state, continuation, out);
    }

    /// A success `worker` reports of its running run is certified, with the
    /// step it returned if it returned one, and a failure applied; a claimed
    /// or running one changes nothing more. A run its certifying revision
    /// was stored for is not running in the records, so is not here: its step
    /// is on the run.
    fn apply_reported_outcome(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        state: ReportedState,
        continuation: Option<Step>,
        out: &mut Adopted,
    ) {
        match state {
            ReportedState::Claimed | ReportedState::Running => {}
            ReportedState::Succeeded {
                result_digest,
                delivered,
            } => {
                let completion = continuation.map_or(Completion::Final, Completion::Continues);
                if let Ok(certified) = self.certify_owned(
                    worker,
                    run_id,
                    result_digest,
                    completion,
                    delivered,
                    None,
                    None,
                ) {
                    out.certified.push(certified);
                }
            }
            ReportedState::Failed {
                failure_kind,
                failure_message,
            } => {
                if let Ok(failed) = self.fail_owned(worker, run_id, failure_kind, failure_message)
                {
                    out.failed.push(failed);
                }
            }
        }
    }

    /// Holds the task `claim` was granted for, which no record is known of, as
    /// its worker's claimed run: the key it holds is occupied with an empty
    /// chain (the worker folded the chain it was given). Says whether it did;
    /// it does not when another generation already holds the key, or another
    /// compaction is the key's.
    fn rebuild_from_claim(&mut self, worker: &WorkerId, claim: &Claim) -> bool {
        let task = &claim.task;
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
        let compaction_key = claimed_compaction_key(task);
        if self.compacted_by_another(&task_id, compaction_key.as_ref()) {
            return false;
        }
        let run = claimed_run(worker, claim, stamped_at);
        self.hold_rebuilt(claim.task.clone(), run);
        self.count_running(&task_id);
        if let Some(key) = key {
            self.occupancy.start(&key, &task_id);
        }
        if let Some(key) = compaction_key {
            self.occupancy.restore_compaction(&key, &task_id, true);
        }
        self.update_pressure();
        true
    }

    /// Whether another task's compaction is the one `compaction_key` names, so
    /// a claimed run of `task_id` may not be rebuilt.
    fn compacted_by_another(&self, task_id: &TaskId, compaction_key: Option<&Key>) -> bool {
        compaction_key.is_some_and(|key| {
            self.occupancy
                .compaction_of(key)
                .is_some_and(|other| other != task_id)
        })
    }

    /// Holds `task` with `run`, its one claimed run, and counts its input
    /// against memory.
    fn hold_rebuilt(&mut self, task: Task, run: TaskRun) {
        let task_id = task.task_id();
        let run_id = run.task_run_id();
        let needed = manifest::payload_bytes(&task);
        self.input_digests
            .insert(task_id.clone(), manifest::input_digest(&task));
        self.current_run.insert(task_id.clone(), run_id.clone());
        self.runs_of_task
            .insert(task_id.clone(), vec![run_id.clone()]);
        self.runs.insert(run_id, run);
        self.tasks.insert(task_id.clone(), task);
        self.mark_current_decided(&task_id);
        self.budget.take(needed);
    }
}

/// The run `claim` granted `worker`, claimed at `stamped_at`, as a record
/// of it would hold it.
fn claimed_run(worker: &WorkerId, claim: &Claim, stamped_at: WallTime) -> TaskRun {
    let task = &claim.task;
    let mut run = TaskRun {
        identity: Some(TaskRunIdentity {
            task_run_id: Some(claim.task_run_id.clone().into()),
            task_id: Some(task.task_id().into()),
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
        delivered: false,
        claimed_at: None,
        started_at: None,
        failure_message: String::new(),
        continuation: None,
        persistence: generated::PersistenceState::Unspecified as i32,
        result_manifest: None,
    };
    run.transition_to(TaskRunState::Claimed, stamped_at)
        .expect("a queued run can be claimed");
    run.selected_worker = Some(worker.clone().into());
    run
}

/// The key of the compaction `task` makes, if it makes one.
fn claimed_compaction_key(task: &Task) -> Option<Key> {
    task.compacts
        .as_ref()
        .map(|prefix| coalescing::key(&task.task_definition_id(), &prefix.coalescing_key))
}
