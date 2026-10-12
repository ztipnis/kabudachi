//! Runs lost with their worker: a worker counted silent loses each run it holds
//! once that run's reconnect timeout has passed, a worker can report one run
//! lost, and a lost run is replayed unless the task forbids it.

use std::collections::BTreeSet;

use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::TaskRun;
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, Instant, WallTime};

use super::{Observer, ReportRejection, Scheduler};

/// A run that was lost with its worker, and the run that replaces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostRun {
    pub task_id: TaskId,
    pub task_run_id: TaskRunId,
    /// What the run became: `Lost`, or `Orphaned` for a running run of a
    /// non-retriable task.
    pub state: TaskRunState,
    /// The queued next attempt, or `None` when the run is not replayed: an
    /// ephemeral task's, an orphaned one, and a coalescing generation that a
    /// newer one had superseded in the meantime.
    pub replayed: Option<TaskRunId>,
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Its node's office counts `worker` silent from `reconnect_from` on:
    /// each claimed or running run the worker holds is lost once that run's
    /// own reconnect timeout has passed since then (a run of a task
    /// submitted without one at the shard's, which the grant carries). A
    /// lost run is replayed at once and uses up no retry, except that an
    /// ephemeral task's stays `Lost`, a running run of a non-retriable task
    /// is `Orphaned`, and a coalescing generation with a newer one waiting
    /// stays lost. `None`: the worker was heard again, and the runs it holds
    /// are kept. Kept while this scheduler does not lead, and acted on while
    /// it does, here and by [`Self::catch_up`] at [`Self::next_deadline`].
    pub fn note_silence(&mut self, worker: &WorkerId, reconnect_from: Option<Instant>) {
        match reconnect_from {
            Some(from) => {
                self.silent.insert(worker.clone(), from);
            }
            None => {
                self.silent.remove(worker);
            }
        }
        if self.check_leader() {
            self.lose_overdue();
        }
        self.end_call();
    }

    /// Each claimed or running current run of a silent worker, by its task,
    /// with the instant from which it may be replayed. One pass over the
    /// current runs, and none while no worker is silent.
    pub(super) fn replays_due(&self) -> Vec<(Instant, TaskId)> {
        if self.silent.is_empty() {
            return Vec::new();
        }
        self.current_run
            .iter()
            .filter_map(|(task_id, run_id)| {
                let run = &self.runs[run_id];
                if !matches!(
                    run.current_state(),
                    TaskRunState::Claimed | TaskRunState::Running
                ) {
                    return None;
                }
                let from = self.silent.get(run.selected_worker().as_ref()?)?;
                Some((*from + self.reconnect_timeout_of(task_id), task_id.clone()))
            })
            .collect()
    }

    /// Loses every run of a silent worker whose reconnect timeout has
    /// passed (see [`Self::note_silence`]). Only a leader decides this, so
    /// its callers check.
    pub(super) fn lose_overdue(&mut self) {
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        for (at, task_id) in self.replays_due() {
            if at <= now {
                self.lose_run(&task_id, now, stamped_at);
            }
        }
    }

    /// One run `worker` holds is gone while the worker is not: the process
    /// that ran its body died. The run becomes lost or orphaned, and is
    /// replayed or not, exactly as if its whole worker had been lost.
    /// Refused, as a failure report is, when this node does not lead, the
    /// run is unknown, or it is not a claimed or running run of `worker`
    /// (it already ended, or a retry or replay replaced it).
    pub fn report_lost(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
    ) -> Result<LostRun, ReportRejection> {
        let outcome = self.lose_held_run(worker, run_id);
        self.end_call();
        outcome
    }

    fn lose_held_run(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
    ) -> Result<LostRun, ReportRejection> {
        self.require_leader()?;
        let expected = match self.runs.get(run_id).map(TaskRun::current_state) {
            Some(TaskRunState::Claimed) => TaskRunState::Claimed,
            _ => TaskRunState::Running,
        };
        // A replaced run is `Failed` or `Lost`, so a run still held in one of
        // these states is its task's current run.
        let task_id = self.owned_run(worker, run_id, expected)?.task_id();
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        Ok(self.lose_run(&task_id, now, stamped_at))
    }

    /// The workers a claimed or running current run of one of `tasks` is
    /// selected for.
    pub(super) fn holders_of<'t>(
        &self,
        tasks: impl Iterator<Item = &'t TaskId>,
    ) -> BTreeSet<WorkerId> {
        tasks
            .filter_map(|task_id| {
                let run = &self.runs[self.current_run.get(task_id)?];
                matches!(
                    run.current_state(),
                    TaskRunState::Claimed | TaskRunState::Running
                )
                .then(|| run.selected_worker())
                .flatten()
            })
            .collect()
    }

    /// The tasks whose current run is claimed or running for `worker`.
    pub(super) fn held_by(&self, worker: &WorkerId) -> Vec<TaskId> {
        self.current_run
            .iter()
            .filter(|(_, run_id)| {
                let run = &self.runs[*run_id];
                matches!(
                    run.current_state(),
                    TaskRunState::Claimed | TaskRunState::Running
                ) && run.selected_worker().as_ref() == Some(worker)
            })
            .map(|(task_id, _)| task_id.clone())
            .collect()
    }

    /// `task_id`'s current run, claimed or running, is gone with its worker:
    /// it becomes lost, or orphaned if it was a running run of a non-retriable
    /// task, and is replayed unless the task is ephemeral, orphaned, or a
    /// newer generation of its coalescing key waits behind it.
    pub(super) fn lose_run(
        &mut self,
        task_id: &TaskId,
        now: Instant,
        stamped_at: WallTime,
    ) -> LostRun {
        let run_id = self.current_run[task_id].clone();
        let state = self.loss_state(task_id, &run_id);
        self.runs
            .get_mut(&run_id)
            .expect("every current run is stored")
            .transition_to(state, stamped_at)
            .expect("a claimed run can be lost, and a running one lost or orphaned");
        self.mark_decided(&run_id);
        let replayed = self.replay_after_loss(task_id, &run_id, state, now);
        if let Some(key) = self.compaction_key_of(task_id) {
            self.compact_if_due(&key);
        }
        LostRun {
            task_id: task_id.clone(),
            task_run_id: run_id,
            state,
            replayed,
        }
    }

    /// What a run lost with its worker becomes: `Orphaned` if it was running
    /// and its task is non-retriable and not ephemeral, `Lost` otherwise.
    fn loss_state(&self, task_id: &TaskId, run_id: &TaskRunId) -> TaskRunState {
        let was_running = self.runs[run_id].current_state() == TaskRunState::Running;
        let task = &self.tasks[task_id];
        if task.non_retriable && !task.ephemeral && was_running {
            TaskRunState::Orphaned
        } else {
            TaskRunState::Lost
        }
    }

    /// Queues the replay of `run_id`, lost as `state`, and returns it; or
    /// finishes the task when it is not replayed: the run is orphaned, the task
    /// is ephemeral, a newer generation of its key waits, or its record has no
    /// room for another attempt. A replay is a loss, not a retry.
    fn replay_after_loss(
        &mut self,
        task_id: &TaskId,
        run_id: &TaskRunId,
        state: TaskRunState,
        now: Instant,
    ) -> Option<TaskRunId> {
        let newer_waits = self
            .coalescing_key_of(task_id)
            .is_some_and(|key| self.occupancy.has_waiting(&key));
        if state == TaskRunState::Orphaned || self.tasks[task_id].ephemeral || newer_waits {
            self.record_finished(task_id, now);
            return None;
        }
        *self.losses.entry(task_id.clone()).or_default() += 1;
        let next = self.queue_next_attempt(task_id, run_id);
        if next.is_none() {
            self.record_finished(task_id, now);
        }
        next
    }
}
