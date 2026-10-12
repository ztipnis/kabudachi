//! What a worker reports about a run it holds: that it started, and that it succeeded, with its result's
//! digest, and how a certified run's continuation ends.

use crate::protocol::digest::Digest;
use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::TaskRun;
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, WallTime};

use super::{Observer, Scheduler};

/// The leader's word that a run's result is the authoritative one. Until a
/// client holds this, result bytes it received are only provisional.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Certification {
    pub task_id: TaskId,
    pub task_run_id: TaskRunId,
    pub result_digest: Digest,
}

/// Whether a certified run ends its task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    /// The task is over: its coalescing key is freed, its payload stops
    /// counting, and it is forgotten `result_ttl` later.
    Final,
    /// The result is a continuation (an implicit flow): the run
    /// is certified, but the task holds its key and its memory until
    /// [`Scheduler::end_continuation`].
    Continues,
}

/// Why a worker's report about a run was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReportRejection {
    #[error("this node is not the leader")]
    NotLeader,
    #[error("no such run")]
    UnknownRun,
    #[error("the run does not belong to this worker or is not in the expected state")]
    NotAuthoritative,
    /// The run's task has a newest record this leader cannot know yet: ask again.
    #[error("the run's task is not known for certain yet")]
    NotReady,
}

/// Why ending a continuation was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ContinuationRejection {
    #[error("this node is not the leader")]
    NotLeader,
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// The worker that claimed `run_id` reports that it began executing. The
    /// same report again, while the run still runs, is taken and changes
    /// nothing.
    pub fn report_started(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
    ) -> Result<(), ReportRejection> {
        let outcome = self.start_run(worker, run_id);
        self.end_call();
        outcome
    }

    fn start_run(&mut self, worker: &WorkerId, run_id: &TaskRunId) -> Result<(), ReportRejection> {
        self.require_leader()?;
        self.start_owned(worker, run_id)
    }

    /// Starts `worker`'s claimed run. A start repeated by the worker of a run
    /// already running (its first went unanswered, or a new leader rebuilt the
    /// run as running) is taken again and changes nothing, so a report sent
    /// again never stops a healthy run.
    pub(super) fn start_owned(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
    ) -> Result<(), ReportRejection> {
        let already_running = self.runs.get(run_id).is_some_and(|run| {
            run.current_state() == TaskRunState::Running
                && run.selected_worker().as_ref() == Some(worker)
        });
        if already_running {
            return Ok(());
        }
        let stamped_at = WallTime::now(&self.clock);
        let run = self.owned_run(worker, run_id, TaskRunState::Claimed)?;
        run.transition_to(TaskRunState::Running, stamped_at)
            .expect("a Claimed run can always start");
        self.mark_decided(run_id);
        Ok(())
    }

    /// As [`Self::complete_delivered`], for a run whose result went to no
    /// client: a task with none, as in the one-node runtime.
    pub fn complete(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
    ) -> Result<Certification, ReportRejection> {
        self.complete_delivered(worker, run_id, result_digest, completion, false)
    }

    /// The worker running `run_id` reports success with the digest of its
    /// result, and whether it delivered the result to the task's client
    /// first, which the run records. Only a `Running` run owned by that
    /// worker can complete, so a stale or repeated report is refused and
    /// certifies nothing. `completion` says whether that ends the task.
    pub fn complete_delivered(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
        delivered: bool,
    ) -> Result<Certification, ReportRejection> {
        let outcome = self.certify(worker, run_id, result_digest, completion, delivered);
        self.end_call();
        outcome
    }

    /// Certifies the result of a running run, and finishes its task unless it continues.
    fn certify(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
        delivered: bool,
    ) -> Result<Certification, ReportRejection> {
        self.require_leader()?;
        // A compaction run's result is its fold, reported by `complete_compaction`.
        if self
            .runs
            .get(run_id)
            .is_some_and(|run| self.is_compaction(&run.task_id()))
        {
            return Err(ReportRejection::NotAuthoritative);
        }
        self.certify_owned(worker, run_id, result_digest, completion, delivered)
    }

    pub(super) fn certify_owned(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
        delivered: bool,
    ) -> Result<Certification, ReportRejection> {
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        let run = self.owned_run(worker, run_id, TaskRunState::Running)?;
        run.transition_to(TaskRunState::Succeeded, stamped_at)
            .expect("a Running run can always succeed");
        run.result_digest = Some(result_digest.clone().into());
        run.delivered = delivered;
        let task_id = run.task_id();
        self.mark_decided(run_id);
        match completion {
            Completion::Continues => {
                self.continuing.insert(task_id.clone());
            }
            Completion::Final => self.record_finished(&task_id, now),
        }
        Ok(Certification {
            task_id,
            task_run_id: run_id.clone(),
            result_digest,
        })
    }

    /// The continuation of `task_id` is over, however it ended: the task is
    /// finished after all. Says whether it had a continuation to end, so
    /// ending twice, or a task with none, changes nothing.
    pub fn end_continuation(&mut self, task_id: &TaskId) -> Result<bool, ContinuationRejection> {
        if !self.check_leader() {
            return Err(ContinuationRejection::NotLeader);
        }
        let had_one = self.finish_continuation(task_id);
        self.end_call();
        Ok(had_one)
    }

    fn finish_continuation(&mut self, task_id: &TaskId) -> bool {
        if !self.continuing.remove(task_id) {
            return false;
        }
        let now = self.clock.now();
        self.record_finished(task_id, now);
        true
    }

    /// `run_id`'s run, if the run exists, `worker` claimed it and it is in
    /// `expected` state. Does not ask whether this node leads: a rebuild
    /// applies what workers report before it holds a grant.
    pub(super) fn owned_run(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        expected: TaskRunState,
    ) -> Result<&mut TaskRun, ReportRejection> {
        if !self.runs.contains_key(run_id) {
            let uncertain = self.reconciliation.holds_uncertain_run(run_id);
            return Err(if uncertain {
                ReportRejection::NotReady
            } else {
                ReportRejection::UnknownRun
            });
        }
        let run = self.runs.get_mut(run_id).expect("just seen");
        let owned = run.selected_worker().as_ref() == Some(worker);
        // A run replaced by a retry or a replay is `Failed` or `Lost`, which no
        // report expects, so ownership and state are enough.
        if !owned || run.current_state() != expected {
            return Err(ReportRejection::NotAuthoritative);
        }
        Ok(run)
    }
}
