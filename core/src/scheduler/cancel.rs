//! Cancelling a task, and the runs stored as cancelled that a worker still holds.

use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, WallTime};

use super::{Event, Observer, Scheduler};

/// A run stored as cancelled that its worker reported it still holds: the
/// worker has to be told to stop the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldCancel {
    pub task_id: TaskId,
    pub task_run_id: TaskRunId,
    pub worker: WorkerId,
}

/// What cancelling a task did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cancellation {
    /// The task's current run is now `Cancelled`.
    Cancelled {
        was_running: bool,
    },
    /// The task had already finished, so there was nothing to cancel.
    AlreadyFinished,
    UnknownTask,
}

/// Why a cancellation was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CancelRejection {
    #[error("this node is not the leader")]
    NotLeader,
    /// The task's newest record cannot be known yet: ask again.
    #[error("the task's newest record is not known yet")]
    NotReady,
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Cancels `task_id` whatever state its current run is in: a pending or
    /// scheduled run never starts, and a claimed or running one is taken from
    /// its worker, whose later reports are refused. Only a leader decides.
    /// A cancelled task is not retried, and does not expire.
    pub fn cancel(&mut self, task_id: &TaskId) -> Result<Cancellation, CancelRejection> {
        let outcome = self.cancel_task(task_id);
        self.end_call();
        outcome
    }

    fn cancel_task(&mut self, task_id: &TaskId) -> Result<Cancellation, CancelRejection> {
        if !self.check_leader() {
            return Err(CancelRejection::NotLeader);
        }
        if self.reconciliation.is_uncertain(task_id) {
            return Err(CancelRejection::NotReady);
        }
        let Some(run_id) = self.current_run.get(task_id).cloned() else {
            return Ok(Cancellation::UnknownTask);
        };
        // Compaction runs are internal: no client holds their ids.
        if self.is_compaction(task_id) {
            return Ok(Cancellation::UnknownTask);
        }
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        let run = self
            .runs
            .get_mut(&run_id)
            .expect("every current run is stored");
        let was_running = match run.current_state() {
            TaskRunState::Scheduled | TaskRunState::Queued => false,
            TaskRunState::Claimed | TaskRunState::Running => true,
            _ => return Ok(Cancellation::AlreadyFinished),
        };
        run.transition_to(TaskRunState::Cancelled, stamped_at)
            .expect("an unfinished run can always be cancelled");
        self.waiting.leave(task_id);
        self.record_finished(task_id, now);
        self.mark_decided(&run_id);
        self.events.push(Event::Cancelled {
            task_id: task_id.clone(),
            task_run_id: run_id,
            was_running,
        });
        Ok(Cancellation::Cancelled { was_running })
    }

    /// The runs stored as cancelled that their workers reported they still
    /// hold, each time one reported it: whichever leader stored the cancel,
    /// the worker must still be told to stop the body. Nothing while the
    /// scheduler does not lead: what a rebuild installed is told of only once
    /// it is stored again at this leader's term. Kept until then.
    pub fn take_held_cancels(&mut self) -> Vec<HeldCancel> {
        if !self.is_leader() {
            return Vec::new();
        }
        std::mem::take(&mut self.held_cancels)
    }
}
