//! A run that failed: how its failure is kept, and the retry that replaces it while the task has retries left.

use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, WallTime};

use super::{Observer, ReportRejection, Scheduler};

/// The leader's record that a run failed, so its client can be told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub task_id: TaskId,
    pub task_run_id: TaskRunId,
    /// The run that replaces the failed one, queued, if the task has retries
    /// left. `None` means the task has failed for good.
    pub retry: Option<TaskRunId>,
}

/// The longest failure kind a record keeps, in bytes. A kind is an error
/// type's name, so any real one fits; a longer one is cut rather than refused
/// (the failure is reported either way), so that what a worker supplies can
/// never push a record past the limit the network refuses to store.
const MAX_FAILURE_KIND_BYTES: usize = 128;

/// `text` cut to at most `max` bytes, at a character boundary.
fn cut_to_fit(mut text: String, max: usize) -> String {
    if text.len() > max {
        let mut end = max;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// The worker running `run_id` reports that it failed with an error of
    /// type `failure_kind` (its name, never its message; one longer than
    /// `MAX_FAILURE_KIND_BYTES` is cut). Like completing,
    /// only a `Running` run owned by that worker can fail.
    pub fn fail(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: impl Into<String>,
    ) -> Result<Failure, ReportRejection> {
        let outcome = self.fail_run(worker, run_id, failure_kind.into());
        self.end_call();
        outcome
    }

    fn fail_run(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: String,
    ) -> Result<Failure, ReportRejection> {
        self.require_leader()?;
        self.fail_owned(worker, run_id, failure_kind)
    }

    pub(super) fn fail_owned(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: String,
    ) -> Result<Failure, ReportRejection> {
        self.start_if_claimed_compaction(worker, run_id);
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        let run = self.owned_run(worker, run_id, TaskRunState::Running)?;
        run.transition_to(TaskRunState::Failed, stamped_at)
            .expect("a Running run can always fail");
        run.failure_kind = cut_to_fit(failure_kind, MAX_FAILURE_KIND_BYTES);
        let task_id = run.task_id();
        let attempt = run.attempt_number();
        self.mark_decided(run_id);
        self.note_failed_compaction(&task_id);
        let retry = self.replace_failed_run(&task_id, run_id, attempt);
        if retry.is_none() {
            self.record_finished(&task_id, now);
        }
        Ok(Failure {
            task_id,
            task_run_id: run_id.clone(),
            retry,
        })
    }

    /// Queues the next attempt of `task_id` if it has retries left, and makes
    /// it the one authoritative run.
    fn replace_failed_run(
        &mut self,
        task_id: &TaskId,
        failed: &TaskRunId,
        attempt: u32,
    ) -> Option<TaskRunId> {
        let retries = self.tasks.get(task_id)?.max_retries;
        // Attempt 1 is the first try, so retries left = retries - (attempt - 1),
        // not counting the attempts that were lost rather than failed.
        let lost = self.losses.get(task_id).copied().unwrap_or(0);
        if attempt - lost > retries {
            return None;
        }
        self.queue_next_attempt(task_id, failed)
    }
}
