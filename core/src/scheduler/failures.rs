//! A run that failed: how its failure is kept, and the retry that replaces it while the task has retries left.

use prost::Message as _;

use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, WallTime};

use super::{Observer, ReportRejection, Scheduler, UNVERSIONED};

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

/// The longest failure message a record keeps, in bytes: enough to say
/// what went wrong, never a traceback. A longer one is cut, at a
/// character boundary, and cut further if the record has less room left.
pub const MAX_FAILURE_MESSAGE_BYTES: usize = 1024;

/// `text` cut to at most `max` bytes, at a character boundary.
pub(super) fn cut_to_fit(mut text: String, max: usize) -> String {
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
    /// type `failure_kind` (its name; one longer than `MAX_FAILURE_KIND_BYTES`
    /// is cut), with no message. Like completing, only a `Running` run owned
    /// by that worker can fail.
    pub fn fail(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: impl Into<String>,
    ) -> Result<Failure, ReportRejection> {
        self.fail_with_message(worker, run_id, failure_kind, String::new())
    }

    /// As [`Self::fail`], keeping `failure_message`, the error's message
    /// (never its traceback), cut to `MAX_FAILURE_MESSAGE_BYTES` and to the
    /// room the task's record has left.
    pub fn fail_with_message(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: impl Into<String>,
        failure_message: impl Into<String>,
    ) -> Result<Failure, ReportRejection> {
        let outcome = self.fail_run(worker, run_id, failure_kind.into(), failure_message.into());
        self.end_call();
        outcome
    }

    fn fail_run(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: String,
        failure_message: String,
    ) -> Result<Failure, ReportRejection> {
        self.require_leader()?;
        self.fail_owned(worker, run_id, failure_kind, failure_message)
    }

    pub(super) fn fail_owned(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: String,
        failure_message: String,
    ) -> Result<Failure, ReportRejection> {
        self.start_if_claimed_compaction(worker, run_id);
        self.fail_running(worker, run_id, failure_kind, failure_message, true)
    }

    /// Fails `worker`'s running run `run_id` with `failure_kind` and no
    /// retry, whatever retries its task has left: a failure no new attempt
    /// could change.
    pub(super) fn fail_for_good(&mut self, worker: &WorkerId, run_id: &TaskRunId, failure_kind: &str) {
        self.fail_running(worker, run_id, failure_kind.to_owned(), String::new(), false)
            .expect("only a running run of its worker is failed for good");
    }

    fn fail_running(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: String,
        failure_message: String,
        may_retry: bool,
    ) -> Result<Failure, ReportRejection> {
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        let run = self.owned_run(worker, run_id, TaskRunState::Running)?;
        run.transition_to(TaskRunState::Failed, stamped_at)
            .expect("a Running run can always fail");
        run.failure_kind = cut_to_fit(failure_kind, MAX_FAILURE_KIND_BYTES);
        run.failure_message = cut_to_fit(failure_message, MAX_FAILURE_MESSAGE_BYTES);
        let task_id = run.task_id();
        let attempt = run.attempt_number();
        // What the record measures with the message in place, less what it
        // may hold: the message gives up what is over. A retry measures
        // again before it is added.
        let over = (self.record_of(&task_id, UNVERSIONED, stamped_at).encoded_len() as u64)
            .saturating_sub(Self::record_limit());
        if over > 0 {
            let run = self.runs.get_mut(run_id).expect("the failed run is stored");
            let keep = run.failure_message.len().saturating_sub(over as usize);
            run.failure_message = cut_to_fit(std::mem::take(&mut run.failure_message), keep);
        }
        self.mark_decided(run_id);
        self.note_failed_compaction(&task_id);
        let retry = may_retry
            .then(|| self.replace_failed_run(&task_id, run_id, attempt))
            .flatten();
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
