//! How a task stands, by its current run, and the conclusions a task with a
//! client leaves when it ends.

use crate::protocol::digest::Digest;
use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::Clock;

use super::{Observer, Scheduler};

/// How a task stands, by its current run: what its client is told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub task_id: TaskId,
    pub task_run_id: TaskRunId,
    pub state: TaskRunState,
    /// No run of the task will change again: the run is over and no
    /// retry, replay or continuation follows it.
    pub finished: bool,
    pub result_digest: Option<Digest>,
    /// The error's type name, when the run failed.
    pub failure_kind: String,
    /// The newer generation that replaced it, when it was superseded.
    pub superseded_by: Option<TaskId>,
    /// The run's result reached the task's client before the run was
    /// reported completed.
    pub delivered: bool,
}

/// A task that has a client ended: the client is to be told how, once
/// the revision that ended it is stored. Drained with
/// [`Scheduler::take_conclusions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conclusion {
    pub client: WorkerId,
    pub outcome: Outcome,
}

/// Why a question about a task's outcome was not answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeRejection {
    /// Only a leader answers.
    NotLeader,
    /// The task's newest record is not known to this leader yet: ask again.
    NotReady,
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// How `task_id` stands now, by its current run; `None` for a task
    /// this leader does not hold (never stored, or forgotten). Decides
    /// nothing. Refused while not leading, and while the task's newest
    /// record is still unknown to this leader.
    pub fn outcome(&mut self, task_id: &TaskId) -> Result<Option<Outcome>, OutcomeRejection> {
        let outcome = if !self.check_leader() {
            Err(OutcomeRejection::NotLeader)
        } else if self.reconciliation.is_uncertain(task_id) {
            Err(OutcomeRejection::NotReady)
        } else {
            Ok(self.outcome_of(task_id))
        };
        self.end_call();
        outcome
    }

    /// Every conclusion since the last call, oldest first; none while not
    /// leading, when they are kept. A rebuild concludes before its node has
    /// stored the revisions it republished, and the scheduler leads only once
    /// they are stored, so a conclusion is never told ahead of its revision.
    /// A later rebuild drops what an office that never led concluded: its
    /// client asks how the task stands instead.
    pub fn take_conclusions(&mut self) -> Vec<Conclusion> {
        if !self.is_leader() {
            return Vec::new();
        }
        std::mem::take(&mut self.conclusions)
    }

    fn outcome_of(&self, task_id: &TaskId) -> Option<Outcome> {
        let run_id = self.current_run.get(task_id)?;
        let run = &self.runs[run_id];
        let state = run.current_state();
        Some(Outcome {
            task_id: task_id.clone(),
            task_run_id: run_id.clone(),
            state,
            finished: state.is_terminal() && !self.continuing.contains(task_id),
            result_digest: run
                .result_digest
                .as_ref()
                .and_then(|digest| Digest::try_from(digest).ok()),
            failure_kind: run.failure_kind.clone(),
            superseded_by: self
                .links
                .get(task_id)
                .and_then(|link| link.superseded_by.clone())
                .map(TaskId::from),
            delivered: run.delivered,
        })
    }

    /// Leaves a conclusion for `task_id`'s client, if it has one: its
    /// current run has just ended the task. A task whose run returned a step
    /// concludes nothing: its client takes its end from the step's, and a
    /// certification of the step's digest would match no result it holds.
    pub(super) fn conclude(&mut self, task_id: &TaskId) {
        let Some(client) = self.tasks.get(task_id).and_then(|task| task.client()) else {
            return;
        };
        let continued = self
            .current_run
            .get(task_id)
            .is_some_and(|run| self.runs[run].continuation.is_some());
        if continued {
            return;
        }
        if let Some(outcome) = self.outcome_of(task_id) {
            self.conclusions.push(Conclusion { client, outcome });
        }
    }
}
