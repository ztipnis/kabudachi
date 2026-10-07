//! Creating Tasks and TaskRuns, and moving a TaskRun through its states.
//!
//! Both records are the generated wire messages, not a second domain type, so
//! there is nothing to keep in sync with the schema. This module adds the
//! parts a message cannot express: minting run IDs, stamping wall-clock times,
//! and refusing any state change the transition table does not allow.
//!
//! The message fields are public, so the types do not stop code from writing
//! `run.state` directly or editing a submitted Task. `transition_to` is the
//! only sanctioned way to change a run's state, and Task immutability
//! holds because the owner of the records, the scheduler, hands out
//! only shared references and clones.

use crate::protocol::generated;
use crate::protocol::ids::{
    IdGenerator, TaskDefinitionId, TaskId, TaskRunId, WorkerId, mint_task_run_id,
};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{Task, TaskRun, TaskRunIdentity};
use crate::protocol::task::TaskRunState;
use crate::time::{Duration, WallTime};

/// The attempt number of a Task's first run.
const FIRST_ATTEMPT: u32 = 1;

/// What a new Task is made of. `source_version` is the version of the task
/// definition's data format that `serialized_input` was encoded with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTask {
    pub task_id: TaskId,
    pub submitted_at: WallTime,
    pub definition_id: TaskDefinitionId,
    pub source_version: u32,
    pub serialized_input: Vec<u8>,
    pub queue: String,
    pub max_retries: u32,
    pub delay: Option<Duration>,
    pub expiry: Option<Duration>,
    pub coalescing_key: Option<String>,
    pub ephemeral: bool,
    pub non_retriable: bool,
}

impl NewTask {
    /// A task with no retries, delay or expiry.
    pub fn new(
        task_id: TaskId,
        submitted_at: WallTime,
        definition_id: TaskDefinitionId,
        source_version: u32,
        serialized_input: Vec<u8>,
        queue: impl Into<String>,
    ) -> Self {
        NewTask {
            task_id,
            submitted_at,
            definition_id,
            source_version,
            serialized_input,
            queue: queue.into(),
            max_retries: 0,
            delay: None,
            expiry: None,
            coalescing_key: None,
            ephemeral: false,
            non_retriable: false,
        }
    }
}

/// A new Task. Everything about it is given, so it needs no ID generator and
/// no clock.
pub fn new_task(new: NewTask) -> Task {
    Task {
        task_id: Some(new.task_id.into()),
        task_definition_id: Some(new.definition_id.into()),
        source_version: new.source_version,
        serialized_input: new.serialized_input,
        queue: new.queue,
        max_retries: new.max_retries,
        coalescing_key: new.coalescing_key,
        ephemeral: new.ephemeral,
        non_retriable: new.non_retriable,
        submitted_at: Some(new.submitted_at.into()),
        delay_millis: new.delay.map(|delay| delay.as_ticks()),
        expiry_millis: new.expiry.map(|expiry| expiry.as_ticks()),
    }
}

/// The first attempt at `task`. A run starts `Scheduled` (not yet due) or
/// `Queued` (due now); any later state is reached through `transition_to`.
///
/// # Panics
///
/// If `state` is anything else.
pub fn first_attempt(
    task: &Task,
    ids: &impl IdGenerator,
    at: WallTime,
    state: TaskRunState,
) -> TaskRun {
    // Checked in release builds too: this is public, and a run created in any
    // other state would skip the transition table.
    assert!(
        matches!(state, TaskRunState::Scheduled | TaskRunState::Queued),
        "a TaskRun starts Scheduled or Queued, not {state:?}"
    );
    TaskRun {
        identity: Some(TaskRunIdentity {
            task_run_id: Some(mint_task_run_id(ids).into()),
            task_id: Some(task.task_id().into()),
            attempt_number: FIRST_ATTEMPT,
            parent_task_run_id: None,
        }),
        source_version: task.source_version,
        execution_version: task.source_version,
        created_at: Some(at.into()),
        state: generated::TaskRunState::from(state) as i32,
        updated_at: Some(at.into()),
        selected_worker: None,
        result_digest: None,
        failure_kind: String::new(),
    }
}

/// The attempt that replaces `failed`, a terminal run of the same task: the
/// next attempt number, with `failed` as its parent, `Queued` at once.
pub fn retry_of(failed: &TaskRun, ids: &impl IdGenerator, at: WallTime) -> TaskRun {
    TaskRun {
        identity: Some(TaskRunIdentity {
            task_run_id: Some(mint_task_run_id(ids).into()),
            task_id: Some(failed.task_id().into()),
            attempt_number: failed.attempt_number() + 1,
            parent_task_run_id: Some(failed.task_run_id().into()),
        }),
        source_version: failed.source_version,
        execution_version: failed.execution_version,
        created_at: Some(at.into()),
        state: generated::TaskRunState::Queued as i32,
        updated_at: Some(at.into()),
        selected_worker: None,
        result_digest: None,
        failure_kind: String::new(),
    }
}

/// A state change the transition table does not allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("illegal TaskRun transition {from:?} -> {to:?}")]
pub struct IllegalTransition {
    pub from: TaskRunState,
    pub to: TaskRunState,
}

/// Reading and advancing a [`TaskRun`]. The ID accessors panic if the run has
/// no identity, which only a malformed message can lack; `current_state`
/// likewise panics on an unset or unknown state. Data decoded from a peer must
/// be checked where it is decoded, before it reaches these methods.
pub trait TaskRunRecord {
    fn task_id(&self) -> TaskId;
    fn task_run_id(&self) -> TaskRunId;
    /// The run this one replaced, if it is a retry or a replay.
    fn parent_task_run_id(&self) -> Option<TaskRunId>;
    fn attempt_number(&self) -> u32;
    fn current_state(&self) -> TaskRunState;
    /// The worker that claimed this run, if one has.
    fn selected_worker(&self) -> Option<WorkerId>;

    /// Moves to `next` and stamps `at`, or returns an error and changes
    /// nothing if the transition table does not allow it.
    fn transition_to(&mut self, next: TaskRunState, at: WallTime) -> Result<(), IllegalTransition>;
}

/// The run's identity, which every valid run has.
fn identity_of(run: &TaskRun) -> &TaskRunIdentity {
    run.identity
        .as_ref()
        .expect("TaskRun.identity is required by protocol invariant but was absent")
}

impl TaskRunRecord for TaskRun {
    fn task_id(&self) -> TaskId {
        identity_of(self).task_id()
    }

    fn task_run_id(&self) -> TaskRunId {
        identity_of(self).task_run_id()
    }

    fn parent_task_run_id(&self) -> Option<TaskRunId> {
        identity_of(self).parent_task_run_id()
    }

    fn attempt_number(&self) -> u32 {
        identity_of(self).attempt_number
    }

    fn current_state(&self) -> TaskRunState {
        TaskRunState::try_from(self.state())
            .expect("TaskRun.state is required by protocol invariant but was unspecified")
    }

    fn selected_worker(&self) -> Option<WorkerId> {
        self.selected_worker.clone().map(Into::into)
    }

    fn transition_to(&mut self, next: TaskRunState, at: WallTime) -> Result<(), IllegalTransition> {
        let from = self.current_state();
        if !from.can_transition_to(next) {
            return Err(IllegalTransition { from, to: next });
        }
        self.set_state(next.into());
        self.updated_at = Some(at.into());
        Ok(())
    }
}

impl From<WallTime> for generated::WallTime {
    fn from(time: WallTime) -> Self {
        generated::WallTime {
            unix_millis: time.as_unix_millis(),
        }
    }
}

impl From<generated::WallTime> for WallTime {
    fn from(time: generated::WallTime) -> Self {
        WallTime::from_unix_millis(time.unix_millis)
    }
}
