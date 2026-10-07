//! Domain `TaskRunState` and its transition table. The
//! wire enum's `UNSPECIFIED` sentinel has no domain meaning and is not
//! represented.

use crate::protocol::generated;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskRunState {
    Scheduled,
    Queued,
    Claimed,
    Running,
    Succeeded,
    Failed,
    Expired,
    Superseded,
    Cancelled,
    Lost,
    Orphaned,
}

impl TaskRunState {
    /// Every variant, in definition order, for exhaustive iteration in tests.
    pub const ALL: [TaskRunState; 11] = [
        TaskRunState::Scheduled,
        TaskRunState::Queued,
        TaskRunState::Claimed,
        TaskRunState::Running,
        TaskRunState::Succeeded,
        TaskRunState::Failed,
        TaskRunState::Expired,
        TaskRunState::Superseded,
        TaskRunState::Cancelled,
        TaskRunState::Lost,
        TaskRunState::Orphaned,
    ];

    /// A terminal state has no legal outgoing transition. `Lost` and
    /// `Orphaned` are terminal here because a retry is a new `TaskRun` with a
    /// higher attempt number, not a transition on this one.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskRunState::Succeeded
                | TaskRunState::Failed
                | TaskRunState::Expired
                | TaskRunState::Superseded
                | TaskRunState::Cancelled
                | TaskRunState::Lost
                | TaskRunState::Orphaned
        )
    }

    /// Whether `self -> next` is a legal edge.
    pub fn can_transition_to(self, next: TaskRunState) -> bool {
        matches!(
            (self, next),
            (TaskRunState::Scheduled, TaskRunState::Queued)
                | (TaskRunState::Scheduled, TaskRunState::Cancelled)
                | (TaskRunState::Scheduled, TaskRunState::Expired)
                | (TaskRunState::Scheduled, TaskRunState::Superseded)
                | (TaskRunState::Queued, TaskRunState::Claimed)
                | (TaskRunState::Queued, TaskRunState::Cancelled)
                | (TaskRunState::Queued, TaskRunState::Expired)
                | (TaskRunState::Queued, TaskRunState::Superseded)
                | (TaskRunState::Claimed, TaskRunState::Running)
                | (TaskRunState::Claimed, TaskRunState::Cancelled)
                | (TaskRunState::Claimed, TaskRunState::Expired)
                | (TaskRunState::Claimed, TaskRunState::Lost)
                | (TaskRunState::Running, TaskRunState::Succeeded)
                | (TaskRunState::Running, TaskRunState::Failed)
                | (TaskRunState::Running, TaskRunState::Cancelled)
                | (TaskRunState::Running, TaskRunState::Expired)
                | (TaskRunState::Running, TaskRunState::Lost)
                | (TaskRunState::Running, TaskRunState::Orphaned)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TaskRunStateConversionError {
    #[error("TaskRunState::Unspecified has no domain meaning")]
    UnspecifiedVariant,
}

impl TryFrom<generated::TaskRunState> for TaskRunState {
    type Error = TaskRunStateConversionError;

    fn try_from(raw: generated::TaskRunState) -> Result<Self, Self::Error> {
        match raw {
            generated::TaskRunState::Unspecified => {
                Err(TaskRunStateConversionError::UnspecifiedVariant)
            }
            generated::TaskRunState::Scheduled => Ok(TaskRunState::Scheduled),
            generated::TaskRunState::Queued => Ok(TaskRunState::Queued),
            generated::TaskRunState::Claimed => Ok(TaskRunState::Claimed),
            generated::TaskRunState::Running => Ok(TaskRunState::Running),
            generated::TaskRunState::Succeeded => Ok(TaskRunState::Succeeded),
            generated::TaskRunState::Failed => Ok(TaskRunState::Failed),
            generated::TaskRunState::Expired => Ok(TaskRunState::Expired),
            generated::TaskRunState::Superseded => Ok(TaskRunState::Superseded),
            generated::TaskRunState::Cancelled => Ok(TaskRunState::Cancelled),
            generated::TaskRunState::Lost => Ok(TaskRunState::Lost),
            generated::TaskRunState::Orphaned => Ok(TaskRunState::Orphaned),
        }
    }
}

impl From<TaskRunState> for generated::TaskRunState {
    fn from(state: TaskRunState) -> Self {
        match state {
            TaskRunState::Scheduled => generated::TaskRunState::Scheduled,
            TaskRunState::Queued => generated::TaskRunState::Queued,
            TaskRunState::Claimed => generated::TaskRunState::Claimed,
            TaskRunState::Running => generated::TaskRunState::Running,
            TaskRunState::Succeeded => generated::TaskRunState::Succeeded,
            TaskRunState::Failed => generated::TaskRunState::Failed,
            TaskRunState::Expired => generated::TaskRunState::Expired,
            TaskRunState::Superseded => generated::TaskRunState::Superseded,
            TaskRunState::Cancelled => generated::TaskRunState::Cancelled,
            TaskRunState::Lost => generated::TaskRunState::Lost,
            TaskRunState::Orphaned => generated::TaskRunState::Orphaned,
        }
    }
}
