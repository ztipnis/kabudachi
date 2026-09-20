//! Domain `TaskRunState` and its transition table (README §4.4, §25.1). The
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_states_have_the_correct_count() {
        assert_eq!(TaskRunState::ALL.len(), 11);
    }

    #[test]
    fn all_states_are_unique() {
        use std::collections::HashSet;
        let unique: HashSet<_> = TaskRunState::ALL.iter().cloned().collect();
        assert_eq!(unique.len(), 11);
    }

    #[test]
    fn legal_transitions_are_exactly_eighteen() {
        let legal = [
            (TaskRunState::Scheduled, TaskRunState::Queued),
            (TaskRunState::Scheduled, TaskRunState::Cancelled),
            (TaskRunState::Scheduled, TaskRunState::Expired),
            (TaskRunState::Scheduled, TaskRunState::Superseded),
            (TaskRunState::Queued, TaskRunState::Claimed),
            (TaskRunState::Queued, TaskRunState::Cancelled),
            (TaskRunState::Queued, TaskRunState::Expired),
            (TaskRunState::Queued, TaskRunState::Superseded),
            (TaskRunState::Claimed, TaskRunState::Running),
            (TaskRunState::Claimed, TaskRunState::Cancelled),
            (TaskRunState::Claimed, TaskRunState::Expired),
            (TaskRunState::Claimed, TaskRunState::Lost),
            (TaskRunState::Running, TaskRunState::Succeeded),
            (TaskRunState::Running, TaskRunState::Failed),
            (TaskRunState::Running, TaskRunState::Cancelled),
            (TaskRunState::Running, TaskRunState::Expired),
            (TaskRunState::Running, TaskRunState::Lost),
            (TaskRunState::Running, TaskRunState::Orphaned),
        ];

        for (from, to) in legal.iter() {
            assert!(
                from.can_transition_to(*to),
                "Expected legal transition ({:?}, {:?}) to return true",
                from,
                to
            );
        }

        let mut legal_set = std::collections::HashSet::new();
        for (from, to) in legal.iter() {
            legal_set.insert((*from, *to));
        }

        for from in TaskRunState::ALL.iter() {
            for to in TaskRunState::ALL.iter() {
                let is_in_table = legal_set.contains(&(*from, *to));
                let result = from.can_transition_to(*to);
                assert_eq!(
                    result, is_in_table,
                    "Transition ({:?}, {:?}): expected {}, got {}",
                    from, to, is_in_table, result
                );
            }
        }
    }

    #[test]
    fn exactly_seven_states_are_terminal() {
        let terminal_states = [
            TaskRunState::Succeeded,
            TaskRunState::Failed,
            TaskRunState::Expired,
            TaskRunState::Superseded,
            TaskRunState::Cancelled,
            TaskRunState::Lost,
            TaskRunState::Orphaned,
        ];

        for state in TaskRunState::ALL.iter() {
            let is_in_terminal_list = terminal_states.contains(state);
            let result = state.is_terminal();
            assert_eq!(
                result, is_in_terminal_list,
                "State {:?}: expected is_terminal() to return {}, got {}",
                state, is_in_terminal_list, result
            );
        }

        let terminal_count = TaskRunState::ALL.iter().filter(|s| s.is_terminal()).count();
        assert_eq!(
            terminal_count, 7,
            "Expected exactly 7 terminal states, got {}",
            terminal_count
        );

        let non_terminal_count = TaskRunState::ALL
            .iter()
            .filter(|s| !s.is_terminal())
            .count();
        assert_eq!(
            non_terminal_count, 4,
            "Expected exactly 4 non-terminal states, got {}",
            non_terminal_count
        );
    }

    #[test]
    fn no_terminal_state_has_outgoing_transitions() {
        let terminal_states = [
            TaskRunState::Succeeded,
            TaskRunState::Failed,
            TaskRunState::Expired,
            TaskRunState::Superseded,
            TaskRunState::Cancelled,
            TaskRunState::Lost,
            TaskRunState::Orphaned,
        ];

        for terminal in terminal_states.iter() {
            for target in TaskRunState::ALL.iter() {
                assert!(
                    !terminal.can_transition_to(*target),
                    "Terminal state {:?} should not have outgoing transition to {:?}",
                    terminal,
                    target
                );
            }
        }
    }

    #[test]
    fn conversions_round_trip_successfully() {
        for state in TaskRunState::ALL.iter() {
            let generated: generated::TaskRunState = (*state).into();
            let back: TaskRunState = generated.try_into().expect("round-trip should succeed");
            assert_eq!(*state, back, "Round-trip failed for {:?}", state);
        }
    }

    #[test]
    fn unspecified_conversion_returns_error() {
        let unspecified = generated::TaskRunState::Unspecified;
        let result: Result<TaskRunState, _> = unspecified.try_into();
        assert!(result.is_err(), "Expected Unspecified to produce an error");
        assert_eq!(
            result.unwrap_err(),
            TaskRunStateConversionError::UnspecifiedVariant
        );
    }
}
