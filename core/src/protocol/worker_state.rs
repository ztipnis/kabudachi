//! Domain `WorkerState` and its transition table (README §10.2, plus the
//! `Leader -> Fenced` edge from §10.4). The wire enum's `UNSPECIFIED` sentinel
//! has no domain meaning and is not represented.

use crate::protocol::generated;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerState {
    Bootstrapping,
    Joining,
    Active,
    LeaderSuspect,
    RollCall,
    Candidate,
    LeaderReconciling,
    Leader,
    NoQuorum,
    Draining,
    Fenced,
    Stopped,
}

impl WorkerState {
    /// Every variant, in definition order, for exhaustive iteration in tests.
    pub const ALL: [WorkerState; 12] = [
        WorkerState::Bootstrapping,
        WorkerState::Joining,
        WorkerState::Active,
        WorkerState::LeaderSuspect,
        WorkerState::RollCall,
        WorkerState::Candidate,
        WorkerState::LeaderReconciling,
        WorkerState::Leader,
        WorkerState::NoQuorum,
        WorkerState::Draining,
        WorkerState::Fenced,
        WorkerState::Stopped,
    ];

    /// A terminal state has no legal outgoing transition. `NoQuorum` and
    /// `Candidate` are not terminal: each has one outgoing edge.
    pub fn is_terminal(self) -> bool {
        matches!(self, WorkerState::Fenced | WorkerState::Stopped)
    }

    /// Whether `self -> next` is a legal edge.
    ///
    /// The README draws no losing-candidate edge, and `NoQuorum` has a single
    /// `-> RollCall` edge standing in for its several exit paths (the
    /// `Abandoned` outcome is shard-level, not a worker state).
    pub fn can_transition_to(self, next: WorkerState) -> bool {
        matches!(
            (self, next),
            (WorkerState::Bootstrapping, WorkerState::Joining)
                | (WorkerState::Joining, WorkerState::Active)
                | (WorkerState::Active, WorkerState::LeaderSuspect)
                | (WorkerState::Active, WorkerState::Draining)
                | (WorkerState::LeaderSuspect, WorkerState::RollCall)
                | (WorkerState::RollCall, WorkerState::Active)
                | (WorkerState::RollCall, WorkerState::Candidate)
                | (WorkerState::Candidate, WorkerState::LeaderReconciling)
                | (WorkerState::LeaderReconciling, WorkerState::Leader)
                | (WorkerState::Leader, WorkerState::NoQuorum)
                | (WorkerState::Leader, WorkerState::Draining)
                | (WorkerState::Leader, WorkerState::Fenced)
                | (WorkerState::NoQuorum, WorkerState::RollCall)
                | (WorkerState::Draining, WorkerState::Stopped)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WorkerStateConversionError {
    #[error("WorkerState::Unspecified has no domain meaning")]
    UnspecifiedVariant,
}

impl TryFrom<generated::WorkerState> for WorkerState {
    type Error = WorkerStateConversionError;

    fn try_from(raw: generated::WorkerState) -> Result<Self, Self::Error> {
        match raw {
            generated::WorkerState::Unspecified => {
                Err(WorkerStateConversionError::UnspecifiedVariant)
            }
            generated::WorkerState::Bootstrapping => Ok(WorkerState::Bootstrapping),
            generated::WorkerState::Joining => Ok(WorkerState::Joining),
            generated::WorkerState::Active => Ok(WorkerState::Active),
            generated::WorkerState::LeaderSuspect => Ok(WorkerState::LeaderSuspect),
            generated::WorkerState::RollCall => Ok(WorkerState::RollCall),
            generated::WorkerState::Candidate => Ok(WorkerState::Candidate),
            generated::WorkerState::LeaderReconciling => Ok(WorkerState::LeaderReconciling),
            generated::WorkerState::Leader => Ok(WorkerState::Leader),
            generated::WorkerState::NoQuorum => Ok(WorkerState::NoQuorum),
            generated::WorkerState::Draining => Ok(WorkerState::Draining),
            generated::WorkerState::Fenced => Ok(WorkerState::Fenced),
            generated::WorkerState::Stopped => Ok(WorkerState::Stopped),
        }
    }
}

impl From<WorkerState> for generated::WorkerState {
    fn from(state: WorkerState) -> Self {
        match state {
            WorkerState::Bootstrapping => generated::WorkerState::Bootstrapping,
            WorkerState::Joining => generated::WorkerState::Joining,
            WorkerState::Active => generated::WorkerState::Active,
            WorkerState::LeaderSuspect => generated::WorkerState::LeaderSuspect,
            WorkerState::RollCall => generated::WorkerState::RollCall,
            WorkerState::Candidate => generated::WorkerState::Candidate,
            WorkerState::LeaderReconciling => generated::WorkerState::LeaderReconciling,
            WorkerState::Leader => generated::WorkerState::Leader,
            WorkerState::NoQuorum => generated::WorkerState::NoQuorum,
            WorkerState::Draining => generated::WorkerState::Draining,
            WorkerState::Fenced => generated::WorkerState::Fenced,
            WorkerState::Stopped => generated::WorkerState::Stopped,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_states_have_the_correct_count() {
        assert_eq!(WorkerState::ALL.len(), 12);
    }

    #[test]
    fn all_states_are_unique() {
        use std::collections::HashSet;
        let unique: HashSet<_> = WorkerState::ALL.iter().cloned().collect();
        assert_eq!(unique.len(), 12);
    }

    #[test]
    fn legal_transitions_are_exactly_fourteen() {
        let legal = [
            (WorkerState::Bootstrapping, WorkerState::Joining),
            (WorkerState::Joining, WorkerState::Active),
            (WorkerState::Active, WorkerState::LeaderSuspect),
            (WorkerState::Active, WorkerState::Draining),
            (WorkerState::LeaderSuspect, WorkerState::RollCall),
            (WorkerState::RollCall, WorkerState::Active),
            (WorkerState::RollCall, WorkerState::Candidate),
            (WorkerState::Candidate, WorkerState::LeaderReconciling),
            (WorkerState::LeaderReconciling, WorkerState::Leader),
            (WorkerState::Leader, WorkerState::NoQuorum),
            (WorkerState::Leader, WorkerState::Draining),
            (WorkerState::Leader, WorkerState::Fenced),
            (WorkerState::NoQuorum, WorkerState::RollCall),
            (WorkerState::Draining, WorkerState::Stopped),
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

        for from in WorkerState::ALL.iter() {
            for to in WorkerState::ALL.iter() {
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
    fn exactly_two_states_are_terminal() {
        let terminal_states = [WorkerState::Fenced, WorkerState::Stopped];

        for state in WorkerState::ALL.iter() {
            let is_in_terminal_list = terminal_states.contains(state);
            let result = state.is_terminal();
            assert_eq!(
                result, is_in_terminal_list,
                "State {:?}: expected is_terminal() to return {}, got {}",
                state, is_in_terminal_list, result
            );
        }

        let terminal_count = WorkerState::ALL.iter().filter(|s| s.is_terminal()).count();
        assert_eq!(
            terminal_count, 2,
            "Expected exactly 2 terminal states, got {}",
            terminal_count
        );

        let non_terminal_count = WorkerState::ALL.iter().filter(|s| !s.is_terminal()).count();
        assert_eq!(
            non_terminal_count, 10,
            "Expected exactly 10 non-terminal states, got {}",
            non_terminal_count
        );
    }

    #[test]
    fn no_terminal_state_has_outgoing_transitions() {
        let terminal_states = [WorkerState::Fenced, WorkerState::Stopped];

        for terminal in terminal_states.iter() {
            for target in WorkerState::ALL.iter() {
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
        for state in WorkerState::ALL.iter() {
            let generated: generated::WorkerState = (*state).into();
            let back: WorkerState = generated.try_into().expect("round-trip should succeed");
            assert_eq!(*state, back, "Round-trip failed for {:?}", state);
        }
    }

    #[test]
    fn unspecified_conversion_returns_error() {
        let unspecified = generated::WorkerState::Unspecified;
        let result: Result<WorkerState, _> = unspecified.try_into();
        assert!(result.is_err(), "Expected Unspecified to produce an error");
        assert_eq!(
            result.unwrap_err(),
            WorkerStateConversionError::UnspecifiedVariant
        );
    }
}
