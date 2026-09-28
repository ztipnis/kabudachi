//! Domain `WorkerState` and its transition table (README §10.2, plus
//! ADR-0001's step-down, deadline, `NoQuorum` exit, authority-path and
//! orphaning edges). No message carries a worker's state.

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

    /// A terminal state has no legal outgoing transition.
    pub fn is_terminal(self) -> bool {
        self == WorkerState::Stopped
    }

    /// Whether `self -> next` is a legal edge.
    ///
    /// Beyond the README's sketch, `LeaderSuspect -> Active` lets an ack
    /// from the leader end a suspicion before any roll call starts: a
    /// pending member, which starts none, has no other way back. A node that
    /// holds or contests a term steps down once it sees a later one
    /// (ADR-0001 decision 14): from `RollCall`, `Candidate` or `Leader`, to
    /// `Active` under that term's leader or to `LeaderSuspect` to contest
    /// again. `LeaderReconciling` has no such edge: a winner passes through
    /// it to `Leader` in one step, so no input ever finds a node there. A
    /// roll call or a vote that misses its deadline leaves `RollCall` or
    /// `Candidate` for `LeaderSuspect` too, or, short of a quorum, `RollCall`
    /// for `NoQuorum` (ADR-0001 decisions 13 and 15). A `NoQuorum` node
    /// leaves by a roll call of its own (`-> RollCall`) or by an ack from a
    /// leader (`-> Active`).
    ///
    /// With a coordination authority (ADR-0001 decisions 11 and 12): a
    /// `NoQuorum` node whose authority path swaps the recovery epoch stands
    /// (`-> Candidate`) while it waits out the recovery fence, and goes back
    /// (`Candidate -> NoQuorum`) if the fence is refused for good; one that
    /// finds the epoch missing stops, its shard abandoned (`-> Stopped`). A
    /// node that fails to renew its registration fences itself (`-> Fenced`)
    /// from any state that takes part in elections, and on reconnecting
    /// resumes (`Fenced -> Active`) or, if the epoch is no longer its own,
    /// rejoins (`Fenced -> Bootstrapping`); a `NoQuorum` node whose
    /// authority path finds an epoch it cannot recover from rejoins it the
    /// same way (`NoQuorum -> Bootstrapping`).
    pub fn can_transition_to(self, next: WorkerState) -> bool {
        matches!(
            (self, next),
            (WorkerState::Bootstrapping, WorkerState::Joining)
                | (WorkerState::Joining, WorkerState::Active)
                | (WorkerState::Active, WorkerState::LeaderSuspect)
                | (WorkerState::Active, WorkerState::Draining)
                | (WorkerState::Active, WorkerState::Fenced)
                | (WorkerState::LeaderSuspect, WorkerState::Active)
                | (WorkerState::LeaderSuspect, WorkerState::RollCall)
                | (WorkerState::LeaderSuspect, WorkerState::Fenced)
                | (WorkerState::RollCall, WorkerState::Active)
                | (WorkerState::RollCall, WorkerState::LeaderSuspect)
                | (WorkerState::RollCall, WorkerState::Candidate)
                | (WorkerState::RollCall, WorkerState::NoQuorum)
                | (WorkerState::RollCall, WorkerState::Fenced)
                | (WorkerState::Candidate, WorkerState::Active)
                | (WorkerState::Candidate, WorkerState::LeaderSuspect)
                | (WorkerState::Candidate, WorkerState::LeaderReconciling)
                | (WorkerState::Candidate, WorkerState::NoQuorum)
                | (WorkerState::Candidate, WorkerState::Fenced)
                | (WorkerState::LeaderReconciling, WorkerState::Leader)
                | (WorkerState::Leader, WorkerState::Active)
                | (WorkerState::Leader, WorkerState::LeaderSuspect)
                | (WorkerState::Leader, WorkerState::NoQuorum)
                | (WorkerState::Leader, WorkerState::Draining)
                | (WorkerState::Leader, WorkerState::Fenced)
                | (WorkerState::NoQuorum, WorkerState::Active)
                | (WorkerState::NoQuorum, WorkerState::RollCall)
                | (WorkerState::NoQuorum, WorkerState::Candidate)
                | (WorkerState::NoQuorum, WorkerState::Stopped)
                | (WorkerState::NoQuorum, WorkerState::Bootstrapping)
                | (WorkerState::NoQuorum, WorkerState::Fenced)
                | (WorkerState::Fenced, WorkerState::Active)
                | (WorkerState::Fenced, WorkerState::Bootstrapping)
                | (WorkerState::Draining, WorkerState::Stopped)
        )
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
    fn legal_transitions_are_exactly_the_table() {
        let legal = [
            (WorkerState::Bootstrapping, WorkerState::Joining),
            (WorkerState::Joining, WorkerState::Active),
            (WorkerState::Active, WorkerState::LeaderSuspect),
            (WorkerState::Active, WorkerState::Draining),
            (WorkerState::Active, WorkerState::Fenced),
            (WorkerState::LeaderSuspect, WorkerState::Active),
            (WorkerState::LeaderSuspect, WorkerState::RollCall),
            (WorkerState::LeaderSuspect, WorkerState::Fenced),
            (WorkerState::RollCall, WorkerState::Active),
            (WorkerState::RollCall, WorkerState::LeaderSuspect),
            (WorkerState::RollCall, WorkerState::Candidate),
            (WorkerState::RollCall, WorkerState::NoQuorum),
            (WorkerState::RollCall, WorkerState::Fenced),
            (WorkerState::Candidate, WorkerState::Active),
            (WorkerState::Candidate, WorkerState::LeaderSuspect),
            (WorkerState::Candidate, WorkerState::LeaderReconciling),
            (WorkerState::Candidate, WorkerState::NoQuorum),
            (WorkerState::Candidate, WorkerState::Fenced),
            (WorkerState::LeaderReconciling, WorkerState::Leader),
            (WorkerState::Leader, WorkerState::Active),
            (WorkerState::Leader, WorkerState::LeaderSuspect),
            (WorkerState::Leader, WorkerState::NoQuorum),
            (WorkerState::Leader, WorkerState::Draining),
            (WorkerState::Leader, WorkerState::Fenced),
            (WorkerState::NoQuorum, WorkerState::Active),
            (WorkerState::NoQuorum, WorkerState::RollCall),
            (WorkerState::NoQuorum, WorkerState::Candidate),
            (WorkerState::NoQuorum, WorkerState::Stopped),
            (WorkerState::NoQuorum, WorkerState::Bootstrapping),
            (WorkerState::NoQuorum, WorkerState::Fenced),
            (WorkerState::Fenced, WorkerState::Active),
            (WorkerState::Fenced, WorkerState::Bootstrapping),
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
    fn only_stopped_is_terminal() {
        for state in WorkerState::ALL {
            assert_eq!(state.is_terminal(), state == WorkerState::Stopped, "{state:?}");
            if state.is_terminal() {
                assert!(
                    WorkerState::ALL
                        .iter()
                        .all(|next| !state.can_transition_to(*next)),
                    "terminal {state:?} has an outgoing transition"
                );
            }
        }
    }
}
