//! Property tests for `TaskRunState` (README §25.1/§25.2): randomized
//! *sequences* of attempted transitions, checking that the terminal/non-terminal
//! classification holds under arbitrary exploration. The exhaustive pairwise
//! transition table is tested in `core/src/protocol/task.rs`.

use kabudachi_core::protocol::task::TaskRunState;
use proptest::prelude::*;

/// A uniformly-random `TaskRunState`. `core/src/` shouldn't carry test-only
/// `Arbitrary` impls, so this is a `prop_oneof!` of `Just(..)` per variant. It
/// deliberately includes illegal attempted targets: rejecting them is the point.
fn any_task_run_state() -> impl Strategy<Value = TaskRunState> {
    prop_oneof![
        Just(TaskRunState::Scheduled),
        Just(TaskRunState::Queued),
        Just(TaskRunState::Claimed),
        Just(TaskRunState::Running),
        Just(TaskRunState::Succeeded),
        Just(TaskRunState::Failed),
        Just(TaskRunState::Expired),
        Just(TaskRunState::Superseded),
        Just(TaskRunState::Cancelled),
        Just(TaskRunState::Lost),
        Just(TaskRunState::Orphaned),
    ]
}

proptest! {
    /// Once a walk reaches a terminal state, `can_transition_to` is `false` for
    /// every target for the rest of the sequence. The walk starts at
    /// `Scheduled` and moves only when `can_transition_to` allows it; an illegal
    /// attempt leaves it parked.
    #[test]
    fn terminal_state_has_no_further_legal_transitions(attempts in proptest::collection::vec(any_task_run_state(), 1..50)) {
        let mut current = TaskRunState::Scheduled;
        let mut reached_terminal = false;

        for attempted_next in attempts {
            if reached_terminal {
                // Once terminal, every target must be illegal, on every remaining step.
                for target in TaskRunState::ALL {
                    prop_assert!(
                        !current.can_transition_to(target),
                        "terminal state {current:?} must have no legal transition, but \
                         can_transition_to({target:?}) returned true"
                    );
                }
                continue;
            }

            if current.can_transition_to(attempted_next) {
                current = attempted_next;
                if current.is_terminal() {
                    reached_terminal = true;
                    for target in TaskRunState::ALL {
                        prop_assert!(
                            !current.can_transition_to(target),
                            "state {current:?} was just entered and classified terminal, but \
                             can_transition_to({target:?}) returned true"
                        );
                    }
                }
            }
            // Illegal attempt: `current` is unchanged.
        }
    }

    /// Every state the walk visits agrees with `is_terminal()`: a non-terminal
    /// state has at least one legal outgoing transition and a terminal one has
    /// none. A light check of the table's structure under random walks, not a
    /// full reachability proof.
    #[test]
    fn every_visited_state_agrees_with_terminal_classification(attempts in proptest::collection::vec(any_task_run_state(), 1..50)) {
        let mut current = TaskRunState::Scheduled;
        let mut visited = vec![current];

        for attempted_next in attempts {
            if current.can_transition_to(attempted_next) {
                current = attempted_next;
                visited.push(current);
            }
        }

        for state in visited {
            let has_any_legal_outgoing = TaskRunState::ALL.iter().any(|&target| state.can_transition_to(target));
            if state.is_terminal() {
                prop_assert!(
                    !has_any_legal_outgoing,
                    "terminal state {state:?} unexpectedly has a legal outgoing transition"
                );
            } else {
                prop_assert!(
                    has_any_legal_outgoing,
                    "non-terminal state {state:?} unexpectedly has NO legal outgoing transition \
                     (would be a dead end the table doesn't document as terminal)"
                );
            }
        }
    }
}
