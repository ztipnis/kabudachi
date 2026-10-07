//! That `TaskRun::transition_to` only ever follows the legal transition table.

use std::collections::VecDeque;

use kabudachi_core::protocol::ids::{IdGenerator as _, TaskDefinitionId};
use kabudachi_core::protocol::messages::{Task, TaskRun};
use kabudachi_core::protocol::records::{
    IllegalTransition, NewTask, TaskRunRecord, first_attempt, new_task,
};
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::time::WallTime;

use crate::support::ids::SequentialIds;

fn at(millis: u64) -> WallTime {
    WallTime::from_unix_millis(millis)
}

fn submit(ids: &SequentialIds, at: WallTime) -> Task {
    new_task(NewTask::new(
        ids.next_task_id(),
        at,
        TaskDefinitionId::new("billing.charge"),
        3,
        b"input-bytes".to_vec(),
        "default",
    ))
}

/// The shortest chain of legal transitions from `start` to `target`,
/// including both ends.
fn legal_path(start: TaskRunState, target: TaskRunState) -> Vec<TaskRunState> {
    let mut routes = VecDeque::from([vec![start]]);
    while let Some(route) = routes.pop_front() {
        let tail = *route.last().unwrap();
        if tail == target {
            return route;
        }
        for next in TaskRunState::ALL {
            if tail.can_transition_to(next) && !route.contains(&next) {
                routes.push_back([route.clone(), vec![next]].concat());
            }
        }
    }
    panic!("{target:?} is unreachable from {start:?}");
}

/// A run in `target`, reached through legal transitions the way a real run
/// gets there.
fn run_in_state(ids: &SequentialIds, task: &Task, target: TaskRunState) -> TaskRun {
    let start = if target == TaskRunState::Scheduled {
        TaskRunState::Scheduled
    } else {
        TaskRunState::Queued
    };
    let mut run = first_attempt(task, ids, at(0), start);
    for step in &legal_path(start, target)[1..] {
        run.transition_to(*step, at(0)).unwrap();
    }
    run
}

#[test]
fn transition_to_follows_the_transition_table_for_every_pair() {
    let ids = SequentialIds::new();
    let task = submit(&ids, at(0));
    let later = at(1);

    for from in TaskRunState::ALL {
        for to in TaskRunState::ALL {
            let mut run = run_in_state(&ids, &task, from);
            let before = run.clone();

            let result = run.transition_to(to, later);

            if from.can_transition_to(to) {
                assert!(result.is_ok(), "{from:?} -> {to:?} should be allowed");
                assert_eq!(run.current_state(), to, "{from:?} -> {to:?}");
                assert_eq!(run.updated_at, Some(later.into()), "{from:?} -> {to:?}");
            } else {
                assert_eq!(
                    result,
                    Err(IllegalTransition { from, to }),
                    "{from:?} -> {to:?} should be rejected"
                );
                assert_eq!(run, before, "{from:?} -> {to:?}");
            }
        }
    }
}
