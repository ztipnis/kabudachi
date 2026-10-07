//! Task and TaskRun records: how they are created, what links a run to its
//! task, and that `transition_to` only ever follows the legal transition
//! table.


use std::collections::VecDeque;

use kabudachi_core::protocol::ids::{IdGenerator as _, TaskDefinitionId};
use kabudachi_core::protocol::messages::{Task, TaskRun};
use kabudachi_core::protocol::records::{
    IllegalTransition, NewTask, TaskRunRecord, first_attempt, new_task,
};
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{Scheduler, Submission};
use kabudachi_core::time::WallTime;

use crate::support::clock::FakeClock;
use crate::support::ids::{OversizedIds, SequentialIds};

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

#[test]
#[should_panic(expected = "required by protocol invariant")]
fn reading_the_state_of_a_run_with_no_state_panics() {
    let ids = SequentialIds::new();
    let task = submit(&ids, at(0));
    let mut run = first_attempt(&task, &ids, at(0), TaskRunState::Queued);
    run.state = 0;

    run.current_state();
}

#[test]
#[should_panic(expected = "required by protocol invariant")]
fn reading_the_id_of_a_run_with_no_identity_panics() {
    let ids = SequentialIds::new();
    let task = submit(&ids, at(0));
    let mut run = first_attempt(&task, &ids, at(0), TaskRunState::Queued);
    run.identity = None;

    run.task_run_id();
}

#[test]
#[should_panic(expected = "a TaskRun starts Scheduled or Queued")]
fn a_run_cannot_be_created_in_a_state_it_could_only_reach_by_transition() {
    let ids = SequentialIds::new();
    let task = submit(&ids, at(0));

    let _ = first_attempt(&task, &ids, at(0), TaskRunState::Running);
}

#[test]
#[should_panic(expected = "task ID of")]
fn a_task_cannot_be_submitted_under_an_oversized_id() {
    let scheduler = Scheduler::new(FakeClock::new(), OversizedIds);

    let _ = scheduler.mint(Submission::new(
        TaskDefinitionId::new("billing.charge"),
        3,
        b"input-bytes".to_vec(),
        "default",
    ));
}

#[test]
#[should_panic(expected = "task run ID of")]
fn a_run_cannot_be_created_under_an_oversized_id() {
    let task = submit(&SequentialIds::new(), at(0));
    first_attempt(&task, &OversizedIds, at(0), TaskRunState::Queued);
}
