//! Task and TaskRun records: how they are created, what links a run to its
//! task, and that `transition_to` only ever follows the legal transition
//! table.

mod support;

use std::collections::VecDeque;

use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{Task, TaskRun};
use kabudachi_core::protocol::records::{
    IllegalTransition, NewTask, TaskRunRecord, first_attempt, new_task,
};
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::time::{Duration, Instant};
use support::ids::SequentialIds;

fn submit(ids: &SequentialIds, now: Instant) -> Task {
    new_task(
        ids,
        now,
        NewTask::new(
            TaskDefinitionId::new("billing.charge"),
            3,
            b"input-bytes".to_vec(),
            "default",
        ),
    )
}

#[test]
fn new_task_records_what_the_caller_submitted() {
    let ids = SequentialIds::new();
    let task = submit(&ids, Instant::at(40));

    assert_eq!(task.task_id(), TaskId::new("task-1"));
    assert_eq!(
        task.task_definition_id(),
        TaskDefinitionId::new("billing.charge")
    );
    assert_eq!(task.source_version, 3);
    assert_eq!(task.serialized_input, b"input-bytes".to_vec());
    assert_eq!(task.queue, "default");
    assert_eq!(task.created_at_ticks, 40);
}

#[test]
fn every_submission_gets_its_own_task_id() {
    let ids = SequentialIds::new();
    let first = submit(&ids, Instant::at(0));
    let second = submit(&ids, Instant::at(0));

    assert_ne!(first.task_id(), second.task_id());
}

#[test]
fn first_attempt_points_at_its_task_with_attempt_number_one() {
    let ids = SequentialIds::new();
    let task = submit(&ids, Instant::at(5));
    let run = first_attempt(&task, &ids, Instant::at(7), TaskRunState::Queued);

    assert_eq!(run.task_id(), TaskId::new("task-1"));
    assert_eq!(run.task_run_id(), TaskRunId::new("run-1"));
    assert_eq!(run.attempt_number(), 1);
    assert_eq!(run.parent_task_run_id(), None);
}

#[test]
fn first_attempt_starts_in_the_requested_state_at_the_given_time() {
    let ids = SequentialIds::new();
    let task = submit(&ids, Instant::at(5));
    let run = first_attempt(&task, &ids, Instant::at(7), TaskRunState::Scheduled);

    assert_eq!(run.current_state(), TaskRunState::Scheduled);
    assert_eq!(run.created_at_ticks, 7);
    assert_eq!(run.updated_at_ticks, 7);
}

#[test]
fn first_attempt_executes_the_version_the_task_was_submitted_with() {
    let ids = SequentialIds::new();
    let task = submit(&ids, Instant::at(0));
    let run = first_attempt(&task, &ids, Instant::at(0), TaskRunState::Queued);

    assert_eq!(run.source_version, 3);
    assert_eq!(run.execution_version, 3);
}

#[test]
fn a_legal_transition_changes_state_and_stamps_the_time() {
    let ids = SequentialIds::new();
    let task = submit(&ids, Instant::at(0));
    let mut run = first_attempt(&task, &ids, Instant::at(1), TaskRunState::Queued);

    run.transition_to(TaskRunState::Claimed, Instant::at(9))
        .unwrap();

    assert_eq!(run.current_state(), TaskRunState::Claimed);
    assert_eq!(run.updated_at_ticks, 9);
    assert_eq!(run.created_at_ticks, 1);
}

#[test]
fn an_illegal_transition_is_rejected_and_leaves_the_run_untouched() {
    let ids = SequentialIds::new();
    let task = submit(&ids, Instant::at(0));
    let mut run = first_attempt(&task, &ids, Instant::at(1), TaskRunState::Queued);
    let before = run.clone();

    let error = run
        .transition_to(TaskRunState::Succeeded, Instant::at(9))
        .unwrap_err();

    assert_eq!(
        error,
        IllegalTransition {
            from: TaskRunState::Queued,
            to: TaskRunState::Succeeded,
        }
    );
    assert_eq!(run, before);
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
    let mut run = first_attempt(task, ids, Instant::at(0), start);
    for step in &legal_path(start, target)[1..] {
        run.transition_to(*step, Instant::at(0)).unwrap();
    }
    run
}

#[test]
fn a_terminal_run_never_moves_again() {
    let ids = SequentialIds::new();
    let task = submit(&ids, Instant::at(0));

    for terminal in TaskRunState::ALL.into_iter().filter(|s| s.is_terminal()) {
        for next in TaskRunState::ALL {
            let mut run = run_in_state(&ids, &task, terminal);
            let before = run.clone();

            let result = run.transition_to(next, Instant::at(1));

            assert!(result.is_err(), "{terminal:?} -> {next:?} must be rejected");
            assert_eq!(run, before);
        }
    }
}

#[test]
fn transition_to_follows_the_transition_table_for_every_pair() {
    let ids = SequentialIds::new();
    let task = submit(&ids, Instant::at(0));
    let later = Instant::at(0) + Duration::from_ticks(1);

    for from in TaskRunState::ALL {
        for to in TaskRunState::ALL {
            let mut run = run_in_state(&ids, &task, from);
            let before = run.clone();

            let result = run.transition_to(to, later);

            if from.can_transition_to(to) {
                assert!(result.is_ok(), "{from:?} -> {to:?} should be allowed");
                assert_eq!(run.current_state(), to, "{from:?} -> {to:?}");
                assert_eq!(run.updated_at_ticks, later.as_ticks(), "{from:?} -> {to:?}");
            } else {
                assert!(result.is_err(), "{from:?} -> {to:?} should be rejected");
                assert_eq!(run, before, "{from:?} -> {to:?}");
            }
        }
    }
}

#[test]
#[should_panic(expected = "required by protocol invariant")]
fn reading_the_state_of_a_run_with_no_state_panics() {
    let ids = SequentialIds::new();
    let task = submit(&ids, Instant::at(0));
    let mut run = first_attempt(&task, &ids, Instant::at(0), TaskRunState::Queued);
    run.state = 0;

    run.current_state();
}

#[test]
#[should_panic(expected = "required by protocol invariant")]
fn reading_the_id_of_a_run_with_no_identity_panics() {
    let ids = SequentialIds::new();
    let task = submit(&ids, Instant::at(0));
    let mut run = first_attempt(&task, &ids, Instant::at(0), TaskRunState::Queued);
    run.identity = None;

    run.task_run_id();
}

#[test]
#[should_panic(expected = "a TaskRun starts Scheduled or Queued")]
fn a_run_cannot_be_created_in_a_state_it_could_only_reach_by_transition() {
    let ids = SequentialIds::new();
    let task = submit(&ids, Instant::at(0));

    let _ = first_attempt(&task, &ids, Instant::at(0), TaskRunState::Running);
}
