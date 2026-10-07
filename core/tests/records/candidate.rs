//! Which records a worker treats as claimable work when it looks in its own
//! store: waiting, due, not expired, not over.

use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, Uuid7Ids};
use kabudachi_core::protocol::records::{NewTask, TaskRunRecord, first_attempt, new_task};
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::task_record::looks_claimable;
use kabudachi_core::time::{Duration, WallTime};

const SUBMITTED: u64 = 1_700_000_000_000;

/// A record whose only run is in `state`, reached by legal transitions.
fn record(state: TaskRunState, delay: Option<u64>, expiry: Option<u64>) -> TaskRecord {
    let at = WallTime::from_unix_millis(SUBMITTED);
    let mut new = NewTask::new(
        TaskId::new("t"),
        at,
        TaskDefinitionId::new("d"),
        0,
        Vec::new(),
        "default",
    );
    new.delay = delay.map(Duration::from_millis);
    new.expiry = expiry.map(Duration::from_millis);
    let task = new_task(new);
    let start = if state == TaskRunState::Scheduled {
        TaskRunState::Scheduled
    } else {
        TaskRunState::Queued
    };
    let mut run = first_attempt(&task, &Uuid7Ids, at, start);
    let path: &[TaskRunState] = match state {
        TaskRunState::Scheduled | TaskRunState::Queued => &[],
        TaskRunState::Claimed => &[TaskRunState::Claimed],
        TaskRunState::Running => &[TaskRunState::Claimed, TaskRunState::Running],
        TaskRunState::Succeeded => &[
            TaskRunState::Claimed,
            TaskRunState::Running,
            TaskRunState::Succeeded,
        ],
        TaskRunState::Superseded => &[TaskRunState::Superseded],
        other => panic!("no path to {other:?} in this test"),
    };
    for step in path {
        run.transition_to(*step, at).unwrap();
    }
    TaskRecord {
        task: Some(task),
        runs: vec![run],
        ..TaskRecord::default()
    }
}

fn at(offset: u64) -> WallTime {
    WallTime::from_unix_millis(SUBMITTED + offset)
}

#[test]
fn a_queued_run_is_claimable_and_a_claimed_or_finished_one_is_not() {
    assert!(looks_claimable(&record(TaskRunState::Queued, None, None), at(0)));
    for state in [
        TaskRunState::Claimed,
        TaskRunState::Running,
        TaskRunState::Superseded,
        TaskRunState::Succeeded,
    ] {
        assert!(!looks_claimable(&record(state, None, None), at(0)), "{state:?}");
    }
    let mut over = record(TaskRunState::Queued, None, None);
    over.finished = true;
    assert!(!looks_claimable(&over, at(0)), "a finished record is never work");
}

#[test]
fn a_scheduled_run_is_claimable_once_its_delay_has_passed_by_the_wall_clock() {
    let delayed = record(TaskRunState::Scheduled, Some(500), None);
    assert!(!looks_claimable(&delayed, at(499)));
    assert!(looks_claimable(&delayed, at(500)));
}

#[test]
fn a_run_past_its_expiry_is_not_claimable() {
    let expiring = record(TaskRunState::Queued, None, Some(1_000));
    assert!(looks_claimable(&expiring, at(999)));
    assert!(!looks_claimable(&expiring, at(1_000)));
}
