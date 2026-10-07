//! The observer seam: what a scheduler tells whoever watches it, in what
//! order, and with which counts. A missed or misplaced notification would
//! show up here or in `Fixture::state`'s cross-check.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    CaughtUp, Completion, Counts, Event, LeadershipGrant, LeaseEnd, MemoryLimits, Submission,
};
use kabudachi_core::time::Instant;

use crate::support::grant::unbounded_grant;
use crate::support::scheduler::{Fixture, ticks};
use crate::support::spy::{Note, Noted};

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn bytes(size: usize) -> Submission {
    Submission::new(
        TaskDefinitionId::new("index.refresh"),
        0,
        vec![0; size],
        "default",
    )
}

fn note(change: Noted, pending: usize, memory_in_use: u64) -> Note {
    Note {
        change,
        counts: Counts {
            pending,
            memory_in_use,
        },
    }
}

/// The note of `task`'s current run in `state`.
fn run(fixture: &Fixture, task: &TaskId, state: TaskRunState) -> Noted {
    Noted::Run {
        task: task.clone(),
        run: fixture.spy.run_of(task).task_run_id(),
        state,
    }
}

#[test]
fn one_submit_is_notified_change_by_change_in_the_order_of_its_events() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits {
        soft: 10,
        hard: 100,
    });
    let older = fixture
        .scheduler
        .submit(bytes(8).with_coalescing_key("k"))
        .unwrap();
    let mark = fixture.spy.mark();

    let newer = fixture
        .scheduler
        .submit(bytes(8).with_coalescing_key("k"))
        .unwrap();

    assert_eq!(
        fixture.spy.since(mark),
        vec![
            note(Noted::TaskRecorded(newer.clone()), 2, 8),
            note(run(&fixture, &newer, TaskRunState::Queued), 2, 8),
            note(Noted::Memory, 2, 16),
            note(run(&fixture, &older, TaskRunState::Superseded), 1, 16),
            note(Noted::SlowDown(true), 1, 16),
        ]
    );
    assert_eq!(
        fixture.scheduler.take_events(),
        vec![
            Event::Superseded {
                task_id: older.clone(),
                task_run_id: fixture.spy.run_of(&older).task_run_id(),
                by: newer,
            },
            Event::SlowDown { active: true },
        ]
    );
}

#[test]
fn a_cancellation_is_notified_after_the_memory_it_frees_as_its_events_are() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits {
        soft: 10,
        hard: 100,
    });
    let task = fixture.scheduler.submit(bytes(11)).unwrap();
    fixture.scheduler.take_events(); // SlowDown was raised.
    let mark = fixture.spy.mark();

    fixture.scheduler.cancel(&task).unwrap();

    assert_eq!(
        fixture.spy.since(mark),
        vec![
            note(Noted::Memory, 0, 0),
            note(Noted::SlowDown(false), 0, 0),
            note(run(&fixture, &task, TaskRunState::Cancelled), 0, 0),
        ]
    );
    assert_eq!(
        fixture.scheduler.take_events(),
        vec![
            Event::SlowDown { active: false },
            Event::Cancelled {
                task_id: task.clone(),
                task_run_id: fixture.spy.run_of(&task).task_run_id(),
                was_running: false,
            },
        ]
    );
}

#[test]
fn one_catch_up_notifies_expiries_then_releases_then_forgetting() {
    let mut fixture = Fixture::leading();
    fixture.scheduler.set_result_ttl(Some(ticks(10)));
    let finished = fixture.scheduler.submit(bytes(0)).unwrap();
    let claim = fixture
        .scheduler
        .request_claim(&worker(), &finished)
        .unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .complete(
            &worker(),
            &claim.task_run_id,
            Digest::blake3(b"d"),
            Completion::Final,
        )
        .unwrap();
    let expiring = fixture
        .scheduler
        .submit(bytes(0).with_expiry(ticks(10)))
        .unwrap();
    let delayed = fixture
        .scheduler
        .submit(bytes(0).with_delay(ticks(10)))
        .unwrap();
    fixture.clock.advance(ticks(10));
    let mark = fixture.spy.mark();

    fixture.scheduler.catch_up();

    // Empty payloads: no memory changes to notify.
    assert_eq!(
        fixture.spy.since(mark),
        vec![
            note(run(&fixture, &expiring, TaskRunState::Expired), 0, 0),
            note(run(&fixture, &delayed, TaskRunState::Queued), 1, 0),
            note(Noted::TaskForgotten(finished), 1, 0),
        ]
    );
}

#[test]
fn leadership_is_notified_when_it_changes_and_a_lapse_once_a_call_finds_it() {
    let mut fixture = Fixture::not_leading();
    let until_10 = LeadershipGrant {
        valid_until: LeaseEnd::At(Instant::at(10)),
        ..unbounded_grant()
    };

    fixture.scheduler.set_leadership_grant(Some(until_10));
    fixture.scheduler.set_leadership_grant(Some(until_10));
    assert_eq!(
        fixture.spy.since(0),
        vec![note(Noted::Leadership(true), 0, 0)],
        "a grant that changes nothing about leading is not a change"
    );

    fixture.clock.advance(ticks(10));
    let mark = fixture.spy.mark();
    assert_eq!(fixture.scheduler.catch_up(), CaughtUp::default());
    assert_eq!(
        fixture.spy.since(mark),
        vec![note(Noted::Leadership(false), 0, 0)],
        "catch_up finds the lapse by the scheduler's own clock"
    );

    let refused = fixture
        .scheduler
        .request_claim(&worker(), &TaskId::new("unknown"));

    assert!(refused.is_err());
    assert_eq!(fixture.spy.since(mark).len(), 1, "the lapse is told once");
}
