//! Delayed submission and expiry: a task with a delay waits `Scheduled` and
//! becomes `Queued` only when due, and a pending task that outlives its
//! expiry becomes `Expired` instead of running late. The leader decides both,
//! whenever time is advanced or a claim is made.

mod support;

use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    ClaimRejection, Event, LeadershipGrant, LeaseEnd, Scheduler, Submission,
};
use kabudachi_core::time::{Clock, Duration, Instant};
use support::clock::FakeClock;
use support::grant::unbounded_grant;
use support::ids::SequentialIds;

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn ticks(n: u64) -> Duration {
    Duration::from_ticks(n)
}

struct Fixture {
    clock: FakeClock,
    scheduler: Scheduler<FakeClock, SequentialIds>,
}

fn leading() -> Fixture {
    let clock = FakeClock::new();
    let mut scheduler = Scheduler::new(clock.clone(), SequentialIds::new());
    scheduler.set_leadership_grant(Some(unbounded_grant()));
    Fixture { clock, scheduler }
}

fn plain() -> Submission {
    Submission::new(
        TaskDefinitionId::new("billing.charge"),
        0,
        b"in".to_vec(),
        "default",
    )
}

fn state(fixture: &Fixture, task: &TaskId) -> TaskRunState {
    fixture.scheduler.run_of(task).unwrap().current_state()
}

#[test]
fn a_delayed_task_waits_scheduled_and_is_not_pending() {
    let mut fixture = leading();

    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)))
        .unwrap();

    assert_eq!(state(&fixture, &task), TaskRunState::Scheduled);
    assert!(fixture.scheduler.pending_tasks().is_empty());
    assert_eq!(
        fixture.scheduler.task(&task).unwrap().not_before_ticks,
        Some(100)
    );
}

#[test]
fn a_task_that_is_not_yet_due_cannot_be_claimed() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)))
        .unwrap();

    let claim = fixture.scheduler.request_claim(&worker(), &task);
    let oldest = fixture.scheduler.claim_oldest(&worker(), 10).unwrap();

    assert_eq!(claim.unwrap_err(), ClaimRejection::NotReady);
    assert!(oldest.is_empty());
}

#[test]
fn a_delayed_task_becomes_queued_when_due_and_not_before() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)))
        .unwrap();

    fixture.clock.advance(ticks(99));
    let early = fixture.scheduler.advance();
    assert_eq!(early.queued, 0);
    assert_eq!(state(&fixture, &task), TaskRunState::Scheduled);

    fixture.clock.advance(ticks(1));
    let due = fixture.scheduler.advance();
    assert_eq!(due.queued, 1);
    assert_eq!(state(&fixture, &task), TaskRunState::Queued);
    assert_eq!(fixture.scheduler.pending_tasks(), vec![task]);
}

#[test]
fn a_task_that_became_due_is_claimed_without_anyone_advancing_time() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(50)))
        .unwrap();
    fixture.clock.advance(ticks(50));

    let claims = fixture.scheduler.claim_oldest(&worker(), 10).unwrap();

    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].task.task_id(), task);
}

#[test]
fn a_delay_of_nothing_queues_the_task_at_once() {
    let mut fixture = leading();

    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(0)))
        .unwrap();

    assert_eq!(state(&fixture, &task), TaskRunState::Queued);
}

#[test]
fn delayed_tasks_join_the_queue_in_order_of_when_they_became_due() {
    let mut fixture = leading();
    let later = fixture
        .scheduler
        .submit(plain().with_delay(ticks(200)))
        .unwrap();
    let sooner = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)))
        .unwrap();
    let immediate = fixture.scheduler.submit(plain()).unwrap();

    fixture.clock.advance(ticks(300));
    fixture.scheduler.advance();

    assert_eq!(
        fixture.scheduler.pending_tasks(),
        vec![immediate, sooner, later]
    );
}

#[test]
fn a_pending_task_expires_instead_of_running_late() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(100)))
        .unwrap();

    fixture.clock.advance(ticks(99));
    assert_eq!(fixture.scheduler.advance().expired, 0);
    assert_eq!(state(&fixture, &task), TaskRunState::Queued);

    fixture.clock.advance(ticks(1));
    assert_eq!(fixture.scheduler.advance().expired, 1);
    assert_eq!(state(&fixture, &task), TaskRunState::Expired);
    assert!(fixture.scheduler.pending_tasks().is_empty());
}

#[test]
fn an_expired_task_is_never_handed_out_even_if_nobody_advanced_time() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(10)))
        .unwrap();
    fixture.clock.advance(ticks(10));

    let claims = fixture.scheduler.claim_oldest(&worker(), 10).unwrap();

    assert!(claims.is_empty());
    assert_eq!(state(&fixture, &task), TaskRunState::Expired);
}

#[test]
fn claiming_an_expired_task_says_it_finished() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(10)))
        .unwrap();
    fixture.clock.advance(ticks(10));

    let result = fixture.scheduler.request_claim(&worker(), &task);

    assert_eq!(result.unwrap_err(), ClaimRejection::Finished);
}

#[test]
fn a_scheduled_task_can_expire_before_it_is_ever_due() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)).with_expiry(ticks(50)))
        .unwrap();

    fixture.clock.advance(ticks(200));
    let outcome = fixture.scheduler.advance();

    assert_eq!(outcome.expired, 1);
    assert_eq!(outcome.queued, 0);
    assert_eq!(state(&fixture, &task), TaskRunState::Expired);
    assert!(fixture.scheduler.pending_tasks().is_empty());
}

#[test]
fn a_task_already_claimed_does_not_expire() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(10)))
        .unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();

    fixture.clock.advance(ticks(1_000));
    let outcome = fixture.scheduler.advance();

    assert_eq!(outcome.expired, 0);
    assert_eq!(state(&fixture, &task), TaskRunState::Claimed);
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    assert!(
        fixture
            .scheduler
            .complete(&worker(), &claim.task_run_id, b"d".to_vec())
            .is_ok()
    );
}

#[test]
fn expiry_is_only_about_starting_so_a_retry_never_expires() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_retries(2).with_expiry(ticks(100)))
        .unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .fail(&worker(), &claim.task_run_id, "ValueError")
        .unwrap();

    fixture.clock.advance(ticks(1_000));
    let outcome = fixture.scheduler.advance();

    assert_eq!(outcome.expired, 0);
    assert_eq!(state(&fixture, &task), TaskRunState::Queued);
    assert_eq!(
        fixture.scheduler.claim_oldest(&worker(), 1).unwrap().len(),
        1
    );
}

#[test]
fn a_task_that_has_started_leaves_no_expiry_to_wake_for() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(10_000)))
        .unwrap();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(10_000)));

    fixture.scheduler.request_claim(&worker(), &task).unwrap();

    assert_eq!(fixture.scheduler.next_deadline(), None);
}

/// A clock that moves on every time it is read, so time passes between the
/// steps of one call, as it does with a real clock.
struct TickingClock {
    now: std::cell::Cell<u64>,
}

impl Clock for TickingClock {
    fn now(&self) -> Instant {
        let now = self.now.get();
        self.now.set(now + 1);
        Instant::at(now)
    }

    fn wall_clock_millis(&self) -> u64 {
        0
    }
}

#[test]
fn a_claim_never_fails_because_time_passed_between_its_own_steps() {
    let clock = TickingClock {
        now: std::cell::Cell::new(0),
    };
    let mut scheduler = Scheduler::new(clock, SequentialIds::new());
    scheduler.set_leadership_grant(Some(unbounded_grant()));
    // Every task expires a few ticks after it is submitted, which is soon
    // enough that some expire in the middle of a claim.
    for _ in 0..20 {
        scheduler.submit(plain().with_expiry(ticks(30))).unwrap();
    }

    // Must not panic, whichever tasks end up expired.
    for _ in 0..40 {
        let _ = scheduler.claim_oldest(&worker(), 5);
    }
}

#[test]
fn an_expiry_is_reported_once_as_an_event_and_then_drained() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(10)))
        .unwrap();
    // The run that will expire, taken from the scheduler before it does.
    let run_id = fixture.scheduler.run_of(&task).unwrap().task_run_id();
    assert!(!fixture.scheduler.has_events());
    fixture.clock.advance(ticks(10));
    fixture.scheduler.advance();

    assert!(fixture.scheduler.has_events());
    let events = fixture.scheduler.take_events();

    assert_eq!(
        events,
        vec![Event::Expired {
            task_id: task,
            task_run_id: run_id,
        }]
    );
    assert!(fixture.scheduler.take_events().is_empty());
    assert!(!fixture.scheduler.has_events());
}

#[test]
fn events_come_out_in_the_order_they_happened() {
    let mut fixture = leading();
    let first = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(10)))
        .unwrap();
    let second = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(20)))
        .unwrap();
    fixture.clock.advance(ticks(30));
    fixture.scheduler.advance();

    let order: Vec<TaskId> = fixture
        .scheduler
        .take_events()
        .into_iter()
        .map(|event| match event {
            Event::Expired { task_id, .. } => task_id,
            other => panic!("expected an expiry, got {other:?}"),
        })
        .collect();

    assert_eq!(order, vec![first, second]);
}

#[test]
fn only_a_leader_decides_that_time_has_run_out() {
    let mut fixture = leading();
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(10)))
        .unwrap();
    let delayed = fixture
        .scheduler
        .submit(plain().with_delay(ticks(10)))
        .unwrap();
    fixture.scheduler.set_leadership_grant(None);
    fixture.clock.advance(ticks(1_000));

    let outcome = fixture.scheduler.advance();

    assert_eq!((outcome.queued, outcome.expired), (0, 0));
    assert_eq!(state(&fixture, &task), TaskRunState::Queued);
    assert_eq!(state(&fixture, &delayed), TaskRunState::Scheduled);
    assert!(!fixture.scheduler.has_events());
}

#[test]
fn a_leader_whose_grant_has_run_out_decides_nothing_about_time() {
    let mut fixture = leading();
    let end = fixture.clock.now() + ticks(5);
    let grant = LeadershipGrant {
        valid_until: LeaseEnd::At(end),
        ..unbounded_grant()
    };
    fixture.scheduler.set_leadership_grant(Some(grant));
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(10)))
        .unwrap();
    let delayed = fixture
        .scheduler
        .submit(plain().with_delay(ticks(10)))
        .unwrap();
    fixture.clock.advance(ticks(1_000));

    let outcome = fixture.scheduler.advance();

    assert_eq!((outcome.queued, outcome.expired), (0, 0));
    assert_eq!(state(&fixture, &task), TaskRunState::Queued);
    assert_eq!(state(&fixture, &delayed), TaskRunState::Scheduled);
    assert!(!fixture.scheduler.has_events());
}

#[test]
fn next_deadline_is_the_earliest_thing_that_will_happen() {
    let mut fixture = leading();
    assert_eq!(fixture.scheduler.next_deadline(), None);

    fixture
        .scheduler
        .submit(plain().with_delay(ticks(300)))
        .unwrap();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(300)));

    fixture
        .scheduler
        .submit(plain().with_expiry(ticks(120)))
        .unwrap();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(120)));

    fixture
        .scheduler
        .submit(plain().with_delay(ticks(50)))
        .unwrap();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(50)));
}

#[test]
fn next_deadline_moves_on_once_a_deadline_has_been_handled() {
    let mut fixture = leading();
    fixture
        .scheduler
        .submit(plain().with_delay(ticks(50)))
        .unwrap();
    fixture
        .scheduler
        .submit(plain().with_delay(ticks(90)))
        .unwrap();
    fixture.clock.advance(ticks(60));
    fixture.scheduler.advance();

    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(90)));
}

#[test]
fn a_finished_tasks_forgetting_time_is_a_deadline_too() {
    let mut fixture = leading();
    fixture.scheduler.set_result_ttl(Some(ticks(500)));
    let task = fixture.scheduler.submit(plain()).unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    fixture.clock.advance(ticks(40));
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, b"d".to_vec())
        .unwrap();

    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(540)));
}

#[test]
fn an_expired_task_is_kept_and_then_forgotten_like_any_finished_task() {
    let mut fixture = leading();
    fixture.scheduler.set_result_ttl(Some(ticks(100)));
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(10)))
        .unwrap();
    fixture.clock.advance(ticks(10));
    fixture.scheduler.advance();
    assert!(fixture.scheduler.task(&task).is_some());

    fixture.clock.advance(ticks(100));

    assert_eq!(fixture.scheduler.sweep(), 1);
    assert!(fixture.scheduler.task(&task).is_none());
}
