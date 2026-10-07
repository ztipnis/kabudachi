//! Delayed submission and expiry: a task with a delay waits `Scheduled` and
//! becomes `Queued` only when due, and a pending task that outlives its
//! expiry becomes `Expired` instead of running late. The leader decides both,
//! whenever time is advanced or a claim is made.


use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    CaughtUp, ClaimRejection, Completion, LeadershipGrant, LeaseEnd, Scheduler, Submission,
};
use kabudachi_core::time::{Clock, Instant};
use crate::support::grant::unbounded_grant;
use crate::support::ids::SequentialIds;
use crate::support::scheduler::{Fixture, ticks};

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn plain() -> Submission {
    Submission::new(
        TaskDefinitionId::new("billing.charge"),
        0,
        b"in".to_vec(),
        "default",
    )
}

#[test]
fn a_delay_queues_the_task_exactly_when_due() {
    // (delay, how long to wait, whether the task is queued by then)
    for (delay, wait, queued) in [(0, 0, true), (100, 99, false), (100, 100, true)] {
        let mut fixture = Fixture::leading();
        let task = fixture
            .scheduler
            .submit(plain().with_delay(ticks(delay)))
            .unwrap();
        assert_eq!(fixture.spy.task(&task).delay_millis, Some(delay));

        fixture.clock.advance(ticks(wait));
        let caught_up = fixture.scheduler.catch_up();

        let what = format!("delay {delay}, waited {wait}");
        let state = fixture.state(&task);
        assert_eq!(caught_up.queued, usize::from(queued && delay > 0), "{what}");
        assert_eq!(fixture.spy.pending(), usize::from(queued), "{what}");
        let claim = fixture.scheduler.request_claim(&worker(), &task);
        if queued {
            assert_eq!(state, TaskRunState::Queued, "{what}");
            assert!(claim.is_ok(), "{what}");
        } else {
            assert_eq!(state, TaskRunState::Scheduled, "{what}");
            assert_eq!(claim.unwrap_err(), ClaimRejection::NotReady, "{what}");
            assert!(
                fixture
                    .scheduler
                    .claim_oldest(&worker(), 10)
                    .unwrap()
                    .is_empty(),
                "{what}"
            );
        }
    }
}

#[test]
fn due_tasks_join_the_queue_by_due_time_then_submission_order() {
    let mut fixture = Fixture::leading();
    let later = fixture
        .scheduler
        .submit(plain().with_delay(ticks(200)))
        .unwrap();
    let sooner = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)))
        .unwrap();
    let immediate = fixture.scheduler.submit(plain()).unwrap();
    // Ten due together, so that ordering by task ID ("task-10" sorts before
    // "task-2") would come out different from submission order.
    let together: Vec<TaskId> = (0..10)
        .map(|_| fixture.scheduler.submit(plain().with_delay(ticks(300))).unwrap())
        .collect();
    fixture.clock.advance(ticks(300));

    // Nobody advanced time through `catch_up`: the claim finds them due.
    let claims = fixture.scheduler.claim_oldest(&worker(), 20).unwrap();

    let order: Vec<TaskId> = claims.iter().map(|claim| claim.task.task_id()).collect();
    let mut expected = vec![immediate, sooner, later];
    expected.extend(together);
    assert_eq!(order, expected);
}

#[test]
fn expiry_is_only_about_starting() {
    let expiring = |fixture: &mut Fixture, ticks_to_expiry| {
        fixture
            .scheduler
            .submit(plain().with_expiry(ticks(ticks_to_expiry)))
            .unwrap()
    };

    // A pending task expires exactly at its expiry instead of running late.
    let mut fixture = Fixture::leading();
    let task = expiring(&mut fixture, 100);
    fixture.clock.advance(ticks(99));
    assert_eq!(fixture.scheduler.catch_up().expired, 0);
    assert_eq!(fixture.state(&task), TaskRunState::Queued);
    fixture.clock.advance(ticks(1));
    assert_eq!(fixture.scheduler.catch_up().expired, 1);
    assert_eq!(fixture.state(&task), TaskRunState::Expired);
    assert_eq!(fixture.spy.pending(), 0);

    // It is never handed out, even if nobody advanced time.
    let mut fixture = Fixture::leading();
    let task = expiring(&mut fixture, 10);
    fixture.clock.advance(ticks(10));
    assert!(fixture.scheduler.claim_oldest(&worker(), 10).unwrap().is_empty());
    assert_eq!(fixture.state(&task), TaskRunState::Expired);

    // Claiming it by name says it finished.
    let mut fixture = Fixture::leading();
    let task = expiring(&mut fixture, 10);
    fixture.clock.advance(ticks(10));
    assert_eq!(
        fixture.scheduler.request_claim(&worker(), &task).unwrap_err(),
        ClaimRejection::Finished
    );

    // A scheduled task can expire before it is ever due.
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)).with_expiry(ticks(50)))
        .unwrap();
    fixture.clock.advance(ticks(200));
    let outcome = fixture.scheduler.catch_up();
    assert_eq!((outcome.expired, outcome.queued), (1, 0));
    assert_eq!(fixture.state(&task), TaskRunState::Expired);
    assert_eq!(fixture.spy.pending(), 0);

    // A retry has already started once, so it never expires.
    let mut fixture = Fixture::leading();
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
    assert_eq!(fixture.scheduler.catch_up().expired, 0);
    assert_eq!(fixture.state(&task), TaskRunState::Queued);
    assert_eq!(fixture.scheduler.claim_oldest(&worker(), 1).unwrap().len(), 1);
}

#[test]
fn a_delayed_task_that_became_due_can_still_expire() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(10)).with_expiry(ticks(50)))
        .unwrap();
    fixture.clock.advance(ticks(10));
    assert_eq!(fixture.scheduler.catch_up().queued, 1);

    fixture.clock.advance(ticks(40));

    assert_eq!(fixture.scheduler.catch_up().expired, 1);
    assert_eq!(fixture.state(&task), TaskRunState::Expired);
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
fn a_leader_whose_grant_has_run_out_decides_nothing_about_time() {
    let mut fixture = Fixture::leading();
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

    let outcome = fixture.scheduler.catch_up();

    assert_eq!((outcome.queued, outcome.expired), (0, 0));
    assert_eq!(fixture.state(&task), TaskRunState::Queued);
    assert_eq!(fixture.state(&delayed), TaskRunState::Scheduled);
    assert!(!fixture.scheduler.has_events());
    assert_eq!(
        fixture.scheduler.next_deadline(),
        None,
        "no TTL is set and the lease has run out, so nothing is left to wake for"
    );
}

fn finished_task(fixture: &mut Fixture) -> TaskId {
    let task = fixture.scheduler.submit(plain()).unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, Digest::blake3(b"d"), Completion::Final)
        .unwrap();
    task
}

#[test]
fn a_scheduler_that_does_not_lead_still_forgets_finished_tasks_on_time() {
    let mut fixture = Fixture::leading();
    fixture.scheduler.set_result_ttl(Some(ticks(10)));
    let delayed = fixture
        .scheduler
        .submit(plain().with_delay(ticks(5)))
        .unwrap();
    let task = finished_task(&mut fixture);
    fixture.scheduler.set_leadership_grant(None);

    // Only forgetting is left to wake for: the delay is a leader's to act on.
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(10)));
    fixture.clock.advance(ticks(10));
    let caught_up = fixture.scheduler.catch_up();

    assert_eq!(
        caught_up,
        CaughtUp {
            queued: 0,
            expired: 0,
            forgotten: 1
        }
    );
    assert!(fixture.scheduler.runs_of(&task).is_empty());
    assert_eq!(fixture.state(&delayed), TaskRunState::Scheduled);
}

#[test]
fn next_deadline_includes_the_lease_end_while_leading_and_not_once_the_lapse_is_found() {
    let mut fixture = Fixture::not_leading();
    fixture.scheduler.set_leadership_grant(Some(LeadershipGrant {
        valid_until: LeaseEnd::At(Instant::at(10)),
        ..unbounded_grant()
    }));
    fixture
        .scheduler
        .submit(plain().with_delay(ticks(50)))
        .unwrap();
    assert_eq!(
        fixture.scheduler.next_deadline(),
        Some(Instant::at(10)),
        "while it leads, its lease end is the first thing to wake for"
    );

    fixture.clock.advance(ticks(10));
    assert_eq!(
        fixture.scheduler.next_deadline(),
        Some(Instant::at(10)),
        "at the lease end, finding the lapse is still catch_up's to do"
    );
    fixture.scheduler.catch_up();

    assert_eq!(
        fixture.scheduler.next_deadline(),
        None,
        "once the lapse is found, neither the lease end nor a leader's delay is left to wake for"
    );
}

#[test]
fn next_deadline_is_the_earliest_live_deadline() {
    let mut fixture = Fixture::leading();
    assert_eq!(fixture.scheduler.next_deadline(), None);

    fixture
        .scheduler
        .submit(plain().with_delay(ticks(300)))
        .unwrap();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(300)));

    let expiring = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(120)))
        .unwrap();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(120)));

    fixture
        .scheduler
        .submit(plain().with_delay(ticks(50)))
        .unwrap();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(50)));

    // Once a deadline has been handled it no longer counts.
    fixture.clock.advance(ticks(60));
    fixture.scheduler.catch_up();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(120)));

    // A cancelled task leaves no expiry to wake for.
    fixture.scheduler.cancel(&expiring).unwrap();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(300)));

    // Neither does one that has started.
    let started = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(100)))
        .unwrap();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(160)));
    fixture.scheduler.request_claim(&worker(), &started).unwrap();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(300)));
}

