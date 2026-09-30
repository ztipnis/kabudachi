//! Delayed submission and expiry: a task with a delay waits `Scheduled` and
//! becomes `Queued` only when due, and a pending task that outlives its
//! expiry becomes `Expired` instead of running late. The leader decides both,
//! whenever time is advanced or a claim is made.


use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    CaughtUp, ClaimRejection, Completion, Event, LeadershipGrant, LeaseEnd, Scheduler, Submission,
};
use kabudachi_core::time::{Clock, Instant};
use crate::support::grant::unbounded_grant;
use crate::support::ids::SequentialIds;
use crate::support::scheduler::{Fixture, TestScheduler, ticks};

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
fn a_delayed_task_waits_scheduled_and_is_not_pending() {
    let mut fixture = Fixture::leading();

    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)))
        .unwrap();

    assert_eq!(fixture.state(&task), TaskRunState::Scheduled);
    assert_eq!(fixture.spy.pending(), 0);
    assert_eq!(
        fixture.spy.task(&task).not_before_ticks,
        Some(100)
    );
}

#[test]
fn a_task_that_is_not_yet_due_cannot_be_claimed() {
    let mut fixture = Fixture::leading();
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
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)))
        .unwrap();

    fixture.clock.advance(ticks(99));
    let early = fixture.scheduler.catch_up();
    assert_eq!(early.queued, 0);
    assert_eq!(fixture.state(&task), TaskRunState::Scheduled);

    fixture.clock.advance(ticks(1));
    let due = fixture.scheduler.catch_up();
    assert_eq!(due.queued, 1);
    assert_eq!(fixture.state(&task), TaskRunState::Queued);
    assert_eq!(fixture.spy.pending(), 1);
}

#[test]
fn a_task_that_became_due_is_claimed_without_anyone_advancing_time() {
    let mut fixture = Fixture::leading();
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
    let mut fixture = Fixture::leading();

    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(0)))
        .unwrap();

    assert_eq!(fixture.state(&task), TaskRunState::Queued);
}

#[test]
fn delayed_tasks_join_the_queue_in_order_of_when_they_became_due() {
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

    fixture.clock.advance(ticks(300));
    let mark = fixture.spy.mark();
    fixture.scheduler.catch_up();

    // `immediate` was queued at submit, before the delayed ones came due.
    assert_eq!(fixture.spy.queued_since(mark), vec![sooner, later]);
    assert_eq!(fixture.spy.pending(), 3);
    assert_eq!(fixture.state(&immediate), TaskRunState::Queued);
}

#[test]
fn a_pending_task_expires_instead_of_running_late() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(100)))
        .unwrap();

    fixture.clock.advance(ticks(99));
    assert_eq!(fixture.scheduler.catch_up().expired, 0);
    assert_eq!(fixture.state(&task), TaskRunState::Queued);

    fixture.clock.advance(ticks(1));
    assert_eq!(fixture.scheduler.catch_up().expired, 1);
    assert_eq!(fixture.state(&task), TaskRunState::Expired);
    assert_eq!(fixture.spy.pending(), 0);
}

#[test]
fn an_expired_task_is_never_handed_out_even_if_nobody_advanced_time() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(10)))
        .unwrap();
    fixture.clock.advance(ticks(10));

    let claims = fixture.scheduler.claim_oldest(&worker(), 10).unwrap();

    assert!(claims.is_empty());
    assert_eq!(fixture.state(&task), TaskRunState::Expired);
}

#[test]
fn claiming_an_expired_task_says_it_finished() {
    let mut fixture = Fixture::leading();
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
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)).with_expiry(ticks(50)))
        .unwrap();

    fixture.clock.advance(ticks(200));
    let outcome = fixture.scheduler.catch_up();

    assert_eq!(outcome.expired, 1);
    assert_eq!(outcome.queued, 0);
    assert_eq!(fixture.state(&task), TaskRunState::Expired);
    assert_eq!(fixture.spy.pending(), 0);
}

#[test]
fn expiry_is_only_about_starting_so_a_retry_never_expires() {
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
    let outcome = fixture.scheduler.catch_up();

    assert_eq!(outcome.expired, 0);
    assert_eq!(fixture.state(&task), TaskRunState::Queued);
    assert_eq!(
        fixture.scheduler.claim_oldest(&worker(), 1).unwrap().len(),
        1
    );
}

#[test]
fn a_task_that_has_started_leaves_no_expiry_to_wake_for() {
    let mut fixture = Fixture::leading();
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
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(10)))
        .unwrap();
    // The run that will expire, taken from the scheduler before it does.
    let run_id = fixture.spy.run_of(&task).task_run_id();
    let later = fixture
        .scheduler
        .submit(plain().with_expiry(ticks(20)))
        .unwrap();
    let later_run_id = fixture.spy.run_of(&later).task_run_id();
    assert!(!fixture.scheduler.has_events());
    fixture.clock.advance(ticks(20));
    fixture.scheduler.catch_up();

    assert!(fixture.scheduler.has_events());
    let events = fixture.scheduler.take_events();

    assert_eq!(
        events,
        vec![
            Event::Expired {
                task_id: task,
                task_run_id: run_id,
            },
            Event::Expired {
                task_id: later,
                task_run_id: later_run_id,
            },
        ]
    );
    assert!(fixture.scheduler.take_events().is_empty());
    assert!(!fixture.scheduler.has_events());
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
        .complete(&worker(), &claim.task_run_id, b"d".to_vec(), Completion::Final)
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
fn every_mutating_call_forgets_what_has_outlived_its_ttl() {
    type Call = fn(&mut TestScheduler);
    let calls: [(&str, Call); 14] = [
        ("submit", |s| {
            s.submit(plain()).unwrap();
        }),
        ("claim_oldest", |s| {
            let _ = s.claim_oldest(&worker(), 1);
        }),
        ("claim_oldest_fitting", |s| {
            let _ = s.claim_oldest_fitting(&worker(), 1, |_| true);
        }),
        ("request_claim", |s| {
            let _ = s.request_claim(&worker(), &TaskId::new("unknown"));
        }),
        ("report_started", |s| {
            let _ = s.report_started(&worker(), &TaskRunId::new("unknown"));
        }),
        ("complete", |s| {
            let _ = s.complete(
                &worker(),
                &TaskRunId::new("unknown"),
                Vec::new(),
                Completion::Final,
            );
        }),
        ("fail", |s| {
            let _ = s.fail(&worker(), &TaskRunId::new("unknown"), "E");
        }),
        ("end_continuation", |s| {
            s.end_continuation(&TaskId::new("unknown"));
        }),
        ("cancel", |s| {
            let _ = s.cancel(&TaskId::new("unknown"));
        }),
        ("lose_worker", |s| {
            let _ = s.lose_worker(&WorkerId::new("gone"));
        }),
        ("set_leadership_grant", |s| {
            s.set_leadership_grant(Some(unbounded_grant()))
        }),
        ("set_memory_limits", |s| s.set_memory_limits(None)),
        ("set_result_ttl", |s| s.set_result_ttl(Some(ticks(10)))),
        ("take_events", |s| {
            s.take_events();
        }),
    ];
    for (name, call) in calls {
        let mut fixture = Fixture::leading();
        fixture.scheduler.set_result_ttl(Some(ticks(10)));
        let finished = finished_task(&mut fixture);
        fixture.clock.advance(ticks(10));

        call(&mut fixture.scheduler);

        assert!(
            fixture.scheduler.runs_of(&finished).is_empty(),
            "{name} left a task past its TTL"
        );
    }
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
fn next_deadline_is_the_earliest_thing_that_will_happen() {
    let mut fixture = Fixture::leading();
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
    let mut fixture = Fixture::leading();
    fixture
        .scheduler
        .submit(plain().with_delay(ticks(50)))
        .unwrap();
    fixture
        .scheduler
        .submit(plain().with_delay(ticks(90)))
        .unwrap();
    fixture.clock.advance(ticks(60));
    fixture.scheduler.catch_up();

    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(90)));
}

#[test]
fn a_finished_tasks_forgetting_time_is_a_deadline_too() {
    let mut fixture = Fixture::leading();
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
        .complete(&worker(), &claim.task_run_id, b"d".to_vec(), Completion::Final)
        .unwrap();

    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(540)));
}

#[test]
fn a_cancelled_task_leaves_no_expiry_to_wake_for() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain().with_expiry(ticks(100))).unwrap();
    assert_eq!(fixture.scheduler.next_deadline(), Some(Instant::at(100)));

    fixture.scheduler.cancel(&task).unwrap();

    assert_eq!(fixture.scheduler.next_deadline(), None);
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

#[test]
fn delayed_tasks_due_at_the_same_time_join_the_queue_in_submission_order() {
    let mut fixture = Fixture::leading();
    // Ten, so that ordering by task ID ("task-10" sorts before "task-2")
    // would come out different from submission order.
    let submitted: Vec<TaskId> = (0..10)
        .map(|_| fixture.scheduler.submit(plain().with_delay(ticks(10))).unwrap())
        .collect();
    fixture.clock.advance(ticks(10));

    let claims = fixture.scheduler.claim_oldest(&worker(), 10).unwrap();

    let order: Vec<TaskId> = claims.iter().map(|claim| claim.task.task_id()).collect();
    assert_eq!(order, submitted);
}
