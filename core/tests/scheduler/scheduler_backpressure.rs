//! Memory pressure at one node: the scheduler counts the serialized bytes of
//! every task that has not finished, raises `SlowDown` past a soft limit, and
//! refuses a submission past a hard one. A coalescing
//! task may opt in to dropping its own oldest retained payloads instead.


use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::scheduler::{
    Completion, Event, MAX_SUBMISSION_BYTES, MemoryLimits, Submission, SubmitRejection,
};
use kabudachi_core::time::Duration;
use crate::support::scheduler::Fixture;

const SOFT: u64 = 100;
const HARD: u64 = 200;

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn payload(size: usize) -> Submission {
    Submission::new(
        TaskDefinitionId::new("bulk.load"),
        0,
        vec![b'x'; size],
        "default",
    )
}

fn generation(size: usize, key: &str) -> Submission {
    Submission::new(
        TaskDefinitionId::new("index.refresh"),
        0,
        vec![b'x'; size],
        "default",
    )
    .with_coalescing_key(key)
}

fn slow_down_events(fixture: &mut Fixture) -> Vec<bool> {
    fixture
        .scheduler
        .take_events()
        .into_iter()
        .filter_map(|event| match event {
            Event::SlowDown { active } => Some(active),
            _ => None,
        })
        .collect()
}

fn finish(fixture: &mut Fixture, task: &TaskId) {
    let claim = fixture.scheduler.request_claim(&worker(), task).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, b"d".to_vec(), Completion::Final)
        .unwrap();
}

#[test]
fn memory_in_use_counts_the_payload_of_every_task_that_has_not_finished() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    assert_eq!(fixture.spy.memory_in_use(), 0);

    let first = fixture.scheduler.submit(payload(30)).unwrap();
    fixture
        .scheduler
        .submit(payload(20).with_delay(Duration::from_ticks(50)))
        .unwrap();
    assert_eq!(fixture.spy.memory_in_use(), 50);

    finish(&mut fixture, &first);
    assert_eq!(fixture.spy.memory_in_use(), 20);
}

#[test]
fn a_failed_cancelled_or_expired_task_stops_counting() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    let failing = fixture.scheduler.submit(payload(10)).unwrap();
    let cancelled = fixture.scheduler.submit(payload(10)).unwrap();
    let expiring = fixture
        .scheduler
        .submit(payload(10).with_expiry(Duration::from_ticks(5)))
        .unwrap();
    let claim = fixture
        .scheduler
        .request_claim(&worker(), &failing)
        .unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();

    fixture
        .scheduler
        .fail(&worker(), &claim.task_run_id, "ValueError")
        .unwrap();
    fixture.scheduler.cancel(&cancelled).unwrap();
    fixture.clock.advance(Duration::from_ticks(5));
    fixture.scheduler.catch_up();

    assert_eq!(fixture.spy.memory_in_use(), 0, "{expiring:?}");
}

#[test]
fn a_task_waiting_for_its_retry_still_counts() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    let task = fixture
        .scheduler
        .submit(payload(40).with_retries(1))
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

    assert_eq!(fixture.spy.memory_in_use(), 40);
}

#[test]
fn a_superseded_payload_counts_until_the_generation_that_absorbed_it_finishes() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    fixture.scheduler.submit(generation(30, "k")).unwrap();
    let newest = fixture.scheduler.submit(generation(20, "k")).unwrap();
    assert_eq!(fixture.spy.memory_in_use(), 50);

    finish(&mut fixture, &newest);

    assert_eq!(fixture.spy.memory_in_use(), 0);
}

#[test]
fn slow_down_can_be_raised_again_after_it_cleared() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    let first = fixture.scheduler.submit(payload(101)).unwrap();
    fixture.scheduler.cancel(&first).unwrap();
    assert_eq!(slow_down_events(&mut fixture), vec![true, false]);

    fixture.scheduler.submit(payload(101)).unwrap();

    assert_eq!(slow_down_events(&mut fixture), vec![true]);
}

#[test]
fn backpressure_error_is_raised_past_the_hard_limit() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    fixture.scheduler.submit(payload(150)).unwrap();

    let mark = fixture.spy.mark();
    let rejected = fixture.scheduler.submit(payload(51));

    assert_eq!(
        rejected.unwrap_err(),
        SubmitRejection::Backpressure {
            hard_limit: HARD,
            in_use: 150,
            needed: 51,
        }
    );
    assert!(
        fixture.spy.since(mark).is_empty(),
        "a refused submission changes nothing"
    );
    assert_eq!(fixture.spy.memory_in_use(), 150);
    assert_eq!(fixture.spy.pending(), 1);
}

#[test]
fn a_submission_that_exactly_fills_the_hard_limit_is_accepted() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    fixture.scheduler.submit(payload(150)).unwrap();

    assert!(fixture.scheduler.submit(payload(50)).is_ok());
    assert_eq!(fixture.spy.memory_in_use(), HARD);
}

#[test]
fn room_made_by_finishing_tasks_lets_submissions_through_again() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    let first = fixture.scheduler.submit(payload(150)).unwrap();
    assert!(fixture.scheduler.submit(payload(100)).is_err());

    finish(&mut fixture, &first);

    assert!(fixture.scheduler.submit(payload(100)).is_ok());
}

#[test]
fn removing_the_limits_clears_a_raised_slow_down() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    fixture.scheduler.submit(payload(101)).unwrap();
    assert_eq!(slow_down_events(&mut fixture), vec![true]);

    fixture.scheduler.set_memory_limits(None);

    assert_eq!(slow_down_events(&mut fixture), vec![false]);
    fixture.scheduler.submit(payload(1_000)).unwrap();
    assert!(
        slow_down_events(&mut fixture).is_empty(),
        "with no limits nothing raises it again"
    );
}

#[test]
fn without_limits_nothing_is_refused_and_no_signal_is_raised() {
    let mut fixture = Fixture::leading();

    for _ in 0..3 {
        fixture.scheduler.submit(payload(1_000_000)).unwrap();
    }

    assert!(!fixture.scheduler.has_events());
}

#[test]
fn a_coalescing_task_is_refused_at_the_hard_limit_and_never_silently_dropped() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    fixture.scheduler.submit(generation(90, "k")).unwrap();
    fixture.scheduler.submit(generation(90, "k")).unwrap();
    assert_eq!(fixture.spy.memory_in_use(), 180);

    let rejected = fixture.scheduler.submit(generation(90, "k"));

    assert!(rejected.is_err());
    // The older payloads are all still there to be folded.
    let claim = fixture
        .scheduler
        .claim_oldest(&worker(), 1)
        .unwrap()
        .remove(0);
    assert_eq!(claim.chain.len(), 1);
}

#[test]
fn drop_oldest_makes_room_by_dropping_the_keys_oldest_retained_payloads_first() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    // Retained payloads of 60, 60 and 60 (the last is the waiting generation).
    for size in [61, 62, 63] {
        fixture.scheduler.submit(generation(size, "k")).unwrap();
    }
    assert_eq!(fixture.spy.memory_in_use(), 186);

    let accepted = fixture
        .scheduler
        .submit(generation(50, "k").with_drop_oldest())
        .unwrap();

    // 186 + 50 = 236 > 200: the oldest payload (61) goes, which is enough.
    assert_eq!(fixture.spy.memory_in_use(), 175);
    let claim = fixture
        .scheduler
        .request_claim(&worker(), &accepted)
        .unwrap();
    assert_eq!(
        claim.chain.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![62, 63]
    );
}

#[test]
fn drop_oldest_drops_as_many_as_it_takes() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    for size in [70, 70] {
        fixture.scheduler.submit(generation(size, "k")).unwrap();
    }

    let accepted = fixture
        .scheduler
        .submit(generation(180, "k").with_drop_oldest())
        .unwrap();

    assert_eq!(fixture.spy.memory_in_use(), 180);
    let claim = fixture
        .scheduler
        .request_claim(&worker(), &accepted)
        .unwrap();
    assert!(claim.chain.is_empty());
}

#[test]
fn drop_oldest_never_lets_usage_exceed_the_hard_limit() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    for size in [40, 40, 40] {
        fixture.scheduler.submit(generation(size, "k")).unwrap();
    }

    // Even dropping everything retained, 201 bytes cannot fit.
    let rejected = fixture
        .scheduler
        .submit(generation(201, "k").with_drop_oldest());

    assert!(rejected.is_err());
    assert_eq!(
        fixture.spy.memory_in_use(),
        120,
        "nothing was dropped"
    );
}

#[test]
fn drop_oldest_is_refused_when_the_key_has_too_little_to_drop() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    // Another key holds most of the memory; this key has nothing retained.
    fixture.scheduler.submit(generation(190, "other")).unwrap();

    let rejected = fixture
        .scheduler
        .submit(generation(50, "mine").with_drop_oldest());

    assert!(rejected.is_err());
    assert_eq!(fixture.spy.memory_in_use(), 190);
}

#[test]
fn a_running_generations_payload_is_never_dropped() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    let running = fixture.scheduler.submit(generation(150, "k")).unwrap();
    let claim = fixture
        .scheduler
        .request_claim(&worker(), &running)
        .unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();

    let rejected = fixture
        .scheduler
        .submit(generation(60, "k").with_drop_oldest());

    assert!(rejected.is_err());
    assert_eq!(fixture.spy.memory_in_use(), 150);
}

#[test]
fn a_task_too_large_for_a_claim_frame_is_refused_with_no_limits_set_and_leaves_nothing() {
    let mut fixture = Fixture::leading();
    let queue_bytes = "default".len();
    let definition_bytes = "bulk.load".len();
    let largest_input = MAX_SUBMISSION_BYTES as usize - queue_bytes - definition_bytes;

    let fits = fixture.scheduler.submit(payload(largest_input));
    let too_big = fixture.scheduler.submit(payload(largest_input + 1));

    assert!(fits.is_ok());
    assert_eq!(
        too_big.unwrap_err(),
        SubmitRejection::TooLarge {
            size: MAX_SUBMISSION_BYTES + 1,
            limit: MAX_SUBMISSION_BYTES,
        }
    );
    // Only the accepted one is held.
    assert_eq!(fixture.spy.memory_in_use(), largest_input as u64);
    assert_eq!(fixture.spy.pending(), 1);
}

#[test]
fn what_makes_a_task_too_large_for_a_claim_frame_is_its_queue_and_key_as_well_as_its_input() {
    let mut fixture = Fixture::leading();
    let long = "q".repeat(MAX_SUBMISSION_BYTES as usize);

    let by_queue = fixture
        .scheduler
        .submit(Submission::new(TaskDefinitionId::new("d"), 0, Vec::new(), long.clone()));
    let by_key = fixture
        .scheduler
        .submit(payload(1).with_coalescing_key(long));

    assert!(matches!(by_queue, Err(SubmitRejection::TooLarge { .. })));
    assert!(matches!(by_key, Err(SubmitRejection::TooLarge { .. })));
}
