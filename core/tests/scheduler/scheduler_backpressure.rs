//! Memory pressure at one node: the scheduler counts the serialized bytes of
//! every task that has not finished, raises `SlowDown` past a soft limit, and
//! refuses a submission past a hard one. A coalescing
//! task may opt in to dropping its own oldest retained payloads instead.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
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

/// Claims and starts `task`, returning its run.
fn start(fixture: &mut Fixture, task: &TaskId) -> TaskRunId {
    let claim = fixture.scheduler.request_claim(&worker(), task).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    claim.task_run_id
}

fn finish(fixture: &mut Fixture, task: &TaskId) {
    let claim = fixture.scheduler.request_claim(&worker(), task).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, Digest::blake3(b"d"), Completion::Final)
        .unwrap();
}

#[test]
fn memory_counts_exactly_the_unfinished_tasks() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    assert_eq!(fixture.scheduler.memory_in_use(), 0);

    // A queued and a delayed task both count.
    let first = fixture.scheduler.submit(payload(30)).unwrap();
    fixture
        .scheduler
        .submit(payload(20).with_delay(Duration::from_ticks(50)))
        .unwrap();
    assert_eq!(fixture.scheduler.memory_in_use(), 50);

    finish(&mut fixture, &first);
    assert_eq!(fixture.scheduler.memory_in_use(), 20, "a completed task stops counting");

    let failing = fixture.scheduler.submit(payload(10)).unwrap();
    let claim = start(&mut fixture, &failing);
    fixture
        .scheduler
        .fail(&worker(), &claim, "ValueError")
        .unwrap();
    assert_eq!(fixture.scheduler.memory_in_use(), 20, "a failed task stops counting");

    let cancelled = fixture.scheduler.submit(payload(10)).unwrap();
    fixture.scheduler.cancel(&cancelled).unwrap();
    assert_eq!(fixture.scheduler.memory_in_use(), 20, "a cancelled task stops counting");

    fixture
        .scheduler
        .submit(payload(10).with_expiry(Duration::from_ticks(5)))
        .unwrap();
    assert_eq!(fixture.scheduler.memory_in_use(), 30);
    fixture.clock.advance(Duration::from_ticks(5));
    fixture.scheduler.catch_up();
    assert_eq!(fixture.scheduler.memory_in_use(), 20, "an expired task stops counting");

    let retrying = fixture
        .scheduler
        .submit(payload(40).with_retries(1))
        .unwrap();
    let claim = start(&mut fixture, &retrying);
    fixture
        .scheduler
        .fail(&worker(), &claim, "ValueError")
        .unwrap();
    assert_eq!(
        fixture.scheduler.memory_in_use(),
        60,
        "a task waiting for its retry still counts"
    );

    let continuing = fixture.scheduler.submit(payload(40)).unwrap();
    let claim = start(&mut fixture, &continuing);
    fixture
        .scheduler
        .complete(&worker(), &claim, Digest::blake3(b"d"), Completion::Continues)
        .unwrap();
    assert_eq!(
        fixture.scheduler.memory_in_use(),
        100,
        "a task with a continuation counts until it ends"
    );
    assert_eq!(fixture.scheduler.end_continuation(&continuing), Ok(true));
    assert_eq!(fixture.scheduler.memory_in_use(), 60);
}

#[test]
fn slow_down_can_be_raised_again_after_it_cleared() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    let base = fixture.scheduler.submit(payload(80)).unwrap();
    let margin = fixture.scheduler.submit(payload(20)).unwrap();
    assert!(
        slow_down_events(&mut fixture).is_empty(),
        "exactly the soft limit is not past it"
    );
    let over = fixture.scheduler.submit(payload(1)).unwrap();
    assert_eq!(slow_down_events(&mut fixture), vec![true]);
    let more = fixture.scheduler.submit(payload(50)).unwrap();
    assert!(
        slow_down_events(&mut fixture).is_empty(),
        "not raised again while usage stays past the soft limit"
    );
    fixture.scheduler.cancel(&more).unwrap();
    fixture.scheduler.cancel(&margin).unwrap();
    assert!(
        slow_down_events(&mut fixture).is_empty(),
        "81% of the soft limit keeps it raised"
    );
    fixture.scheduler.cancel(&over).unwrap();
    assert_eq!(slow_down_events(&mut fixture), vec![false], "80% clears it");
    fixture.scheduler.cancel(&base).unwrap();

    let first = fixture.scheduler.submit(payload(101)).unwrap();
    fixture.scheduler.cancel(&first).unwrap();
    assert_eq!(slow_down_events(&mut fixture), vec![true, false]);

    fixture.scheduler.submit(payload(101)).unwrap();

    assert_eq!(slow_down_events(&mut fixture), vec![true]);
}

#[test]
fn a_submission_is_refused_past_the_hard_limit_and_accepted_when_it_exactly_fills_it() {
    let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
    fixture.scheduler.submit(payload(150)).unwrap();
    let _ = fixture.scheduler.take_events();

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
        fixture.spy.revised_since(mark).is_empty(),
        "a refused submission changes nothing"
    );
    assert!(fixture.scheduler.take_events().is_empty());
    assert_eq!(fixture.scheduler.memory_in_use(), 150);
    assert_eq!(fixture.scheduler.pending_len(), 1);

    assert!(fixture.scheduler.submit(payload(50)).is_ok());
    assert_eq!(fixture.scheduler.memory_in_use(), HARD);
}

#[test]
fn drop_oldest_outcomes() {
    struct Row {
        what: &'static str,
        /// The generations of the key already submitted, oldest first.
        existing: Vec<(usize, &'static str)>,
        /// Whether the oldest of them is claimed and started first.
        running: bool,
        submission: Submission,
        accepted: bool,
        memory: u64,
        /// How many older payloads the next claim folds, when it is checked.
        folded: Option<usize>,
    }
    let rows = vec![
        Row {
            what: "a coalescing task without drop_oldest is refused and nothing is dropped",
            existing: vec![(90, "k"), (90, "k")],
            running: false,
            submission: generation(90, "k"),
            accepted: false,
            memory: 180,
            folded: Some(1),
        },
        Row {
            what: "drop_oldest drops as many payloads as it takes",
            existing: vec![(70, "k"), (70, "k")],
            running: false,
            submission: generation(180, "k").with_drop_oldest(),
            accepted: true,
            memory: 180,
            folded: Some(0),
        },
        Row {
            what: "drop_oldest never lets usage exceed the hard limit",
            existing: vec![(40, "k"), (40, "k"), (40, "k")],
            running: false,
            submission: generation(201, "k").with_drop_oldest(),
            accepted: false,
            memory: 120,
            folded: None,
        },
        Row {
            what: "drop_oldest is refused when its key has too little to drop",
            existing: vec![(190, "other")],
            running: false,
            submission: generation(50, "mine").with_drop_oldest(),
            accepted: false,
            memory: 190,
            folded: None,
        },
        Row {
            what: "a running generation's payload is never dropped",
            existing: vec![(150, "k")],
            running: true,
            submission: generation(60, "k").with_drop_oldest(),
            accepted: false,
            memory: 150,
            folded: None,
        },
    ];

    for row in rows {
        let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: SOFT, hard: HARD });
        let mut submitted = Vec::new();
        for (size, key) in row.existing {
            submitted.push(fixture.scheduler.submit(generation(size, key)).unwrap());
        }
        if row.running {
            start(&mut fixture, &submitted[0]);
        }

        let outcome = fixture.scheduler.submit(row.submission);

        assert_eq!(outcome.is_ok(), row.accepted, "{}", row.what);
        assert_eq!(fixture.scheduler.memory_in_use(), row.memory, "{}", row.what);
        if let Some(folded) = row.folded {
            let claim = fixture
                .scheduler
                .claim_oldest(&worker(), 1)
                .unwrap()
                .remove(0);
            assert_eq!(claim.chain.len(), folded, "{}", row.what);
        }
    }
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
    assert_eq!(fixture.scheduler.memory_in_use(), largest_input as u64);
    assert_eq!(fixture.scheduler.pending_len(), 1);
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
