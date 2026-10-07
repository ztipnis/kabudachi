//! Cancelling a task in whatever state its current run is in. The leader
//! decides: the run becomes `Cancelled` at once, so nothing the worker still
//! reports about it can count, and the worker is told to stop the body.

use crate::support::scheduler::{ticks, Fixture};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    Cancellation, ClaimRejection, Completion, Event, ReportRejection, Submission,
};

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

fn cancelled_events(fixture: &mut Fixture) -> Vec<(TaskId, bool)> {
    fixture
        .scheduler
        .take_events()
        .into_iter()
        .filter_map(|event| match event {
            Event::Cancelled {
                task_id,
                was_running,
                ..
            } => Some((task_id, was_running)),
            _ => None,
        })
        .collect()
}

#[test]
fn a_task_is_cancelled_in_whatever_state_its_run_is_in() {
    let mut fixture = Fixture::leading();
    let queued = fixture.scheduler.submit(plain()).unwrap();
    let scheduled = fixture
        .scheduler
        .submit(plain().with_delay(ticks(100)).with_expiry(ticks(50)))
        .unwrap();
    let claimed = fixture.scheduler.submit(plain()).unwrap();
    let claimed_run = fixture
        .scheduler
        .request_claim(&worker(), &claimed)
        .unwrap()
        .task_run_id;
    let running = fixture.scheduler.submit(plain().with_retries(3)).unwrap();
    let running_run = fixture
        .scheduler
        .request_claim(&worker(), &running)
        .unwrap()
        .task_run_id;
    fixture
        .scheduler
        .report_started(&worker(), &running_run)
        .unwrap();
    let retrying = fixture.scheduler.submit(plain().with_retries(1)).unwrap();
    let failed_run = fixture
        .scheduler
        .request_claim(&worker(), &retrying)
        .unwrap()
        .task_run_id;
    fixture
        .scheduler
        .report_started(&worker(), &failed_run)
        .unwrap();
    fixture
        .scheduler
        .fail(&worker(), &failed_run, "ValueError")
        .unwrap();

    let outcomes: Vec<Cancellation> = [&queued, &scheduled, &claimed, &running, &retrying]
        .into_iter()
        .map(|task| fixture.scheduler.cancel(task).unwrap())
        .collect();

    let cancelled = |was_running| Cancellation::Cancelled { was_running };
    assert_eq!(
        outcomes,
        vec![
            cancelled(false),
            cancelled(false),
            cancelled(true),
            cancelled(true),
            cancelled(false)
        ]
    );
    for task in [&queued, &scheduled, &claimed, &running, &retrying] {
        assert_eq!(fixture.state(task), TaskRunState::Cancelled);
    }
    assert_eq!(
        cancelled_events(&mut fixture)
            .into_iter()
            .filter(|(task, _)| task == &queued || task == &running)
            .collect::<Vec<_>>(),
        vec![(queued.clone(), false), (running.clone(), true)]
    );
    // Nothing waits on time any more, before the clock could clear a leftover.
    assert_eq!(fixture.scheduler.next_deadline(), None);
    fixture.clock.advance(ticks(500));
    assert_eq!(fixture.scheduler.catch_up().queued, 0);
    // A cancelled task is never handed out, and a cancelled running task is
    // not retried.
    assert_eq!(fixture.spy.pending(), 0);
    assert_eq!(
        fixture
            .scheduler
            .request_claim(&worker(), &queued)
            .unwrap_err(),
        ClaimRejection::Finished
    );
    assert!(
        fixture
            .scheduler
            .claim_oldest(&worker(), 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(fixture.scheduler.runs_of(&running).len(), 1);
    // The worker can no longer start the claimed run, and nothing it reports
    // about the running one afterwards counts.
    let mark = fixture.spy.mark();
    assert_eq!(
        fixture
            .scheduler
            .report_started(&worker(), &claimed_run)
            .unwrap_err(),
        ReportRejection::NotAuthoritative
    );
    let completed = fixture.scheduler.complete(
        &worker(),
        &running_run,
        Digest::blake3(b"late"),
        Completion::Final,
    );
    let failed = fixture
        .scheduler
        .fail(&worker(), &running_run, "ValueError");
    assert_eq!(completed.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(failed.unwrap_err(), ReportRejection::NotAuthoritative);
    assert!(
        fixture.spy.since(mark).is_empty(),
        "refused reports change nothing"
    );
    assert_eq!(
        fixture.scheduler.task_run(&running_run).unwrap().result_digest,
        None
    );
}
