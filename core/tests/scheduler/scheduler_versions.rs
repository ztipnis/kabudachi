//! A worker that cannot run a task's input version gives the task back,
//! either for another worker (its claimed run returns to the queue untouched
//! and the leader never hands that worker the task again) or as incompatible
//! (the task ends FAILED with the reason, never retried). A task left waiting
//! past the block TTL fails the same way.

use kabudachi_core::protocol::ids::{TaskDefinitionId, WorkerId};
use kabudachi_core::protocol::messages::TaskIds;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::reconcile::Rebuild;
use kabudachi_core::scheduler::composite::{CompositeState, OnError};
use kabudachi_core::scheduler::{ClaimRejection, Event, Submission};
use kabudachi_core::time::Clock;

use crate::support::composites::{finished, group_of, submitted, task_stage, worker};
use crate::support::scheduler::{Fixture, OFFICE, grant_of, newest_records, reconciling_after, ticks};

fn submitted_at(version: u32) -> Submission {
    Submission::new(
        TaskDefinitionId::new("orders.ship"),
        version,
        b"in".to_vec(),
        "default",
    )
    .with_retries(3)
}

fn old() -> WorkerId {
    WorkerId::new("old")
}

fn new() -> WorkerId {
    WorkerId::new("new")
}

/// Passes `millis` on both clocks: the block TTL counts wall time, delays
/// and expiries count the monotonic clock.
fn pass(fixture: &Fixture, millis: u64) {
    fixture.clock.advance(ticks(millis));
    fixture
        .clock
        .set_wall_clock_millis(fixture.clock.wall_clock_millis() + millis);
}

#[test]
fn a_blocked_claim_goes_back_to_the_queue_and_only_another_worker_gets_it() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(submitted_at(4)).unwrap();
    let claim = fixture.scheduler.request_claim(&old(), &task).unwrap();

    let blocked = fixture
        .scheduler
        .report_blocked(&old(), &task, "version 4 is newer than 3".into(), false)
        .unwrap();

    assert_eq!(
        (blocked.released, blocked.ended),
        (Some(claim.task_run_id.clone()), false)
    );
    assert_eq!(fixture.state(&task), TaskRunState::Queued);
    let record = fixture.spy.newest_revision_of(&task).unwrap();
    assert_eq!(record.runs.len(), 1, "the same run is queued again, no attempt is spent");
    assert!(record.runs[0].selected_worker.is_none());
    assert!(record.blocked_since.is_some());
    assert_eq!(record.blocked_reason, "version 4 is newer than 3");
    assert_eq!(
        fixture.scheduler.request_claim(&old(), &task).unwrap_err(),
        ClaimRejection::CannotRun
    );
    assert!(fixture.scheduler.claim_oldest(&old(), 10).unwrap().is_empty());
    let [again] = <[_; 1]>::try_from(fixture.scheduler.claim_oldest(&new(), 10).unwrap()).unwrap();
    assert_eq!(again.task_run_id, claim.task_run_id);
}

#[test]
fn a_coalescing_generation_given_back_waits_again_and_is_superseded_by_a_newer_one() {
    for newer_before_the_block in [false, true] {
        let mut fixture = Fixture::leading();
        let older = fixture
            .scheduler
            .submit(submitted_at(4).with_coalescing_key("k"))
            .unwrap();
        fixture.scheduler.request_claim(&old(), &older).unwrap();
        let mut newer = None;
        if newer_before_the_block {
            newer = Some(
                fixture
                    .scheduler
                    .submit(submitted_at(3).with_coalescing_key("k"))
                    .unwrap(),
            );
        }

        fixture
            .scheduler
            .report_blocked(&old(), &older, "version 4 is newer than 3".into(), false)
            .unwrap();
        let newer = newer.unwrap_or_else(|| {
            fixture
                .scheduler
                .submit(submitted_at(3).with_coalescing_key("k"))
                .unwrap()
        });

        let case = format!("newer submitted before the block: {newer_before_the_block}");
        assert_eq!(fixture.state(&older), TaskRunState::Superseded, "{case}");
        let [claim] = <[_; 1]>::try_from(fixture.scheduler.claim_oldest(&old(), 10).unwrap())
            .unwrap_or_else(|_| panic!("{case}: the newer generation is the one to claim"));
        assert_eq!(claim.task.task_id(), newer, "{case}");
        assert_eq!(claim.chain, vec![b"in".to_vec()], "{case}: it folds the given-back input");
    }
}

#[test]
fn an_incompatible_report_ends_the_task_at_once_and_spends_no_retry() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(submitted_at(1)).unwrap();
    let claim = fixture.scheduler.request_claim(&old(), &task).unwrap();

    let reason =
        "version 2 of shop.Order cannot be migrated to version 3: the bump is declared incompatible";
    let blocked = fixture
        .scheduler
        .report_blocked(&old(), &task, reason.into(), true)
        .unwrap();

    assert!(blocked.ended);
    assert_eq!(fixture.state(&task), TaskRunState::Failed);
    let record = fixture.spy.newest_revision_of(&task).unwrap();
    assert_eq!(record.runs.len(), 1, "never retried, whatever its retries");
    assert_eq!(record.runs[0].failure_kind, "MigrationError");
    assert_eq!(record.blocked_reason, reason);
    assert!(record.finished);
    assert!(fixture.scheduler.take_events().contains(&Event::MigrationFailed {
        task_id: task.clone(),
        task_run_id: claim.task_run_id,
        reason: reason.into(),
    }));
}

#[test]
fn a_task_blocked_past_the_ttl_fails_with_the_reason_unless_a_worker_started_it() {
    for started in [false, true] {
        let mut fixture = Fixture::leading();
        fixture.scheduler.set_version_block_ttl(ticks(100));
        let task = fixture.scheduler.submit(submitted_at(4)).unwrap();
        fixture
            .scheduler
            .report_blocked(&old(), &task, "version 4 is newer than 3".into(), false)
            .unwrap();
        if started {
            let claim = fixture.scheduler.request_claim(&new(), &task).unwrap();
            fixture
                .scheduler
                .report_started(&new(), &claim.task_run_id, Some(4))
                .unwrap();
        }
        pass(&fixture, 99);
        assert_eq!(fixture.scheduler.catch_up().migration_failed, 0, "started {started}");
        pass(&fixture, 1);
        let caught_up = fixture.scheduler.catch_up();

        if started {
            assert_eq!(caught_up.migration_failed, 0);
            assert_eq!(fixture.state(&task), TaskRunState::Running);
            assert!(fixture.spy.newest_revision_of(&task).unwrap().blocked_since.is_none());
        } else {
            assert_eq!(caught_up.migration_failed, 1);
            assert_eq!(fixture.state(&task), TaskRunState::Failed);
            let record = fixture.spy.newest_revision_of(&task).unwrap();
            assert_eq!(record.runs[0].failure_kind, "MigrationError");
            assert_eq!(record.blocked_reason, "version 4 is newer than 3");
        }
    }
}

#[test]
fn a_blocked_task_that_is_cancelled_leaves_no_block_deadline_and_no_stale_block_in_its_record() {
    let mut fixture = Fixture::leading();
    fixture.scheduler.set_version_block_ttl(ticks(100));
    let task = fixture.scheduler.submit(submitted_at(4)).unwrap();
    fixture
        .scheduler
        .report_blocked(&old(), &task, "newer".into(), false)
        .unwrap();
    assert!(fixture.scheduler.next_deadline().is_some(), "the block TTL is pending");

    fixture.scheduler.cancel(&task).unwrap();
    pass(&fixture, 500);

    assert_eq!(
        fixture.scheduler.next_deadline(),
        None,
        "a cancelled task has no block TTL left to wait for"
    );
    assert_eq!(fixture.scheduler.catch_up().migration_failed, 0);
    assert_eq!(fixture.state(&task), TaskRunState::Cancelled);
    let record = fixture.spy.newest_revision_of(&task).unwrap();
    assert!(record.blocked_since.is_none());
    assert!(record.blocked_reason.is_empty());
}

#[test]
fn an_incompatible_report_on_a_group_member_fails_the_group() {
    let mut fixture = Fixture::leading();
    let group = submitted(
        &mut fixture,
        group_of(
            OnError::FailFast,
            vec![task_stage("a", Some(b"x")), task_stage("b", Some(b"y"))],
        ),
    );

    let reason = "version 2 of shop.Order cannot be migrated to version 3";
    fixture
        .scheduler
        .report_blocked(&worker(), &group.member_task(0), reason.into(), true)
        .unwrap();

    let finished = finished(&mut fixture);
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].state, CompositeState::Failed);
    assert_eq!(finished[0].failure.as_ref().unwrap().kind, "MigrationError");
}

#[test]
fn a_successor_keeps_the_block_time_but_not_who_blocked() {
    let mut fixture = Fixture::leading();
    fixture.scheduler.set_version_block_ttl(ticks(100));
    let task = fixture.scheduler.submit(submitted_at(4)).unwrap();
    fixture
        .scheduler
        .report_blocked(&old(), &task, "newer".into(), false)
        .unwrap();
    pass(&fixture, 60);

    let mut successor = reconciling_after(&fixture);
    successor
        .clock
        .set_wall_clock_millis(fixture.clock.wall_clock_millis());
    successor
        .scheduler
        .reconcile(Rebuild {
            records: newest_records(&fixture),
            ..Rebuild::default()
        })
        .unwrap();
    successor.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));
    successor.scheduler.set_version_block_ttl(ticks(100));
    assert!(
        successor.scheduler.request_claim(&old(), &task).is_ok(),
        "the blocked set is not stored"
    );
    successor
        .scheduler
        .report_blocked(&old(), &task, "newer".into(), false)
        .unwrap();
    pass(&successor, 40);
    assert_eq!(
        successor.scheduler.catch_up().migration_failed,
        1,
        "the TTL counts from the first block"
    );
}
