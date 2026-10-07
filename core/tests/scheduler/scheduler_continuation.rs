//! A task that returns a continuation (an implicit flow): its run is certified
//! at once, but the task is not over until its continuation is, so a
//! coalescing key stays held and its memory stays counted until the client
//! ends the continuation.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::reconcile::Rebuild;
use kabudachi_core::scheduler::{Completion, ClaimRejection, Submission};
use crate::support::scheduler::{Fixture, OFFICE, grant_of, newest_records, reconciling_after};

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn generation() -> Submission {
    Submission::new(
        TaskDefinitionId::new("index.refresh"),
        0,
        b"g".to_vec(),
        "default",
    )
    .with_coalescing_key("k")
}

fn running(fixture: &mut Fixture, task: &TaskId) -> TaskRunId {
    let claim = fixture.scheduler.request_claim(&worker(), task).unwrap();
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    claim.task_run_id
}

/// A certified run whose task is not finished is the record of a continuation,
/// so a new leader rebuilds the flow's lifetime from the records alone.
#[test]
fn an_implicit_flows_lifetime_survives_a_leader_change() {
    let mut old = Fixture::leading();
    let task = old.scheduler.submit(generation()).unwrap();
    let run = running(&mut old, &task);
    old.scheduler
        .complete(&worker(), &run, Digest::blake3(b"step"), Completion::Continues)
        .unwrap();
    let waiting = old.scheduler.submit(generation()).unwrap();
    let mut new = reconciling_after(&old);

    new.scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(
        new.scheduler.request_claim(&worker(), &waiting),
        Err(ClaimRejection::KeyBusy),
        "the flow still holds its key"
    );
    assert_eq!(
        new.scheduler.end_continuation(&task),
        Ok(true),
        "and its continuation can still end"
    );
}
