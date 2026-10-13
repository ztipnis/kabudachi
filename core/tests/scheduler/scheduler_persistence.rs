//! A certified run records whether its worker mirrors its result, and the
//! worker's later word on how that went; only that worker's word counts.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskRunId, WorkerId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::result_backend::PersistenceState;
use kabudachi_core::scheduler::{Completion, ReportRejection, Reported, Submission};

use crate::support::scheduler::{Fixture, newest_records};

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn persistence_of(fixture: &Fixture, run: &TaskRunId) -> generated::PersistenceState {
    fixture.scheduler.task_run(run).expect("the run is held").persistence()
}

fn published_persistence(fixture: &Fixture, run: &TaskRunId) -> generated::PersistenceState {
    newest_records(fixture)
        .iter()
        .flat_map(|record| record.runs.iter())
        .find(|candidate| candidate.task_run_id() == *run)
        .expect("a revision holds the run")
        .persistence()
}

#[test]
fn a_certified_run_is_pending_until_its_worker_says_how_its_mirror_went() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(Submission::new(
            TaskDefinitionId::new("billing.charge"),
            0,
            b"in".to_vec(),
            "default",
        ))
        .unwrap();
    let run = fixture.scheduler.request_claim(&worker(), &task).unwrap().task_run_id;
    fixture.scheduler.report_started(&worker(), &run, None).unwrap();
    // Before it succeeds, no word on persistence is taken.
    assert_eq!(
        fixture.scheduler.note_persistence(&worker(), &run, PersistenceState::Stored),
        Err(ReportRejection::NotAuthoritative)
    );
    fixture
        .scheduler
        .complete_reported(
            &worker(),
            &run,
            Digest::blake3(b"out"),
            Completion::Final,
            Reported { delivered: false, persistence: PersistenceState::Pending },
        )
        .unwrap();
    assert_eq!(persistence_of(&fixture, &run), generated::PersistenceState::Pending);

    // Neither a state the worker cannot report nor another worker's word is taken.
    assert_eq!(
        fixture.scheduler.note_persistence(&worker(), &run, PersistenceState::Pending),
        Err(ReportRejection::NotAuthoritative)
    );
    assert_eq!(
        fixture.scheduler.note_persistence(&WorkerId::new("w2"), &run, PersistenceState::Stored),
        Err(ReportRejection::NotAuthoritative)
    );
    assert_eq!(persistence_of(&fixture, &run), generated::PersistenceState::Pending);

    // The run's own worker's word is taken, twice alike, and a later `Stored`
    // replaces `Failed`.
    fixture.scheduler.note_persistence(&worker(), &run, PersistenceState::Failed).unwrap();
    assert_eq!(persistence_of(&fixture, &run), generated::PersistenceState::Failed);
    fixture.scheduler.note_persistence(&worker(), &run, PersistenceState::Stored).unwrap();
    fixture.scheduler.note_persistence(&worker(), &run, PersistenceState::Stored).unwrap();
    assert_eq!(persistence_of(&fixture, &run), generated::PersistenceState::Stored);
    assert_eq!(
        published_persistence(&fixture, &run),
        generated::PersistenceState::Stored,
        "the word is published in a revision of the finished record"
    );
}
