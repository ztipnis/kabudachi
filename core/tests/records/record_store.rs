//! A worker's store of Task records keeps the newest revision of each, in
//! the order every worker agrees on, and refuses anything else.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId};
use kabudachi_core::task_record::{Put, PutRefusal, RecordVersion, VersionedRecords};

fn version(number: u64, lineage: u64, term: u64, revision: u64) -> RecordVersion {
    RecordVersion {
        recovery_epoch: RecoveryEpoch::new(number, lineage),
        leader_term: term,
        revision,
    }
}

fn record(task: &str, version: RecordVersion, queue: &str) -> TaskRecord {
    TaskRecord {
        version: Some(version.into()),
        task: Some(Task {
            task_id: Some(TaskId::new(task).into()),
            task_definition_id: Some(TaskDefinitionId::new("definition").into()),
            queue: queue.to_owned(),
            ..Task::default()
        }),
        ..TaskRecord::default()
    }
}

#[test]
fn the_store_orders_revisions_by_epoch_then_term_then_revision() {
    let held = version(3, 5, 4, 10);
    let cases = [
        ("a later revision of the same term", version(3, 5, 4, 11), Ok(Put::Stored)),
        ("an earlier revision of the same term", version(3, 5, 4, 9), Err(PutRefusal::Older)),
        ("a later term with a lower revision", version(3, 5, 5, 0), Ok(Put::Stored)),
        ("an earlier term with a higher revision", version(3, 5, 3, 99), Err(PutRefusal::Older)),
        ("a later epoch of the lineage with an earlier term", version(4, 5, 1, 0), Ok(Put::Stored)),
        ("an earlier epoch of the lineage with a later term", version(2, 5, 9, 0), Err(PutRefusal::Older)),
        ("another lineage's epoch numbered above", version(4, 6, 1, 0), Ok(Put::Stored)),
        ("another lineage's epoch at the same number", version(3, 6, 9, 0), Err(PutRefusal::Older)),
        ("another lineage's epoch numbered below", version(2, 6, 9, 0), Err(PutRefusal::Older)),
    ];
    for (case, incoming, expected) in cases {
        let mut store = VersionedRecords::default();
        store.put(record("task-1", held, "q")).unwrap();
        assert_eq!(store.put(record("task-1", incoming, "q")), expected, "{case}");
        let kept = if expected.is_ok() { incoming } else { held };
        assert_eq!(
            store
                .get(&TaskId::new("task-1"))
                .and_then(|r| r.version.as_ref())
                .map(RecordVersion::from),
            Some(kept),
            "{case}: the store holds the newer of the two"
        );
    }
}

#[test]
fn the_same_version_is_a_republish_only_when_the_record_is_identical() {
    let mut store = VersionedRecords::default();
    let held = version(0, 0, 1, 7);
    store.put(record("task-1", held, "q")).unwrap();

    assert_eq!(store.put(record("task-1", held, "q")), Ok(Put::Unchanged));
    assert_eq!(
        store.put(record("task-1", held, "other")),
        Err(PutRefusal::Conflicting)
    );
    assert_eq!(
        store.get(&TaskId::new("task-1")).unwrap().task.as_ref().unwrap().queue,
        "q"
    );
}

#[test]
fn a_record_without_a_version_or_task_id_is_refused() {
    let mut store = VersionedRecords::default();
    let mut unversioned = record("task-1", version(0, 0, 1, 0), "q");
    unversioned.version = None;
    let mut anonymous = record("task-1", version(0, 0, 1, 0), "q");
    anonymous.task.as_mut().unwrap().task_id = None;

    assert_eq!(store.put(unversioned), Err(PutRefusal::Malformed));
    assert_eq!(store.put(anonymous), Err(PutRefusal::Malformed));
    assert!(store.is_empty());
}
