//! A worker's store of Task records keeps the newest revision of each, in
//! the order every worker agrees on, and refuses anything else.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::task_record::{Origin, Put, PutRefusal, RecordVersion, VersionedRecords};
use kabudachi_core::time::{Duration, Instant};

const NOW: Instant = Instant::at(0);

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
        store.put(record("task-1", held, "q"), NOW).unwrap();
        assert_eq!(store.put(record("task-1", incoming, "q"), NOW), expected, "{case}");
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
    store.put(record("task-1", held, "q"), NOW).unwrap();

    assert_eq!(store.put(record("task-1", held, "q"), NOW), Ok(Put::Unchanged));
    assert_eq!(
        store.put(record("task-1", held, "other"), NOW),
        Err(PutRefusal::Conflicting)
    );
    assert_eq!(
        store.get(&TaskId::new("task-1")).unwrap().task.as_ref().unwrap().queue,
        "q"
    );

    let mut placed_elsewhere = record("task-1", held, "q");
    let placed_elsewhere_placement = vec![WorkerId::new("w9").into()];
    placed_elsewhere.placement = placed_elsewhere_placement.clone();
    assert_eq!(store.put(placed_elsewhere, NOW), Ok(Put::Stored), "only its placement differs");
    assert_eq!(store.get(&TaskId::new("task-1")).unwrap().placement, placed_elsewhere_placement);
}

#[test]
fn a_finished_record_is_dropped_once_its_retention_has_passed_and_an_unfinished_one_never_is() {
    let mut store = VersionedRecords::with_retention(Some(Duration::from_ticks(100)));
    let mut finished = record("done", version(0, 0, 1, 0), "q");
    finished.finished = true;
    store.put(finished, Instant::at(10)).unwrap();
    store
        .put(record("running", version(0, 0, 1, 1), "q"), Instant::at(10))
        .unwrap();

    assert_eq!(store.sweep(Instant::at(109)), 0);
    assert_eq!(store.next_due(), Some(Instant::at(110)));
    assert_eq!(store.sweep(Instant::at(110)), 1);
    assert!(store.get(&TaskId::new("done")).is_none());
    assert!(
        store.get(&TaskId::new("running")).is_some(),
        "an unfinished record never expires"
    );
}

#[test]
fn a_put_drops_expired_finished_records_first_and_is_never_refused_for_room() {
    let mut store = VersionedRecords::with_retention(Some(Duration::from_ticks(1)));
    for n in 0..1_000 {
        let mut over = record(&format!("over-{n}"), version(0, 0, 1, n), "q");
        over.finished = true;
        store.put(over, Instant::at(0)).unwrap();
    }

    store
        .put(record("new", version(0, 0, 1, 1_000), "q"), Instant::at(5))
        .unwrap();

    assert_eq!(
        store.len(),
        1,
        "the expired finished records went first; nothing refused the new one"
    );
}

#[test]
fn a_republished_finished_record_keeps_its_first_retention_deadline() {
    let mut store = VersionedRecords::with_retention(Some(Duration::from_ticks(100)));
    let mut finished = record("done", version(0, 0, 1, 0), "q");
    finished.finished = true;
    store.put(finished.clone(), Instant::at(10)).unwrap();
    store.put(finished, Instant::at(90)).unwrap();

    assert_eq!(store.next_due(), Some(Instant::at(110)));
}

/// `record` placed on `holders`.
fn placed(mut record: TaskRecord, holders: &[&str]) -> TaskRecord {
    record.placement = holders.iter().map(|id| WorkerId::new(*id).into()).collect();
    record
}

#[test]
fn a_holder_drops_its_copy_when_the_leader_moves_the_record_away() {
    let task = TaskId::new("t");
    let mut store = VersionedRecords::default().held_by(WorkerId::new("me"));
    store.put(placed(record("t", version(0, 0, 1, 0), "q"), &["me", "a"]), NOW).unwrap();

    let moved = placed(record("t", version(0, 0, 1, 1), "q"), &["a", "b"]);
    assert_eq!(store.put(moved, NOW), Ok(Put::Retired));
    assert!(store.get(&task).is_none(), "a copy nothing will update again would never go away");

    // The same revision placed anew without this holder retires it too.
    store.put(placed(record("t", version(0, 0, 1, 2), "q"), &["me"]), NOW).unwrap();
    let re_placed = placed(record("t", version(0, 0, 1, 2), "q"), &["a", "b"]);
    assert_eq!(store.put(re_placed, NOW), Ok(Put::Retired));
    assert!(store.get(&task).is_none());

    // A revision that is not newer than what is held is refused as ever, and a
    // record whose placement is not yet known names no one to leave out.
    store.put(placed(record("t", version(0, 0, 1, 5), "q"), &["me"]), NOW).unwrap();
    let older_and_away = placed(record("t", version(0, 0, 1, 4), "q"), &["a", "b"]);
    assert_eq!(store.put(older_and_away, NOW), Err(PutRefusal::Older));
    assert!(store.get(&task).is_some());
    let unplaced = record("t", version(0, 0, 1, 6), "q");
    assert_eq!(store.put(unplaced, NOW), Ok(Put::Stored));
}

#[test]
fn a_handed_off_copy_is_kept_whatever_its_placement_names_and_yields_to_a_newer_one() {
    let task = TaskId::new("t");
    let mut store = VersionedRecords::default().held_by(WorkerId::new("me"));
    let handed = placed(record("t", version(0, 0, 1, 1), "q"), &["drainer"]);

    assert_eq!(store.put_from(handed.clone(), Origin::HandOff, NOW), Ok(Put::Stored));
    assert_eq!(store.get(&task), Some(&handed), "kept until the leader places it");

    // A copy of the same revision from another drainer changes nothing.
    let same = placed(record("t", version(0, 0, 1, 1), "q"), &["other"]);
    assert_eq!(store.put_from(same, Origin::HandOff, NOW), Ok(Put::Unchanged));
    assert_eq!(store.get(&task), Some(&handed));

    // A copy older than the leader's is refused.
    store.put(placed(record("t", version(0, 0, 1, 9), "q"), &["me"]), NOW).unwrap();
    assert_eq!(store.put_from(handed, Origin::HandOff, NOW), Err(PutRefusal::Older));
}
