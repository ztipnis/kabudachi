//! A worker's store of Task records keeps the newest revision of each, in
//! the order every worker agrees on, and refuses anything else.

use kabudachi_core::blob::{BlobKey, BlobOwner, BlobRecord};
use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::task_record::{Origin, Put, PutRefusal, RecordVersion, ShardRecord, VersionedRecords};
use kabudachi_core::time::{Duration, Instant};

use crate::support::composites::{composite_record, page};

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
        ("another lineage's epoch at the same number, ranked above", version(3, 6, 1, 0), Ok(Put::Stored)),
        ("another lineage's epoch at the same number, ranked below", version(3, 4, 9, 0), Err(PutRefusal::Older)),
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
fn a_finished_record_its_leader_keeps_for_a_composite_stays_with_its_result_blobs_until_a_revision_lets_go() {
    let mut store = VersionedRecords::with_retention(Some(Duration::from_ticks(100)));
    let member = TaskId::new("member");
    let result = BlobRecord {
        value: b"result".to_vec(),
        owner: BlobOwner::Task(member.clone()),
        placement: Vec::new(),
        result: true,
    };
    let mut kept = record("member", version(0, 0, 1, 0), "q");
    kept.finished = true;
    kept.retained_by_composite = true;
    store.put(kept.clone(), Instant::at(10)).unwrap();
    store.put_blob(&BlobKey::of(b"result"), result, Instant::at(10)).unwrap();

    assert_eq!(store.sweep(Instant::at(100_000)), 0, "kept however long ago it finished");
    assert!(store.get(&member).is_some());
    assert_eq!(store.blobs_of(&BlobOwner::Task(member.clone())).len(), 1);
    assert_eq!(store.next_due(), None);

    let mut released = kept;
    released.version = Some(version(0, 0, 1, 1).into());
    released.retained_by_composite = false;
    store.put(released, Instant::at(100_000)).unwrap();

    assert_eq!(store.sweep(Instant::at(100_099)), 0, "its retention counts from the revision that let go");
    assert_eq!(store.next_due(), Some(Instant::at(100_100)));
    assert_eq!(store.sweep(Instant::at(100_100)), 2, "the record and its blob");
    assert!(store.get(&member).is_none());
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

#[test]
fn a_holder_that_a_record_moved_away_from_reports_where_it_went_until_the_task_finishes() {
    let task = TaskId::new("t");
    let retention = Duration::from_ticks(100);
    let mut store = VersionedRecords::with_retention(Some(retention)).held_by(WorkerId::new("me"));
    store.put(placed(record("t", version(0, 0, 1, 0), "q"), &["me", "a"]), NOW).unwrap();

    let moved = placed(record("t", version(0, 0, 1, 1), "q"), &["a", "b"]);
    assert_eq!(store.put(moved, NOW), Ok(Put::Retired));

    // Nothing is served, handed off or claimed from the stub, but a reader
    // that asks this holder learns the revision and its holders.
    assert!(store.get(&task).is_none());
    assert_eq!(store.iter().count(), 0);
    let reported: Vec<&ShardRecord> = store.reported().collect();
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].identify().map(|(_, version)| version), Ok(version(0, 0, 1, 1)));
    assert_eq!(reported[0].placement(), placed(TaskRecord::default(), &["a", "b"]).placement);

    // A write that is not newer than the stub is refused, and one that names
    // this holder again replaces it with the record.
    assert_eq!(store.put(placed(record("t", version(0, 0, 1, 0), "q"), &["me"]), NOW), Err(PutRefusal::Older));
    let back = placed(record("t", version(0, 0, 1, 2), "q"), &["me", "b"]);
    assert_eq!(store.put(back.clone(), NOW), Ok(Put::Stored));
    assert_eq!(store.get(&task), Some(&back));

    // The stub of a finished task goes with its retention.
    let mut finished = placed(record("t", version(0, 0, 1, 3), "q"), &["a", "b"]);
    finished.finished = true;
    assert_eq!(store.put(finished, NOW), Ok(Put::Retired));
    assert_eq!(store.reported().count(), 1);
    store.sweep(NOW + retention);
    assert_eq!(store.reported().count(), 0, "the stub does not outlive the finished task");
}

#[test]
fn a_stub_of_a_revision_takes_the_same_revision_again_whether_it_names_this_holder_or_not() {
    let task = TaskId::new("t");
    let mut store = VersionedRecords::default().held_by(WorkerId::new("me"));
    store.put(placed(record("t", version(0, 0, 1, 0), "q"), &["me", "a"]), NOW).unwrap();
    let moved = placed(record("t", version(0, 0, 1, 1), "q"), &["a", "b"]);
    assert_eq!(store.put(moved, NOW), Ok(Put::Retired));

    // The same revision placed elsewhere again is the same stub, not a conflict.
    let elsewhere_again = placed(record("t", version(0, 0, 1, 1), "q"), &["b", "c"]);
    assert_eq!(store.put(elsewhere_again, NOW), Ok(Put::Retired));
    assert!(store.get(&task).is_none());

    // The same revision naming this holder now is the revision itself.
    let back = placed(record("t", version(0, 0, 1, 1), "q"), &["me", "b"]);
    assert_eq!(store.put(back.clone(), NOW), Ok(Put::Stored));
    assert_eq!(store.get(&task), Some(&back));
}

/// `record` with a different input: the same task submitted otherwise.
fn resubmitted(mut record: TaskRecord) -> TaskRecord {
    record.task.as_mut().expect("a record carries its task").serialized_input = b"other input".to_vec();
    record
}

#[test]
fn a_revision_that_changes_what_the_first_revision_fixed_is_refused_from_any_sender_even_after_a_stub() {
    let (held, newer, newest) = (version(0, 0, 1, 7), version(0, 0, 2, 0), version(0, 0, 3, 0));

    let mut store = VersionedRecords::default();
    store.put(record("task-1", held, "q"), NOW).unwrap();
    let mut with_a_run = record("task-1", newer, "q");
    with_a_run.runs.push(Default::default());
    assert_eq!(store.put(with_a_run, NOW), Ok(Put::Stored), "a later revision of the same submission");
    assert_eq!(
        store.put(resubmitted(record("task-1", newest, "q")), NOW),
        Err(PutRefusal::SubmissionChanged),
        "a later revision with another input"
    );
    assert_eq!(
        store.put_from(record("task-1", newest, "other queue"), Origin::HandOff, NOW),
        Err(PutRefusal::SubmissionChanged),
        "a handed-off copy with another queue"
    );
    assert_eq!(store.get(&TaskId::new("task-1")).unwrap().runs.len(), 1, "the store kept what it had");
    assert_eq!(
        store.put(resubmitted(record("task-1", newer, "q")), NOW),
        Err(PutRefusal::Conflicting),
        "the same version with another input stays a conflict"
    );
    assert_eq!(
        store.put(resubmitted(record("task-1", held, "q")), NOW),
        Err(PutRefusal::Older),
        "an older version stays older"
    );

    // A holder the leader moved the record away from keeps only a stub,
    // which still knows what was submitted.
    let mut holder = VersionedRecords::default().held_by(WorkerId::new("me"));
    holder.put(placed(record("t", held, "q"), &["me"]), NOW).unwrap();
    assert_eq!(holder.put(placed(record("t", newer, "q"), &["other"]), NOW), Ok(Put::Retired));
    assert_eq!(
        holder.put(placed(resubmitted(record("t", newer, "q")), &["me"]), NOW),
        Err(PutRefusal::SubmissionChanged),
        "the stub's own revision, resubmitted"
    );
    assert_eq!(
        holder.put(placed(resubmitted(record("t", newest, "q")), &["me"]), NOW),
        Err(PutRefusal::SubmissionChanged)
    );
    assert_eq!(holder.put(placed(record("t", newest, "q"), &["me"]), NOW), Ok(Put::Stored));

    // A composite's step description is fixed the same way; its pages fill in.
    let id = CompositeId::new("c1");
    let mut composites = VersionedRecords::with_retention(None);
    composites.put(composite_record(&id, 0, 1), NOW).unwrap();
    let mut redescribed = composite_record(&id, 0, 2);
    let description = redescribed.description.as_mut().expect("a composite record carries its step");
    description.stages.push(description.stages[0].clone());
    assert_eq!(composites.put(redescribed, NOW), Err(PutRefusal::SubmissionChanged));
    assert_eq!(composites.put(composite_record(&id, 0, 2), NOW), Ok(Put::Stored));
    composites.put(page(&id, 0, 0, 1), NOW).unwrap();
    let mut filled = page(&id, 0, 0, 2);
    filled.finished = true;
    assert_eq!(composites.put(filled, NOW), Ok(Put::Stored), "a page is not fixed by its first revision");

    // A first revision without a description fixed that too.
    let bare = CompositeId::new("c2");
    let mut undescribed = composite_record(&bare, 0, 1);
    undescribed.description = None;
    composites.put(undescribed.clone(), NOW).unwrap();
    assert_eq!(
        composites.put(composite_record(&bare, 0, 2), NOW),
        Err(PutRefusal::SubmissionChanged),
        "a description added after a first revision without one"
    );
    undescribed.version = composite_record(&bare, 0, 2).version;
    assert_eq!(composites.put(undescribed, NOW), Ok(Put::Stored), "a later revision still without one");
}
