//! A holder keeps composite records and their pages by the same rules as
//! Task records: newest revision wins, a moved one leaves a stub, and a
//! finished one goes once its retention passes.

use kabudachi_core::blob::{BlobKey, BlobOwner, BlobRecord};
use kabudachi_core::protocol::generated::{
    CoalescingLink, CompositeRecord, Task, TaskRecord, WorkerId as WireWorker,
};
use kabudachi_core::protocol::ids::{CompositeId, TaskId, WorkerId};
use kabudachi_core::scheduler::Observer;
use kabudachi_core::task_record::{
    EffectGate, LocalRecords, OwnerState, Put, PublishedRevisions, PutRefusal, RecordKey,
    RecordVersion, Settled, Settlement, ShardRecord, VersionedRecords, Waits, Write, WriteLedger, WriteOrder,
};
use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::time::{Duration, Instant, RealClock};

use crate::support::composites::{composite_record, page};
use crate::support::spy::Spy;

#[test]
fn composite_records_and_pages_keep_their_newest_revision_under_their_own_keys() {
    let mut store = VersionedRecords::with_retention(None);
    let id = CompositeId::new("c1");
    let now = Instant::at(0);
    assert_eq!(store.put(composite_record(&id, 0, 3), now), Ok(Put::Stored));
    assert_eq!(store.put(page(&id, 0, 1, 4), now), Ok(Put::Stored));
    assert_eq!(store.put(page(&id, 1, 1, 5), now), Ok(Put::Stored));
    assert_eq!(store.put(composite_record(&id, 0, 2), now), Err(PutRefusal::Older));
    assert_eq!(store.put(composite_record(&id, 0, 4), now), Ok(Put::Stored));
    let keys: Vec<RecordKey> = store
        .records()
        .map(|record| record.identify().unwrap().0)
        .collect();
    assert_eq!(
        keys,
        [
            RecordKey::Composite(id.clone()),
            RecordKey::Page(id.clone(), 0),
            RecordKey::Page(id.clone(), 1)
        ]
    );
    assert!(store.iter().next().is_none(), "no Task record is held");
    for key in &keys {
        assert_eq!(RecordKey::from_kad_bytes(&key.kad_bytes()).as_ref(), Some(key));
    }
    assert_eq!(RecordKey::Page(id, 1).kad_bytes(), b"composite/c1/1");
    assert_eq!(RecordKey::from_kad_bytes(b"composite/c1/x"), None, "a page number is a number");
}

#[test]
fn a_finished_composite_goes_after_its_retention_and_a_moved_one_leaves_a_stub() {
    let me = WorkerId::new("me");
    let mut store = VersionedRecords::with_retention(Some(Duration::from_millis(10))).held_by(me);
    let (moved, done) = (CompositeId::new("moved"), CompositeId::new("done"));
    let start = Instant::at(0);
    let mut elsewhere = composite_record(&moved, 0, 1);
    elsewhere.placement = vec![WireWorker { value: "other".into() }];
    elsewhere.finished = true;
    assert_eq!(store.put(elsewhere, start), Ok(Put::Retired));
    assert!(store.get_record(&RecordKey::Composite(moved.clone())).is_none());
    assert_eq!(store.reported().count(), 1, "the stub is still reported");
    let mut finished = page(&done, 0, 0, 1);
    finished.finished = true;
    store.put(finished, start).unwrap();
    assert_eq!(store.sweep(start + Duration::from_millis(10)), 2);
    assert_eq!(store.reported().count(), 0);
}

#[test]
fn a_composites_blobs_are_kept_while_it_is_unfinished_move_with_it_and_go_with_it() {
    let grace = Duration::from_secs(1);
    let mut store = VersionedRecords::with_retention(Some(Duration::from_millis(10)))
        .with_orphan_blob_grace(grace)
        .held_by(WorkerId::new("a"));
    let id = CompositeId::new("c1");
    let owner = BlobOwner::Composite(id.clone());
    let item = b"item";
    let key = BlobKey::of(item);
    let start = Instant::at(0);
    // A nested composite's record is written only when its stage starts.
    // A blob it owned would be asked about once the grace passed, and
    // dropped when no record was found; so the root owns such values.
    let not_started = BlobOwner::Composite(id.child(1));
    let nested_item = b"nested item";
    store
        .put_blob(
            &BlobKey::of(nested_item),
            BlobRecord {
                value: nested_item.to_vec(),
                owner: not_started.clone(),
                placement: Vec::new(),
                result: false,
            },
            start,
        )
        .unwrap();
    let placed_on = |mut record: CompositeRecord, holders: &[&str]| {
        record.placement = holders.iter().map(|holder| WireWorker { value: (*holder).into() }).collect();
        record
    };
    store.put(placed_on(composite_record(&id, 1, 0), &["a", "b"]), start).unwrap();
    store
        .put_blob(
            &key,
            BlobRecord { value: item.to_vec(), owner: owner.clone(), placement: Vec::new(), result: true },
            start,
        )
        .unwrap();
    assert_eq!(store.sweep(start + Duration::from_secs(3600)), 0, "kept while the composite is unfinished");
    assert_eq!(
        store.owners_to_check(start + Duration::from_secs(3600)),
        [not_started.clone()],
        "past the grace, only the owner whose record was never held is asked about; the root is not"
    );
    store.owner_checked(&not_started, OwnerState::Gone, start + Duration::from_secs(3600));
    assert!(store.blob(&key).is_some(), "the root's blob outlives the grace");
    store.put(placed_on(composite_record(&id, 1, 1), &["a", "c"]), start).unwrap();
    assert_eq!(
        store.take_blob_moves(),
        vec![(owner.clone(), vec![WorkerId::new("a"), WorkerId::new("c")])]
    );
    let input = b"input";
    store
        .put_blob(
            &BlobKey::of(input),
            BlobRecord { value: input.to_vec(), owner: owner.clone(), placement: Vec::new(), result: false },
            start,
        )
        .unwrap();
    let mut finished = placed_on(composite_record(&id, 1, 2), &["a", "c"]);
    finished.finished = true;
    store.put(finished, start).unwrap();
    assert!(store.blob(&BlobKey::of(input)).is_none(), "a value that is not a result goes when the composite finishes");
    assert!(store.blob(&key).is_some(), "a result stays as long as the finished record");
    store.sweep(start + Duration::from_millis(10));
    assert!(store.blob(&key).is_none(), "gone with its finished composite");
}

#[test]
fn a_node_storing_its_own_records_keeps_published_composite_revisions_as_placed_on_itself() {
    let node = WorkerId::new("worker-1");
    let mut records = LocalRecords::new(node.clone(), RealClock::new(), None);
    let id = CompositeId::new("c1");

    records.composite_revision(composite_record(&id, 1, 0));
    records.page_revision(page(&id, 0, 1, 1));

    let held = records.records();
    let composite = held.get_record(&RecordKey::Composite(id.clone())).expect("the composite is stored");
    assert_eq!(composite.placement(), [WireWorker::from(node.clone())]);
    let held_page = held.get_record(&RecordKey::Page(id.clone(), 0)).expect("the page is stored");
    assert_eq!(held_page.placement(), [WireWorker::from(node)]);
    let settled = records.take_settled();
    assert_eq!(
        settled.iter().map(|(write, kept)| (write.key.clone(), *kept)).collect::<Vec<_>>(),
        [(RecordKey::Composite(id.clone()), true), (RecordKey::Page(id, 0), true)]
    );
}

fn version_at(revision: u64) -> RecordVersion {
    RecordVersion { recovery_epoch: RecoveryEpoch::new(0, 0), leader_term: 1, revision }
}

#[test]
fn an_answer_about_a_composite_waits_on_that_composites_writes_and_on_nothing_else() {
    let id = CompositeId::new("c1");
    let composite = |revision| Write { key: RecordKey::Composite(id.clone()), version: version_at(revision) };
    let task_write = Write { key: RecordKey::Task(TaskId::new("c1")), version: version_at(0) };
    let mut ledger = WriteLedger::default();
    ledger.made(&[composite(0), task_write.clone()]);

    assert_eq!(ledger.waits_on(&RecordKey::Composite(id.clone())), Waits::Writes(vec![composite(0)]));
    assert_eq!(ledger.waits_on(&RecordKey::Page(id.clone(), 0)), Waits::Writes(vec![]));

    let mut gate = EffectGate::new();
    assert!(gate.hold("finished", [composite(0)]).is_none());
    assert!(gate.acknowledged(&task_write, true).is_empty(), "a task of the same name is another record");
    assert_eq!(gate.acknowledged(&composite(1), true), [Settled::Released("finished")]);
}

#[test]
fn a_composite_or_page_is_written_at_once_while_a_superseded_task_waits_for_its_successor() {
    let task = |id: &str, superseded_by: Option<&str>, revision| {
        ShardRecord::Task(TaskRecord {
            version: Some(version_at(revision).into()),
            task: Some(Task { task_id: Some(TaskId::new(id).into()), ..Default::default() }),
            link: superseded_by.map(|newer| CoalescingLink {
                superseded_by: Some(TaskId::new(newer).into()),
                ..Default::default()
            }),
            ..Default::default()
        })
    };
    let id = CompositeId::new("c1");
    let mut outbox = Spy::default();
    outbox.revision(match task("new", None, 1) {
        ShardRecord::Task(record) => record,
        _ => unreachable!(),
    });
    outbox.composite_revision(composite_record(&id, 1, 2));
    outbox.page_revision(page(&id, 0, 1, 3));
    let ShardRecord::Task(old) = task("old", Some("new"), 4) else { unreachable!() };
    outbox.revision(old);

    assert_eq!(outbox.newest_composite(&id), Some(composite_record(&id, 1, 2)));
    assert_eq!(outbox.newest_page(&id, 0), Some(page(&id, 0, 1, 3)));
    let published = outbox.take_published();
    let mut order = WriteOrder::default();
    let now = order.admit(published.clone());
    assert_eq!(now, published[..3], "the old generation is held behind the new one");

    let new = Write::of(&published[0]);
    assert_eq!(order.settled(&new, true), Settlement::Release(vec![published[3].clone()]));
}
