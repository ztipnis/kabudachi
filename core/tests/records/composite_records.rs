//! A holder keeps composite records and their pages by the same rules as
//! Task records: newest revision wins, a moved one leaves a stub, and a
//! finished one goes once its retention passes.

use kabudachi_core::blob::{BlobKey, BlobOwner, BlobRecord};
use kabudachi_core::protocol::generated::{CompositeRecord, WorkerId as WireWorker};
use kabudachi_core::protocol::ids::{CompositeId, WorkerId};
use kabudachi_core::task_record::{OwnerState, Put, PutRefusal, RecordKey, VersionedRecords};
use kabudachi_core::time::{Duration, Instant};

use crate::support::composites::{composite_record, page};

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
    let mut finished = placed_on(composite_record(&id, 1, 2), &["a", "c"]);
    finished.finished = true;
    store.put(finished, start).unwrap();
    assert!(store.blob(&key).is_some(), "a result stays as long as the finished record");
    store.sweep(start + Duration::from_millis(10));
    assert!(store.blob(&key).is_none(), "gone with its finished composite");
}
