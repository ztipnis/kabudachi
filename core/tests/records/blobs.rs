//! Payloads too large for one message: cut into content-addressed chunks
//! named by an ordered manifest, and joined back only whole and unaltered.

use std::collections::BTreeMap;

use kabudachi_core::blob::{BlobKey, BlobOwner, BlobRecord};
use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::manifest::{
    BLOB_CHUNK_BYTES, Failure, INLINE_PAYLOAD_BYTES, Manifest, ManifestEntry, Unresolved, fits_inline,
};
use kabudachi_core::protocol::digest::{Digest, DigestAlgorithm};
use kabudachi_core::protocol::generated::{self, Task, TaskRecord, manifest_entry};
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::task_record::{BlobRefusal, Origin, OwnerState, RecordVersion, VersionedRecords};
use kabudachi_core::time::{Duration, Instant};

/// `len` bytes that differ from chunk to chunk, so a reordering shows.
fn varied(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at % 251) as u8).collect()
}

#[test]
fn a_payload_past_the_inline_bound_is_cut_into_chunks_that_join_back_only_whole_and_unaltered() {
    let bound = INLINE_PAYLOAD_BYTES as usize;
    assert!(fits_inline(bound));
    assert!(!fits_inline(bound + 1));

    let payload = varied(bound + 3 * BLOB_CHUNK_BYTES + 7);
    let chunked = Manifest::chunk(&payload);
    let sizes: Vec<usize> = chunked.chunks.iter().map(Vec::len).collect();
    assert!(sizes[..sizes.len() - 1].iter().all(|size| *size == BLOB_CHUNK_BYTES), "{sizes:?}");
    assert_eq!(sizes.iter().sum::<usize>(), payload.len());
    assert_eq!(chunked.manifest.total_bytes(), payload.len() as u64);
    let keys: Vec<BlobKey> = chunked.manifest.blob_keys().cloned().collect();
    assert_eq!(keys, chunked.chunks.iter().map(|chunk| BlobKey::of(chunk)).collect::<Vec<_>>());

    let held: BTreeMap<BlobKey, Vec<u8>> = keys.iter().cloned().zip(chunked.chunks.iter().cloned()).collect();
    assert_eq!(chunked.manifest.join(|key| held.get(key).cloned()), Ok(payload.clone()));

    // The wire form round-trips, and its digest names exactly this payload.
    let wire = generated::Manifest::from(chunked.manifest.clone());
    let back = Manifest::try_from(&wire).expect("a well-formed manifest");
    assert_eq!(back, chunked.manifest);
    assert_eq!(back.digest(), chunked.manifest.digest());
    assert_ne!(chunked.manifest.digest(), Manifest::chunk(&payload[1..]).manifest.digest());

    // A chunk no one holds, one altered, chunks in another order, a length
    // the manifest does not name: none joins.
    let first = keys[0].clone();
    assert_eq!(
        chunked.manifest.join(|key| (*key != first).then(|| held[key].clone())),
        Err(Unresolved::Missing(first.clone()))
    );
    let altered = chunked.manifest.join(|key| {
        let mut bytes = held[key].clone();
        if *key == first {
            bytes[0] ^= 1;
        }
        Some(bytes)
    });
    assert_eq!(altered, Err(Unresolved::Corrupt(first.clone())));
    let mut swapped: Vec<ManifestEntry> = chunked.manifest.entries().to_vec();
    swapped.swap(0, 1);
    let swapped = Manifest::new(swapped, payload.len() as u64);
    assert_ne!(swapped.join(|key| held.get(key).cloned()), Ok(payload.clone()));
    let short = Manifest::new(chunked.manifest.entries().to_vec(), payload.len() as u64 + 1);
    assert_eq!(
        short.join(|key| held.get(key).cloned()),
        Err(Unresolved::WrongLength { expected: payload.len() as u64 + 1, actual: payload.len() as u64 })
    );

    // A list of outcomes is not one payload.
    let outcomes = Manifest::new(
        vec![ManifestEntry::Failure(Failure { kind: "ValueError".into(), message: "no".into() })],
        0,
    );
    assert_eq!(outcomes.join(|_| None), Err(Unresolved::NotAPayload));

    // A manifest whose entry names no usable digest is refused on arrival.
    let mut unusable = wire.clone();
    unusable.entries[0].entry = Some(manifest_entry::Entry::Blob(generated::Digest {
        algorithm: generated::DigestAlgorithm::Unspecified as i32,
        value: vec![0; 32],
    }));
    assert!(Manifest::try_from(&unusable).is_err());

    // A blob's record key is its digest's, and nothing else parses as one.
    assert_eq!(BlobKey::from_record_key(&first.record_key()), Some(first.clone()));
    assert_eq!(BlobKey::from(&first.digest()), first);
    assert_eq!(
        first.digest(),
        Digest::new(DigestAlgorithm::Blake3, blake3::hash(&chunked.chunks[0]).as_bytes().to_vec()).unwrap()
    );
    assert_eq!(BlobKey::from_record_key(b"0192a3b4-task-id"), None);
    assert_eq!(BlobKey::from_record_key(b"blob/not-hex"), None);

    // A blob record keeps its owner across the wire.
    let record = BlobRecord {
        value: b"chunk".to_vec(),
        owner: BlobOwner::Task(TaskId::new("t1")),
        placement: Vec::new(),
        result: false,
    };
    assert_eq!(BlobRecord::try_from(generated::BlobRecord::from(record.clone())), Ok(record));
}

fn at(ticks: u64) -> Instant {
    Instant::at(ticks)
}

/// Revision `revision` of `task`'s record, placed on `holders`, finished or not.
fn task_record(task: &str, revision: u64, holders: &[&str], finished: bool) -> TaskRecord {
    TaskRecord {
        version: Some(RecordVersion { recovery_epoch: RecoveryEpoch::new(0, 0), leader_term: 1, revision }.into()),
        task: Some(Task {
            task_id: Some(TaskId::new(task).into()),
            task_definition_id: Some(TaskDefinitionId::new("definition").into()),
            ..Task::default()
        }),
        placement: holders.iter().map(|holder| WorkerId::new(*holder).into()).collect(),
        finished,
        ..TaskRecord::default()
    }
}

fn owned_by(task: &str, value: &[u8]) -> BlobRecord {
    BlobRecord {
        value: value.to_vec(),
        owner: BlobOwner::Task(TaskId::new(task)),
        placement: Vec::new(),
        result: false,
    }
}

fn task(task: &str) -> BlobOwner {
    BlobOwner::Task(TaskId::new(task))
}

#[test]
fn a_holder_keeps_a_blob_under_its_own_digest_while_its_owner_is_unfinished_and_notes_where_it_must_move() {
    const RETENTION: u64 = 100;
    const GRACE: u64 = 1000;
    let store = || {
        VersionedRecords::with_retention(Some(Duration::from_ticks(RETENTION)))
            .with_orphan_blob_grace(Duration::from_ticks(GRACE))
            .held_by(WorkerId::new("a"))
    };
    let chunk = b"chunk";
    let key = BlobKey::of(chunk);

    // Only under its own digest, and no larger than a chunk.
    let mut held = store();
    assert_eq!(held.put_blob(&BlobKey::of(b"other"), owned_by("t1", chunk), at(0)), Err(BlobRefusal::WrongKey));
    let oversized = vec![0; kabudachi_core::blob::MAX_BLOB_BYTES + 1];
    assert_eq!(
        held.put_blob(&BlobKey::of(&oversized), owned_by("t1", &oversized), at(0)),
        Err(BlobRefusal::TooLarge)
    );

    // The same bytes for two tasks: one blob, two owners.
    assert_eq!(held.put_blob(&key, owned_by("t1", chunk), at(0)), Ok(()));
    assert_eq!(held.put_blob(&key, owned_by("t2", chunk), at(0)), Ok(()));
    assert_eq!(held.blobs(), vec![(key.clone(), vec![task("t1"), task("t2")])]);
    assert_eq!(held.blobs_of(&task("t1")), vec![(key.clone(), chunk.to_vec())]);

    // t1's record is held unfinished: kept past any grace, and never asked about.
    held.put(task_record("t1", 1, &["a", "b"], false), at(10)).unwrap();
    held.sweep(at(10 * GRACE));
    assert!(held.blob(&key).is_some());
    assert_eq!(held.owners_to_check(at(10 * GRACE)), vec![task("t2")], "only the never-seen owner");

    // t2 is still unfinished somewhere in the shard: kept, and asked about
    // again a grace later. Once found gone, it lets go at once.
    held.owner_checked(&task("t2"), OwnerState::Unfinished, at(10 * GRACE));
    assert!(held.owners_to_check(at(10 * GRACE + GRACE - 1)).is_empty());
    held.owner_checked(&task("t2"), OwnerState::Gone, at(11 * GRACE));
    assert_eq!(held.blobs(), vec![(key.clone(), vec![task("t1")])]);

    // The leader moves t1's record from [a, b] to [a, c]: the move is noted,
    // once; the same placement again notes nothing.
    held.put(task_record("t1", 2, &["a", "c"], false), at(11 * GRACE)).unwrap();
    assert_eq!(held.take_blob_moves(), vec![(task("t1"), vec![WorkerId::new("a"), WorkerId::new("c")])]);
    assert!(held.take_blob_moves().is_empty());
    held.put(task_record("t1", 3, &["a", "c"], false), at(11 * GRACE)).unwrap();
    assert!(held.take_blob_moves().is_empty());

    // Moved off this holder entirely, it keeps a stub, and so the blob,
    // while the record is unfinished; the move is noted for it to push.
    held.put(task_record("t1", 4, &["b", "c"], false), at(11 * GRACE)).unwrap();
    assert_eq!(held.take_blob_moves(), vec![(task("t1"), vec![WorkerId::new("b"), WorkerId::new("c")])]);
    held.sweep(at(20 * GRACE));
    assert!(held.blob(&key).is_some(), "a stub of an unfinished owner keeps its blobs");

    // Finished, the record (here, its stub) goes after its retention, and
    // the blob with it.
    held.put(task_record("t1", 5, &["b", "c"], true), at(20 * GRACE)).unwrap();
    held.sweep(at(20 * GRACE + RETENTION));
    assert!(held.blob(&key).is_none());

    // An owner never seen here but found finished elsewhere lets go a grace
    // after the answer.
    let mut held = store();
    held.put_blob(&key, owned_by("t3", chunk), at(0)).unwrap();
    held.owner_checked(&task("t3"), OwnerState::Finished, at(GRACE));
    assert_eq!(held.next_due(), Some(at(2 * GRACE)));
    held.sweep(at(2 * GRACE - 1));
    assert!(held.blob(&key).is_some());
    held.sweep(at(2 * GRACE));
    assert!(held.blob(&key).is_none());

    // A copy handed off by a draining worker names that worker's old holders:
    // it is no move.
    let mut held = store();
    held.put_blob(&key, owned_by("t6", chunk), at(0)).unwrap();
    held.put_from(task_record("t6", 1, &["a", "b"], false), Origin::Leader, at(1)).unwrap();
    held.put_from(task_record("t6", 2, &["a", "d"], false), Origin::HandOff, at(2)).unwrap();
    assert!(held.take_blob_moves().is_empty());

    // An answer that the owner was gone, from a look-up begun before its
    // record arrived, does not drop the blobs of the record now held.
    let mut held = store();
    held.put_blob(&key, owned_by("t5", chunk), at(0)).unwrap();
    assert_eq!(held.owners_to_check(at(GRACE)), vec![task("t5")]);
    held.put(task_record("t5", 1, &["a", "b"], false), at(GRACE)).unwrap();
    held.owner_checked(&task("t5"), OwnerState::Gone, at(GRACE));
    assert!(held.blob(&key).is_some());

    // With no grace, nothing is asked about; forgetting an owner drops what
    // only it owned.
    let mut held = VersionedRecords::with_retention(None);
    held.put_blob(&key, owned_by("t4", chunk), at(0)).unwrap();
    assert!(held.owners_to_check(at(u64::MAX / 2)).is_empty());
    held.forget_blobs_of(&task("t4"));
    assert!(held.blob(&key).is_none());
}
