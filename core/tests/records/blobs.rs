//! Payloads too large for one message: cut into content-addressed chunks
//! named by an ordered manifest, and joined back only whole and unaltered.

use std::collections::BTreeMap;

use kabudachi_core::blob::{BlobKey, BlobOwner, BlobRecord};
use kabudachi_core::manifest::{
    BLOB_CHUNK_BYTES, Failure, INLINE_PAYLOAD_BYTES, Manifest, ManifestEntry, Unresolved, fits_inline,
};
use kabudachi_core::protocol::digest::{Digest, DigestAlgorithm};
use kabudachi_core::protocol::generated::{self, manifest_entry};
use kabudachi_core::protocol::ids::TaskId;

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
    };
    assert_eq!(BlobRecord::try_from(generated::BlobRecord::from(record.clone())), Ok(record));
}
