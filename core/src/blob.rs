//! Blobs: bytes stored under the BLAKE3 digest of their content, for an
//! owner (a task or a composite) whose lifetime bounds theirs. Two owners of
//! the same bytes share one blob.

use std::fmt;

use crate::manifest::BLOB_CHUNK_BYTES;
use crate::protocol::digest::{BLAKE3_DIGEST_BYTES, Digest, DigestAlgorithm};
use crate::protocol::generated::{self, blob_record};
use crate::protocol::ids::{CompositeId, TaskId, WorkerId};

/// The largest blob a holder stores: one chunk of a payload.
pub const MAX_BLOB_BYTES: usize = BLOB_CHUNK_BYTES;

/// What every blob's record key starts with, so it never meets a task's
/// record key (a task id has no `/`).
const RECORD_KEY_PREFIX: &[u8] = b"blob/";

/// A blob's name: the BLAKE3 digest of its bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobKey([u8; BLAKE3_DIGEST_BYTES]);

impl BlobKey {
    /// The key of `bytes`.
    pub fn of(bytes: &[u8]) -> BlobKey {
        BlobKey(*blake3::hash(bytes).as_bytes())
    }

    pub fn digest(&self) -> Digest {
        Digest::new(DigestAlgorithm::Blake3, self.0.to_vec()).expect("a BLAKE3 digest's length")
    }

    /// Whether `bytes` are the blob this key names.
    pub fn holds(&self, bytes: &[u8]) -> bool {
        BlobKey::of(bytes) == *self
    }

    /// The key the shard's record store keeps the blob under:
    /// `blob/<the digest in lowercase hex>`.
    pub fn record_key(&self) -> Vec<u8> {
        let mut key = RECORD_KEY_PREFIX.to_vec();
        key.extend_from_slice(blake3::Hash::from_bytes(self.0).to_hex().as_bytes());
        key
    }

    /// The blob a record key names; `None` for any other key.
    pub fn from_record_key(key: &[u8]) -> Option<BlobKey> {
        let hex = key.strip_prefix(RECORD_KEY_PREFIX)?;
        let hash = blake3::Hash::from_hex(hex).ok()?;
        Some(BlobKey(*hash.as_bytes()))
    }
}

impl From<&Digest> for BlobKey {
    fn from(digest: &Digest) -> Self {
        // Every algorithm a `Digest` can name is listed here, so a new one
        // must say how it names a blob.
        match digest.algorithm() {
            DigestAlgorithm::Blake3 => {
                BlobKey(digest.value().try_into().expect("a checked BLAKE3 digest has 32 bytes"))
            }
        }
    }
}

impl fmt::Display for BlobKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", blake3::Hash::from_bytes(self.0).to_hex())
    }
}

/// Who a blob is kept for.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BlobOwner {
    Task(TaskId),
    Composite(CompositeId),
}

/// A blob as its writer sends it to a holder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRecord {
    pub value: Vec<u8>,
    pub owner: BlobOwner,
    /// The holders its writer stored it with.
    pub placement: Vec<WorkerId>,
    /// Stored as a run's result rather than as an input.
    pub result: bool,
}

/// A blob record arrived naming no owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a blob record names no owner")]
pub struct OwnerMissing;

impl From<BlobRecord> for generated::BlobRecord {
    fn from(record: BlobRecord) -> Self {
        let owner = match record.owner {
            BlobOwner::Task(task) => blob_record::Owner::Task(task.into()),
            BlobOwner::Composite(composite) => blob_record::Owner::Composite(composite.into()),
        };
        generated::BlobRecord {
            value: record.value,
            owner: Some(owner),
            placement: record.placement.into_iter().map(Into::into).collect(),
            result: record.result,
        }
    }
}

impl TryFrom<generated::BlobRecord> for BlobRecord {
    type Error = OwnerMissing;

    fn try_from(wire: generated::BlobRecord) -> Result<Self, OwnerMissing> {
        let owner = match wire.owner.ok_or(OwnerMissing)? {
            blob_record::Owner::Task(task) => BlobOwner::Task(task.into()),
            blob_record::Owner::Composite(composite) => BlobOwner::Composite(composite.into()),
        };
        Ok(BlobRecord {
            value: wire.value,
            owner,
            placement: wire.placement.into_iter().map(WorkerId::from).collect(),
            result: wire.result,
        })
    }
}
