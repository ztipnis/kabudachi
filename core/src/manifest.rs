//! Payloads too large for one message, and the ordered lists that stand in
//! for them. A payload past [`INLINE_PAYLOAD_BYTES`] is cut into blobs of
//! [`BLOB_CHUNK_BYTES`]; its manifest names them in order, with the total
//! length, and travels where the bytes would have. A manifest also stands
//! in for a list of outcomes, whose entries may be failures or pages of
//! further entries; such a manifest is not one payload.

use prost::Message as _;

use crate::blob::BlobKey;
use crate::protocol::digest::{Digest, InvalidDigest};
use crate::protocol::generated::{self, manifest_entry};
use crate::protocol::ids::CompositeId;
use crate::scheduler::MAX_SUBMISSION_BYTES;

/// How large each blob of a cut payload is; the last may be shorter.
pub const BLOB_CHUNK_BYTES: usize = 256 * 1024;

/// What a message keeps beside an inline payload: its queue, definition id,
/// coalescing key and framing.
const INLINE_HEADROOM_BYTES: u64 = 64 * 1024;

/// The largest payload that travels inline, in the message itself: one
/// message less its headroom. A larger one travels as blobs.
pub const INLINE_PAYLOAD_BYTES: u64 = MAX_SUBMISSION_BYTES - INLINE_HEADROOM_BYTES;

/// The failure kind of a run whose input's blobs could not all be fetched
/// and checked: its body never started.
pub const PAYLOAD_UNAVAILABLE_KIND: &str = "PayloadUnavailable";

/// Whether a payload of `len` bytes travels inline.
pub fn fits_inline(len: usize) -> bool {
    len as u64 <= INLINE_PAYLOAD_BYTES
}

/// What a task's input weighs: its whole length, inline or as blobs.
pub fn payload_bytes(task: &generated::Task) -> u64 {
    task.input_manifest
        .as_ref()
        .map_or(task.serialized_input.len() as u64, |manifest| manifest.total_bytes)
}

/// The digest of a task's input: of the input itself, or, for one carried
/// as blobs, of its manifest, which commits to every blob and the length.
pub fn input_digest(task: &generated::Task) -> Digest {
    match &task.input_manifest {
        Some(manifest) => Digest::blake3(&manifest.encode_to_vec()),
        None => Digest::blake3(&task.serialized_input),
    }
}

/// An error a body raised, as a list of outcomes carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// The error's type name.
    pub kind: String,
    pub message: String,
}

/// One part of a manifest: a blob of a cut payload, or one outcome of a list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestEntry {
    Blob(BlobKey),
    Failure(Failure),
    Inline(Vec<u8>),
    /// A page of a composite's outcomes, itself a list of entries.
    Page { composite: CompositeId, page: u32 },
}

/// A payload cut into blobs, or a list of outcomes, and the bytes it stands
/// for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    entries: Vec<ManifestEntry>,
    total_bytes: u64,
}

/// A payload cut into blobs: the manifest naming them, and their bytes in
/// the same order.
pub struct Chunked {
    pub manifest: Manifest,
    pub chunks: Vec<Vec<u8>>,
}

/// Why a manifest's payload could not be put back together.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unresolved {
    #[error("blob {0} could not be found")]
    Missing(BlobKey),
    #[error("blob {0} does not hold the bytes its digest names")]
    Corrupt(BlobKey),
    #[error("the manifest lists outcomes, not one payload's bytes")]
    NotAPayload,
    #[error("the joined payload is {actual} bytes, not the {expected} its manifest names")]
    WrongLength { expected: u64, actual: u64 },
    #[error("the manifest could not be read")]
    Malformed,
}

/// Why a manifest that arrived could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidManifest {
    #[error("a manifest entry is empty")]
    EmptyEntry,
    #[error("a manifest entry names an unusable digest: {0}")]
    Digest(#[from] InvalidDigest),
    #[error("a manifest page names no composite")]
    PageWithoutComposite,
    #[error("a manifest of {blobs} blobs names {total_bytes} bytes, which they cannot hold")]
    TotalBytesOutOfRange { blobs: usize, total_bytes: u64 },
}

impl Manifest {
    /// A manifest of `entries`, standing for `total_bytes`.
    pub fn new(entries: Vec<ManifestEntry>, total_bytes: u64) -> Manifest {
        Manifest { entries, total_bytes }
    }

    /// `payload` cut into blobs of [`BLOB_CHUNK_BYTES`], in order.
    pub fn chunk(payload: &[u8]) -> Chunked {
        let chunks: Vec<Vec<u8>> = payload.chunks(BLOB_CHUNK_BYTES).map(<[u8]>::to_vec).collect();
        let entries = chunks.iter().map(|chunk| ManifestEntry::Blob(BlobKey::of(chunk))).collect();
        Chunked {
            manifest: Manifest {
                entries,
                total_bytes: payload.len() as u64,
            },
            chunks,
        }
    }

    pub fn entries(&self) -> &[ManifestEntry] {
        &self.entries
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Whether this manifest, known to stand for one cut payload, is
    /// well-formed as one. Cut blobs are all full but the last, so a
    /// manifest of only blobs pins its length to within one chunk. A manifest
    /// that is a list of outcomes has no such bound and is not checked here.
    pub fn check_cut_payload(&self) -> Result<(), InvalidManifest> {
        let blobs = self.entries.len() as u64;
        let only_blobs = self.entries.iter().all(|entry| matches!(entry, ManifestEntry::Blob(_)));
        let chunk = BLOB_CHUNK_BYTES as u64;
        if only_blobs
            && blobs > 0
            && !(((blobs - 1) * chunk + 1)..=blobs * chunk).contains(&self.total_bytes)
        {
            return Err(InvalidManifest::TotalBytesOutOfRange {
                blobs: self.entries.len(),
                total_bytes: self.total_bytes,
            });
        }
        Ok(())
    }

    /// The blobs the entries name, in order.
    pub fn blob_keys(&self) -> impl Iterator<Item = &BlobKey> {
        self.entries.iter().filter_map(|entry| match entry {
            ManifestEntry::Blob(key) => Some(key),
            _ => None,
        })
    }

    /// The BLAKE3 digest of the manifest's encoding: the same for every
    /// worker, since the encoding of a message with no maps is fixed.
    pub fn digest(&self) -> Digest {
        Digest::blake3(&generated::Manifest::from(self.clone()).encode_to_vec())
    }

    /// The payload, joined from its entries in order. `blob` looks each blob
    /// up; every blob is checked against its key, and the whole against the
    /// length the manifest names.
    pub fn join(&self, mut blob: impl FnMut(&BlobKey) -> Option<Vec<u8>>) -> Result<Vec<u8>, Unresolved> {
        let mut joined = Vec::new();
        for entry in &self.entries {
            match entry {
                ManifestEntry::Blob(key) => {
                    let bytes = blob(key).ok_or_else(|| Unresolved::Missing(key.clone()))?;
                    if !key.holds(&bytes) {
                        return Err(Unresolved::Corrupt(key.clone()));
                    }
                    joined.extend_from_slice(&bytes);
                }
                ManifestEntry::Inline(bytes) => joined.extend_from_slice(bytes),
                ManifestEntry::Failure(_) | ManifestEntry::Page { .. } => return Err(Unresolved::NotAPayload),
            }
        }
        if joined.len() as u64 != self.total_bytes {
            return Err(Unresolved::WrongLength {
                expected: self.total_bytes,
                actual: joined.len() as u64,
            });
        }
        Ok(joined)
    }
}

impl From<ManifestEntry> for generated::ManifestEntry {
    fn from(entry: ManifestEntry) -> Self {
        let entry = match entry {
            ManifestEntry::Blob(key) => manifest_entry::Entry::Blob(key.digest().into()),
            ManifestEntry::Failure(Failure { kind, message }) => {
                manifest_entry::Entry::Failure(generated::Failure { kind, message })
            }
            ManifestEntry::Inline(bytes) => manifest_entry::Entry::Inline(bytes),
            ManifestEntry::Page { composite, page } => manifest_entry::Entry::Page(generated::PageRef {
                composite_id: Some(composite.into()),
                page,
            }),
        };
        generated::ManifestEntry { entry: Some(entry) }
    }
}

impl From<Manifest> for generated::Manifest {
    fn from(manifest: Manifest) -> Self {
        generated::Manifest {
            entries: manifest.entries.into_iter().map(Into::into).collect(),
            total_bytes: manifest.total_bytes,
        }
    }
}

impl TryFrom<&generated::ManifestEntry> for ManifestEntry {
    type Error = InvalidManifest;

    fn try_from(wire: &generated::ManifestEntry) -> Result<Self, InvalidManifest> {
        match wire.entry.as_ref() {
            None => Err(InvalidManifest::EmptyEntry),
            Some(manifest_entry::Entry::Blob(digest)) => {
                Ok(ManifestEntry::Blob(BlobKey::from(&Digest::try_from(digest)?)))
            }
            Some(manifest_entry::Entry::Failure(failure)) => Ok(ManifestEntry::Failure(Failure {
                kind: failure.kind.clone(),
                message: failure.message.clone(),
            })),
            Some(manifest_entry::Entry::Inline(bytes)) => Ok(ManifestEntry::Inline(bytes.clone())),
            Some(manifest_entry::Entry::Page(page)) => Ok(ManifestEntry::Page {
                composite: page
                    .composite_id
                    .clone()
                    .map(CompositeId::from)
                    .ok_or(InvalidManifest::PageWithoutComposite)?,
                page: page.page,
            }),
        }
    }
}

impl TryFrom<&generated::Manifest> for Manifest {
    type Error = InvalidManifest;

    fn try_from(wire: &generated::Manifest) -> Result<Self, InvalidManifest> {
        let entries: Vec<ManifestEntry> =
            wire.entries.iter().map(ManifestEntry::try_from).collect::<Result<_, _>>()?;
        Ok(Manifest {
            entries,
            total_bytes: wire.total_bytes,
        })
    }
}
