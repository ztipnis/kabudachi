//! Payloads too large for one message, over the shard's blobs: stored as a
//! manifest of chunks owned by a task or a composite, and put back together, checked, by
//! whoever needs them.

use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::blob::{BlobKey, BlobOwner};
use kabudachi_core::manifest::{Chunked, Manifest, Unresolved};
use kabudachi_core::task_record::Placement;
use libp2p::futures::{StreamExt, stream};

use crate::messenger::{Net, PutBlobError};

/// How many blobs one payload stores or fetches at once.
const BLOBS_IN_FLIGHT: usize = 16;

/// What a payload's blobs are stored as. A holder keeps part of its share of
/// memory for results, which inputs cannot fill, and drops a finished task's
/// input blobs at once while its result blobs stay as long as its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobRole {
    Input,
    Result,
}

impl Net {
    /// Cuts `payload` into blobs owned by `owner`, stores each in
    /// `role` at a quorum of `placement` (see [`Net::put_blob`]), and returns
    /// the manifest naming them. Fails if any blob was not stored.
    ///
    /// Known limit: the caller's `payload` and the chunk copies made here are
    /// alive together, so peak memory is about twice the payload, while the
    /// memory ledger counts the payload once.
    pub async fn store_payload(
        &self,
        role: BlobRole,
        owner: &BlobOwner,
        payload: &[u8],
        placement: Placement,
    ) -> Result<Manifest, PutBlobError> {
        let Chunked { manifest, chunks } = Manifest::chunk(payload);
        self.store_blobs(role, owner, chunks, placement).await?;
        Ok(manifest)
    }

    /// Stores each of `blobs`, owned by `owner`, in `role` at a quorum of
    /// `placement`. Fails if any was not stored.
    pub async fn store_blobs(
        &self,
        role: BlobRole,
        owner: &BlobOwner,
        blobs: Vec<Vec<u8>>,
        placement: Placement,
    ) -> Result<(), PutBlobError> {
        let mut puts = stream::iter(
            blobs
                .into_iter()
                .map(|blob| self.put_blob_in(role, owner.clone(), blob, placement.clone())),
        )
        .buffer_unordered(BLOBS_IN_FLIGHT);
        while let Some(put) = puts.next().await {
            put?;
        }
        Ok(())
    }

    /// Every blob `manifest` names, owned by `owner`, that some holder could
    /// give, checked. A manifest that arrived has its length checked against
    /// its blob count, and a holder stores no blob past a chunk, so this
    /// fetches no more than the length it names plus one chunk.
    pub async fn fetch_blobs(
        &self,
        owner: &BlobOwner,
        manifest: &Manifest,
    ) -> BTreeMap<BlobKey, Vec<u8>> {
        let keys: BTreeSet<BlobKey> = manifest.blob_keys().cloned().collect();
        stream::iter(keys.into_iter().map(|key| async move {
            let found = self.get_blob(owner.clone(), &key.digest()).await;
            found.map(|bytes| (key, bytes))
        }))
        .buffer_unordered(BLOBS_IN_FLIGHT)
        .filter_map(|found| async move { found })
        .collect()
        .await
    }

    /// The payload `manifest` stands in for, fetched and checked.
    ///
    /// Known limit: the fetched blobs and the joined payload are alive
    /// together, so peak memory is about twice the payload, while the memory
    /// ledger counts the payload once.
    pub async fn resolve_manifest(
        &self,
        owner: &BlobOwner,
        manifest: &Manifest,
    ) -> Result<Vec<u8>, Unresolved> {
        let fetched = self.fetch_blobs(owner, manifest).await;
        manifest.join(|key| fetched.get(key).cloned())
    }
}
