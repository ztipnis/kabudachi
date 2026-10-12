//! Payloads too large for one message, over the shard's blobs: stored as a
//! manifest of chunks owned by a task, and put back together, checked, by
//! whoever needs them.

use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::blob::{BlobKey, BlobOwner};
use kabudachi_core::manifest::{Chunked, Manifest, Unresolved};
use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::task_record::Placement;
use libp2p::futures::{StreamExt, stream};

use crate::messenger::{Net, PutBlobError};

/// How many blobs one payload stores or fetches at once.
const BLOBS_IN_FLIGHT: usize = 16;

impl Net {
    /// Cuts `payload` into blobs owned by the task `owner`, stores each at a
    /// quorum of `placement` (see [`Net::put_blob`]), and returns the manifest
    /// naming them. Fails if any blob was not stored.
    pub async fn store_payload(
        &self,
        owner: &TaskId,
        payload: &[u8],
        placement: Placement,
    ) -> Result<Manifest, PutBlobError> {
        let Chunked { manifest, chunks } = Manifest::chunk(payload);
        let mut puts = stream::iter(chunks.into_iter().map(|chunk| {
            self.put_blob(BlobOwner::Task(owner.clone()), chunk, placement.clone())
        }))
        .buffer_unordered(BLOBS_IN_FLIGHT);
        while let Some(put) = puts.next().await {
            put?;
        }
        Ok(manifest)
    }

    /// Every blob `manifest` names, owned by `owner`, that some holder could
    /// give, checked.
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
    pub async fn resolve_manifest(
        &self,
        owner: &BlobOwner,
        manifest: &Manifest,
    ) -> Result<Vec<u8>, Unresolved> {
        let fetched = self.fetch_blobs(owner, manifest).await;
        manifest.join(|key| fetched.get(key).cloned())
    }
}
