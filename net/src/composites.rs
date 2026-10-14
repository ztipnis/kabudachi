//! The shard as a reader of composites: a composite's records and the blobs
//! they name, read over the network the way a Task record is.

use std::sync::{Arc, Mutex, PoisonError};

use kabudachi_core::blob::{BlobKey, BlobOwner};
use kabudachi_core::manifest::{Manifest, Unresolved as UnjoinedPayload};
use kabudachi_core::protocol::generated::{CompositePage, CompositeRecord};
use kabudachi_core::protocol::ids::CompositeId;
use kabudachi_core::scheduler::composite::{CompositeSource, Unresolved};

use crate::messenger::Net;
use crate::payload_memory::Taken;

/// A shard's [`Net`] as the [`CompositeSource`] a member's worker or a
/// composite's client reads from: records by the shard's record look-up, blobs
/// from the holders of their owner's record.
pub struct NetSource {
    net: Arc<Net>,
}

impl NetSource {
    pub fn new(net: Arc<Net>) -> Self {
        NetSource { net }
    }
}

impl CompositeSource for NetSource {
    async fn composite(&self, id: &CompositeId) -> Option<CompositeRecord> {
        self.net.composite_record(id).await
    }

    async fn page(&self, id: &CompositeId, page: u32) -> Option<CompositePage> {
        self.net.composite_page(id, page).await
    }

    async fn blob(&self, owner: &BlobOwner, key: &BlobKey) -> Option<Vec<u8>> {
        self.net.get_blob(owner.clone(), &key.digest()).await
    }

    async fn payload(&self, owner: &BlobOwner, manifest: &Manifest) -> Result<Vec<u8>, Unresolved> {
        self.net.resolve_manifest(owner, manifest).await.map_err(|unresolved| match unresolved {
            UnjoinedPayload::Missing(key) | UnjoinedPayload::Corrupt(key) => Unresolved::MissingBlob(key),
            UnjoinedPayload::NotAPayload | UnjoinedPayload::WrongLength { .. } | UnjoinedPayload::Malformed => {
                Unresolved::Malformed
            }
        })
    }
}

/// The shard as the source a member's worker reads its input from, counting
/// each payload it joins in this process's payload memory before it is fetched:
/// counted, not waited for, since the worker that asks is the one holding the
/// room others wait for. A payload alone past the hard limit is refused.
/// [`Self::begin`] starts an attempt, dropping what an earlier one counted;
/// [`Self::counted`] hands over what the last one did, to be held until the
/// member's run ends.
pub struct CountingSource {
    inner: NetSource,
    net: Arc<Net>,
    counted: Mutex<Vec<Taken>>,
}

impl CountingSource {
    pub fn new(net: Arc<Net>) -> Self {
        CountingSource { inner: NetSource::new(Arc::clone(&net)), net, counted: Mutex::new(Vec::new()) }
    }

    pub fn begin(&self) {
        self.counted.lock().unwrap_or_else(PoisonError::into_inner).clear();
    }

    pub fn counted(&self) -> Vec<Taken> {
        std::mem::take(&mut *self.counted.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl CompositeSource for CountingSource {
    async fn composite(&self, id: &CompositeId) -> Option<CompositeRecord> {
        self.inner.composite(id).await
    }

    async fn page(&self, id: &CompositeId, page: u32) -> Option<CompositePage> {
        self.inner.page(id, page).await
    }

    async fn blob(&self, owner: &BlobOwner, key: &BlobKey) -> Option<Vec<u8>> {
        self.inner.blob(owner, key).await
    }

    async fn payload(&self, owner: &BlobOwner, manifest: &Manifest) -> Result<Vec<u8>, Unresolved> {
        let taken = self
            .net
            .payload_memory()
            .take_anyway(manifest.total_bytes())
            .map_err(|too_large| Unresolved::TooLarge { size: too_large.size, limit: too_large.limit })?;
        let payload = self.inner.payload(owner, manifest).await?;
        self.counted.lock().unwrap_or_else(PoisonError::into_inner).push(taken);
        Ok(payload)
    }
}
