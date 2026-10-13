//! The shard as a reader of composites: a composite's records and the blobs
//! they name, read over the network the way a Task record is.

use std::sync::Arc;

use kabudachi_core::blob::{BlobKey, BlobOwner};
use kabudachi_core::manifest::{Manifest, Unresolved as UnjoinedPayload};
use kabudachi_core::protocol::generated::{CompositePage, CompositeRecord};
use kabudachi_core::protocol::ids::CompositeId;
use kabudachi_core::scheduler::composite::{CompositeSource, Unresolved};

use crate::messenger::Net;

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
