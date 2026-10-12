//! The blob half of the records queries: a blob write to the quorum of a
//! placement, and the kad lookup that is the last resort of a blob fetch.

use std::num::NonZeroUsize;

use kabudachi_core::blob::{BlobKey, BlobRecord};
use kabudachi_core::protocol::generated;
use kabudachi_core::task_record::Placement;
use libp2p::kad::store::RecordStore as _;
use libp2p::{Swarm, kad};
use prost::Message as _;
use tokio::sync::oneshot;

use super::{RecordQueries, split_holders};
use crate::messenger::PutBlobError;
use crate::swarm::Behaviour;

impl RecordQueries {
    /// Starts one blob write (see `Net::put_blob`): this worker's own copy
    /// first, then the other holders over the records protocol, at the quorum
    /// still missing. A store with no room for this worker's own copy ends it
    /// at once, with nothing sent.
    pub(in crate::messenger) fn start_blob_put(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        key: BlobKey,
        record: BlobRecord,
        placement: &Placement,
        respond_to: oneshot::Sender<Result<(), PutBlobError>>,
    ) {
        let local = *swarm.local_peer_id();
        let Some(records) = swarm.behaviour_mut().records.as_mut() else {
            let _ = respond_to.send(Err(PutBlobError::NotStored));
            return;
        };
        let record_key = kad::RecordKey::new(&key.record_key());
        let value = generated::BlobRecord::from(record).encode_to_vec();
        let stored_here = records.store_mut().put(kad::Record::new(record_key.clone(), value.clone()));
        if stored_here.is_err() {
            let _ = respond_to.send(Err(PutBlobError::StoreFull));
            return;
        }
        let (remote, acked) = split_holders(&placement.holders, local, true);
        let needed = placement.quorum.saturating_sub(acked);
        if needed == 0 {
            let _ = respond_to.send(Ok(()));
            return;
        }
        if remote.len() < needed {
            let _ = respond_to.send(Err(PutBlobError::NotStored));
            return;
        }
        let quorum = kad::Quorum::N(NonZeroUsize::new(needed).expect("needed is not zero here"));
        let query = records.put_record_to(kad::Record::new(record_key, value), remote.into_iter(), quorum);
        self.blob_puts.insert(query, respond_to);
    }

    /// Starts a kad lookup of the blob `key` names (see `Net::get_blob`'s
    /// last resort); at once with none when this `Net` stores no shard's
    /// records.
    pub(in crate::messenger) fn start_blob_lookup(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        key: BlobKey,
        respond_to: oneshot::Sender<Option<Vec<u8>>>,
    ) {
        match swarm.behaviour_mut().records.as_mut() {
            Some(records) => {
                let query = records.get_record(kad::RecordKey::new(&key.record_key()));
                self.blob_reads.insert(query, BlobRead { key, respond_to });
            }
            None => {
                let _ = respond_to.send(None);
            }
        }
    }

    /// Answers the blob lookup `id` if `result` carries the bytes its key
    /// names; whether it did.
    pub(super) fn answer_blob_lookup(&mut self, id: kad::QueryId, result: &kad::GetRecordResult) -> bool {
        let Ok(kad::GetRecordOk::FoundRecord(found)) = result else {
            return false;
        };
        let Some(bytes) = self.blob_reads.get(&id).and_then(|read| blob_bytes(&read.key, &found.record.value))
        else {
            return false;
        };
        if let Some(read) = self.blob_reads.remove(&id) {
            let _ = read.respond_to.send(Some(bytes));
        }
        true
    }
}

/// A blob lookup waiting for a holder's answer.
pub(super) struct BlobRead {
    pub(super) key: BlobKey,
    pub(super) respond_to: oneshot::Sender<Option<Vec<u8>>>,
}

/// The bytes `value`, an encoded blob record, holds, if they are the ones
/// `key` names: a peer's answer is checked, not trusted.
pub(super) fn blob_bytes(key: &BlobKey, value: &[u8]) -> Option<Vec<u8>> {
    let record = generated::BlobRecord::decode(value).ok()?;
    key.holds(&record.value).then_some(record.value)
}
