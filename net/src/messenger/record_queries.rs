//! The swarm task's `kad` queries of the shard's Task records and blobs:
//! writes to a placement's quorum, a draining worker's hand-offs, and
//! lookups. Each query's state lives here from the command that starts it to
//! the event that ends it.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::str::FromStr;

use kabudachi_core::blob::{BlobKey, BlobRecord};
use kabudachi_core::protocol::generated::{self, TaskRecord};
use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::task_record::{
    PlacedWrite, Placement, RecordVersion, VersionOrder, Write, WriteOutcome, identify,
};
use libp2p::kad::store::RecordStore as _;
use libp2p::{PeerId, Swarm, kad};
use prost::Message as _;
use tokio::sync::oneshot;

use super::{HandOffWrite, Inbound, PutBlobError};
use crate::swarm::Behaviour;
use crate::task_store::record_key;

/// The record queries in flight, each keyed by what the swarm reports its
/// outcome under.
#[derive(Default)]
pub(super) struct RecordQueries {
    /// The `kad` queries of record writes in flight to other workers, each
    /// to the write it is part of.
    writes: HashMap<kad::QueryId, u64>,
    /// The record writes with a query still in flight, by the number each is
    /// known by here.
    unfinished: HashMap<u64, UnfinishedWrite>,
    /// The number the next record write is known by.
    next_write: u64,
    /// Hand-offs in flight to other workers, by their `kad` query, each to
    /// say whether it reached its quorum.
    hand_offs: HashMap<kad::QueryId, oneshot::Sender<bool>>,
    /// Record lookups in flight, by their `kad` query.
    reads: HashMap<kad::QueryId, Read>,
    /// Blob writes in flight to other workers, by their `kad` query, each to
    /// say whether it reached its quorum.
    blob_puts: HashMap<kad::QueryId, oneshot::Sender<Result<(), PutBlobError>>>,
    /// Blob lookups in flight, by their `kad` query.
    blob_reads: HashMap<kad::QueryId, BlobRead>,
}

impl RecordQueries {
    /// Starts each write (see [`Self::start_write`]).
    pub(super) fn start_writes(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        writes: Vec<PlacedWrite>,
        inbound: &Inbound,
    ) {
        for placed in writes {
            self.start_write(swarm, placed, inbound);
        }
    }

    /// Starts one record write (see `Net::write_records`): puts the copy of
    /// a holder that is this worker into its own store, and writes the
    /// others' over the records protocol, to each placement the write must
    /// reach at the quorum of it still missing. A holder of two placements
    /// is written twice, once for each quorum that counts it. The write is
    /// stored once every placement's quorum has stored it, and refused as
    /// soon as one cannot.
    fn start_write(&mut self, swarm: &mut Swarm<Behaviour>, placed: PlacedWrite, inbound: &Inbound) {
        let write = Write::of(&placed.record);
        let local = *swarm.local_peer_id();
        let Some(records) = swarm.behaviour_mut().records.as_mut() else {
            inbound.queue_write(WriteOutcome { write, stored: false });
            return;
        };
        let value = placed.record.encode_to_vec();
        let key = record_key(&write.task_id);
        // This worker's own copy first, so its acknowledgement counts for
        // each placement it belongs to.
        let goes_here = placed
            .recipients()
            .iter()
            .any(|holder| PeerId::from_str(holder.as_str()).is_ok_and(|peer| peer == local));
        let stored_here = goes_here
            && records
                .store_mut()
                .put(kad::Record::new(key.clone(), value.clone()))
                .is_ok();
        let Some(queries) = remote_quorums(&placed, local, stored_here) else {
            inbound.queue_write(WriteOutcome { write, stored: false });
            return;
        };
        if queries.is_empty() {
            inbound.queue_write(WriteOutcome { write, stored: true });
            return;
        }
        let number = self.next_write;
        self.next_write += 1;
        self.unfinished.insert(
            number,
            UnfinishedWrite {
                write,
                outstanding: queries.len(),
            },
        );
        for (remote, needed) in queries {
            let quorum = kad::Quorum::N(NonZeroUsize::new(needed).expect("needed is not zero here"));
            let query = records.put_record_to(
                kad::Record::new(key.clone(), value.clone()),
                remote.into_iter(),
                quorum,
            );
            self.writes.insert(query, number);
        }
    }

    /// Starts one hand-off (see `Net::hand_off`): the copy goes to each holder
    /// but this worker, under this worker's name as its publisher, at the
    /// quorum the hand-off asks.
    pub(super) fn start_hand_off(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        write: HandOffWrite,
        respond_to: oneshot::Sender<bool>,
    ) {
        let local = *swarm.local_peer_id();
        let Some(records) = swarm.behaviour_mut().records.as_mut() else {
            let _ = respond_to.send(false);
            return;
        };
        let Ok((task, _)) = identify(&write.record) else {
            let _ = respond_to.send(false);
            return;
        };
        let remote: Vec<PeerId> = write
            .holders
            .iter()
            .filter_map(|holder| PeerId::from_str(holder.as_str()).ok())
            .filter(|peer| *peer != local)
            .collect();
        let Some(needed) = NonZeroUsize::new(write.quorum.max(1)).filter(|needed| needed.get() <= remote.len()) else {
            let _ = respond_to.send(false);
            return;
        };
        let mut record = kad::Record::new(record_key(&task), write.record.encode_to_vec());
        record.publisher = Some(local);
        let query = records.put_record_to(record, remote.into_iter(), kad::Quorum::N(needed));
        self.hand_offs.insert(query, respond_to);
    }

    /// Starts a lookup of `task`'s record, answered with the newest revision
    /// any holder returns; at once with none when this `Net` stores no
    /// shard's records.
    pub(super) fn start_read(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        task: TaskId,
        respond_to: oneshot::Sender<Option<TaskRecord>>,
    ) {
        match swarm.behaviour_mut().records.as_mut() {
            Some(records) => {
                let query = records.get_record(record_key(&task));
                self.reads.insert(
                    query,
                    Read {
                        best: None,
                        respond_to,
                    },
                );
            }
            None => {
                let _ = respond_to.send(None);
            }
        }
    }

    /// Starts one blob write (see `Net::put_blob`): this worker's own copy
    /// first, then the other holders over the records protocol, at the quorum
    /// still missing. A store with no room for this worker's own copy ends it
    /// at once, with nothing sent.
    pub(super) fn start_blob_put(
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
    pub(super) fn start_blob_lookup(
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

    /// Takes one event of the records `kad`: the progress of a write, a
    /// hand-off or a lookup this started. Any other event is ignored. Says
    /// which query, if any, has done its work and should be finished.
    pub(super) fn on_event(&mut self, event: kad::Event, inbound: &Inbound) -> Option<kad::QueryId> {
        match event {
            kad::Event::OutboundQueryProgressed {
                id,
                result: kad::QueryResult::PutRecord(result),
                ..
            } => {
                self.on_put(id, &result, inbound);
                None
            }
            kad::Event::OutboundQueryProgressed {
                id,
                result: kad::QueryResult::GetRecord(result),
                step,
                ..
            } => self.on_get(id, &result, &step),
            _ => None,
        }
    }

    fn on_put(&mut self, id: kad::QueryId, result: &kad::PutRecordResult, inbound: &Inbound) {
        if let Some(number) = self.writes.remove(&id) {
            self.write_answered(number, result, inbound);
        } else if let Some(respond_to) = self.hand_offs.remove(&id) {
            if let Err(error) = result {
                tracing::debug!(%error, "a record hand-off did not reach its quorum");
            }
            let _ = respond_to.send(result.is_ok());
        } else if let Some(respond_to) = self.blob_puts.remove(&id) {
            if let Err(error) = result {
                tracing::debug!(%error, "a blob write did not reach its quorum");
            }
            let _ = respond_to.send(result.as_ref().map(|_| ()).map_err(|_| PutBlobError::NotStored));
        }
    }

    /// One query of write `number` answered. A write already refused has no
    /// entry left: its other queries end unheard.
    fn write_answered(&mut self, number: u64, result: &kad::PutRecordResult, inbound: &Inbound) {
        let Some(mut unfinished) = self.unfinished.remove(&number) else {
            return;
        };
        unfinished.outstanding -= 1;
        match result {
            Err(error) => {
                tracing::debug!(
                    task = unfinished.write.task_id.as_str(),
                    %error,
                    "a record write did not reach its quorum"
                );
                inbound.queue_write(WriteOutcome {
                    write: unfinished.write,
                    stored: false,
                });
            }
            Ok(_) if unfinished.outstanding == 0 => {
                inbound.queue_write(WriteOutcome {
                    write: unfinished.write,
                    stored: true,
                });
            }
            Ok(_) => {
                self.unfinished.insert(number, unfinished);
            }
        }
    }

    /// One step of a lookup. A blob lookup that found a checked copy is
    /// answered at once, and its query returned for the caller to finish: one
    /// copy is enough, and the rest of the lookup is wasted.
    fn on_get(
        &mut self,
        id: kad::QueryId,
        result: &kad::GetRecordResult,
        step: &kad::ProgressStep,
    ) -> Option<kad::QueryId> {
        if let Some(read) = self.reads.get_mut(&id)
            && let Ok(kad::GetRecordOk::FoundRecord(found)) = result
        {
            read.offer(&found.record.value);
        }
        let found_blob = self.answer_blob_lookup(id, result);
        // The lookup's last step, whether it found records or not.
        if step.last
            && let Some(read) = self.reads.remove(&id)
        {
            read.finish();
        }
        if step.last
            && let Some(read) = self.blob_reads.remove(&id)
        {
            let _ = read.respond_to.send(None);
        }
        found_blob.then_some(id)
    }

    /// Answers the blob lookup `id` if `result` carries the bytes its key
    /// names; whether it did.
    fn answer_blob_lookup(&mut self, id: kad::QueryId, result: &kad::GetRecordResult) -> bool {
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

/// What each placement of `placed` still needs from other workers: its
/// holders other than this one, and how many of them must store the write.
/// A placement this worker's own copy already satisfies needs nothing.
/// `None` when a placement has too few other holders to reach its quorum,
/// which refuses the write.
fn remote_quorums(
    placed: &PlacedWrite,
    local: PeerId,
    stored_here: bool,
) -> Option<Vec<(Vec<PeerId>, usize)>> {
    let placements = std::iter::once((placed.holders(), placed.quorum))
        .chain(placed.prior.iter().map(|prior| (prior.holders.clone(), prior.quorum)));
    let mut queries = Vec::new();
    for (holders, quorum) in placements {
        let (remote, acked) = split_holders(&holders, local, stored_here);
        let needed = quorum.saturating_sub(acked);
        if needed == 0 {
            continue;
        }
        if remote.len() < needed {
            return None;
        }
        queries.push((remote, needed));
    }
    Some(queries)
}

/// `holders` other than this worker, as peers, and the acknowledgements this
/// worker's own copy gives them: one if it is a holder and stored it.
fn split_holders(holders: &[WorkerId], local: PeerId, stored_here: bool) -> (Vec<PeerId>, usize) {
    let mut acked = 0;
    let mut remote = Vec::new();
    for holder in holders {
        let Ok(peer) = PeerId::from_str(holder.as_str()) else {
            continue;
        };
        if peer == local {
            acked += usize::from(stored_here);
        } else {
            remote.push(peer);
        }
    }
    (remote, acked)
}

/// A record write that waits for the queries it started: one for each
/// placement it must reach, each to a quorum of that placement.
struct UnfinishedWrite {
    write: Write,
    /// The queries not answered yet.
    outstanding: usize,
}

/// A blob lookup waiting for a holder's answer.
struct BlobRead {
    key: BlobKey,
    respond_to: oneshot::Sender<Option<Vec<u8>>>,
}

/// The bytes `value`, an encoded blob record, holds, if they are the ones
/// `key` names: a peer's answer is checked, not trusted.
fn blob_bytes(key: &BlobKey, value: &[u8]) -> Option<Vec<u8>> {
    let record = generated::BlobRecord::decode(value).ok()?;
    key.holds(&record.value).then_some(record.value)
}

/// A record lookup collecting the answers of its `kad` query.
struct Read {
    /// The newest answer so far, with its version.
    best: Option<(RecordVersion, TaskRecord)>,
    respond_to: oneshot::Sender<Option<TaskRecord>>,
}

impl Read {
    /// Keeps `value` if it decodes to a record newer than the best so far.
    fn offer(&mut self, value: &[u8]) {
        let Ok(record) = TaskRecord::decode(value) else {
            return;
        };
        let Ok((_, version)) = identify(&record) else {
            return;
        };
        let newer = self
            .best
            .as_ref()
            .is_none_or(|(best, _)| best.order(&version) == VersionOrder::Newer);
        if newer {
            self.best = Some((version, record));
        }
    }

    /// Ends the lookup with the best answer, to a caller who may have
    /// stopped waiting.
    fn finish(self) {
        let _ = self.respond_to.send(self.best.map(|(_, record)| record));
    }
}
