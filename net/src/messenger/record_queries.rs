//! The swarm task's `kad` queries of the shard's Task records and blobs:
//! writes to a placement's quorum, a draining worker's hand-offs, and
//! lookups. Each query's state lives here from the command that starts it to
//! the event that ends it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::str::FromStr;

use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::task_record::{
    PlacedWrite, RecordKey, RecordVersion, ShardRecord, VersionOrder, Write, WriteOutcome,
};
use libp2p::kad::store::RecordStore as _;
use libp2p::{PeerId, Swarm, kad};
use tokio::sync::oneshot;

use self::blobs::BlobRead;
use super::{HandOffWrite, Inbound, PutBlobError, RecordLookup};
use crate::swarm::Behaviour;

mod blobs;

// Leave half of kad's 32 streams per connection available for reads and
// replies. A burst of admitted work must wait here instead of flooding a
// holder and turning every write in the burst into a refusal.
const MAX_RECORD_QUERIES: usize = 16;

/// The record queries in flight, each keyed by what the swarm reports its
/// outcome under.
#[derive(Default)]
pub(super) struct RecordQueries {
    queued: VecDeque<(u64, kad::Record, Vec<PeerId>, usize)>,
    /// The `kad` queries of record writes in flight to other workers, each
    /// to the write it is part of.
    writes: HashMap<kad::QueryId, (u64, usize)>,
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
        self.advance_writes(swarm, inbound);
    }

    pub(super) fn advance_writes(&mut self, swarm: &mut Swarm<Behaviour>, inbound: &Inbound) {
        while self.writes.len() < MAX_RECORD_QUERIES {
            let Some((number, record, remote, needed)) = self.queued.pop_front() else { break; };
            if !self.unfinished.contains_key(&number) { continue; }
            let Some(records) = swarm.behaviour_mut().records.as_mut() else {
                self.write_answered(number, false, inbound);
                continue;
            };
            // Keep this budget until every targeted RPC settles. Kad's
            // quorum shortcut leaves requests to slower holders in flight;
            // releasing a slot then lets a burst exhaust their streams.
            let query = records.put_record_to(record, remote.into_iter(), kad::Quorum::All);
            self.writes.insert(query, (number, needed));
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
        let key = kad::RecordKey::new(&write.key.kad_bytes());
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
            self.queued.push_back((
                number,
                kad::Record::new(key.clone(), value.clone()),
                remote,
                needed,
            ));
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
        let Ok((key, _)) = write.record.identify() else {
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
        let mut record = kad::Record::new(kad::RecordKey::new(&key.kad_bytes()), write.record.encode_to_vec());
        record.publisher = Some(local);
        let query = records.put_record_to(record, remote.into_iter(), kad::Quorum::N(needed));
        self.hand_offs.insert(query, respond_to);
    }

    /// Starts a lookup of the record `key` names, answered with the newest
    /// revision any holder returns; at once unanswered when this `Net` stores
    /// no shard's records.
    pub(super) fn start_read(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        key: RecordKey,
        respond_to: oneshot::Sender<RecordLookup<ShardRecord>>,
    ) {
        match swarm.behaviour_mut().records.as_mut() {
            Some(records) => {
                let known = records
                    .kbuckets()
                    .flat_map(|bucket| bucket.iter().map(|entry| *entry.node.key.preimage()).collect::<Vec<_>>())
                    .collect();
                let query = records.get_record(kad::RecordKey::new(&key.kad_bytes()));
                self.reads.insert(
                    query,
                    Read {
                        key,
                        best: None,
                        known,
                        respond_to,
                    },
                );
            }
            None => {
                let _ = respond_to.send(RecordLookup::Unanswered);
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
        if let Some((number, needed)) = self.writes.remove(&id) {
            let stored = match result {
                Ok(_) => true,
                Err(kad::PutRecordError::QuorumFailed { success, .. }
                    | kad::PutRecordError::Timeout { success, .. }) => success.len() >= needed,
            };
            self.write_answered(number, stored, inbound);
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
    fn write_answered(&mut self, number: u64, stored: bool, inbound: &Inbound) {
        let Some(mut unfinished) = self.unfinished.remove(&number) else {
            return;
        };
        unfinished.outstanding -= 1;
        match stored {
            false => {
                tracing::debug!(
                    key = ?unfinished.write.key,
                    "a record write did not reach its quorum"
                );
                inbound.queue_write(WriteOutcome {
                    write: unfinished.write,
                    stored: false,
                });
            }
            true if unfinished.outstanding == 0 => {
                inbound.queue_write(WriteOutcome {
                    write: unfinished.write,
                    stored: true,
                });
            }
            true => {
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
            read.finish(result);
        }
        if step.last
            && let Some(read) = self.blob_reads.remove(&id)
        {
            let _ = read.respond_to.send(None);
        }
        found_blob.then_some(id)
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

/// A record lookup collecting the answers of its `kad` query.
struct Read {
    /// The record looked up.
    key: RecordKey,
    /// The newest answer so far, with its version.
    best: Option<(RecordVersion, ShardRecord)>,
    /// The peers in this worker's routing table when the lookup started.
    known: HashSet<PeerId>,
    respond_to: oneshot::Sender<RecordLookup<ShardRecord>>,
}

impl Read {
    /// Keeps `value` if it decodes to the record looked up, and is newer than
    /// the best so far.
    fn offer(&mut self, value: &[u8]) {
        let Ok(record) = ShardRecord::decode(&self.key, value) else {
            return;
        };
        let Ok((key, version)) = record.identify() else {
            return;
        };
        if key != self.key {
            return;
        }
        let newer = self
            .best
            .as_ref()
            .is_none_or(|(best, _)| best.order(&version) == VersionOrder::Newer);
        if newer {
            self.best = Some((version, record));
        }
    }

    /// Ends the lookup, whose last step came to `last`, with the best answer,
    /// to a caller who may have stopped waiting. A lookup that found nothing
    /// says the record is held nowhere only if it asked every peer this
    /// worker knew and each answered (every one of them is among the peers
    /// the lookup heard from, whatever else it heard from): a timeout, an
    /// empty routing table, a shard with more peers than one lookup asks and a
    /// peer that did not answer all leave it unanswered, for a holder may be
    /// among those not asked.
    fn finish(self, last: &kad::GetRecordResult) {
        let lookup = match self.best {
            Some((_, record)) => RecordLookup::Found(record),
            None => match last {
                Err(kad::GetRecordError::NotFound { closest_peers, .. })
                    if !self.known.is_empty()
                        && self.known.iter().all(|peer| closest_peers.contains(peer)) =>
                {
                    RecordLookup::NotFound
                }
                _ => RecordLookup::Unanswered,
            },
        };
        let _ = self.respond_to.send(lookup);
    }
}
