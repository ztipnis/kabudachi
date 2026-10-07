//! Where a shard keeps its Task records, and how its workers store them for
//! one another.
//!
//! Every worker of a shard runs a second `kad` behaviour (the swarm's
//! `records`), apart from the one that routes, on a protocol only the shard's
//! workers speak, so a record never lands in another shard. Its record store
//! is [`TaskRecordStore`], over the worker's [`HeldRecords`]: a put is
//! acknowledged only if the record was stored, because `kad` answers a put
//! the store refused by resetting the stream rather than acknowledging it. A
//! writer that counts acknowledgements therefore counts holders.

use std::borrow::Cow;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::reconcile::HeldKey;
use kabudachi_core::reconcile::wire::held_key;
use kabudachi_core::task_record::{MAX_RECORD_BYTES, Origin, VersionedRecords, identify, looks_claimable};
use kabudachi_core::time::{Clock, RealClock, WallTime};
use libp2p::PeerId;
use libp2p::kad;
use prost::Message as _;

pub mod placement;

/// The most bytes a records-protocol message may have: the largest record
/// plus room for the rest of a kad message. A `GET_VALUE` response carries,
/// besides the record (with its key, publisher and message type), up to the
/// bucket size of closer peers with every address each is known by, so the
/// headroom is sized for those.
pub const MAX_RECORD_PACKET_BYTES: usize = MAX_RECORD_BYTES as usize + 64 * 1024;

/// How long a record write waits for its quorum before it counts as
/// refused: shorter than a claimant waits for its answer, so a claimant
/// whose claim could not be recorded hears "not the leader" rather than
/// nothing.
pub const RECORD_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// The records this worker holds, shared by its kad record store (which
/// peers write into and read from) and the rest of the worker (which reads
/// it as its local view of the shard's tasks).
#[derive(Clone)]
pub struct HeldRecords {
    records: Arc<Mutex<VersionedRecords>>,
    clock: Arc<RealClock>,
}

impl HeldRecords {
    /// Records that drop a finished record `retention` after this worker
    /// first stored a finished revision of it, or keep finished records with
    /// `None`.
    pub fn new(retention: Option<kabudachi_core::time::Duration>) -> Self {
        HeldRecords {
            records: Arc::new(Mutex::new(VersionedRecords::with_retention(retention))),
            clock: Arc::new(RealClock::new()),
        }
    }

    /// Names this worker as the holder of the records: a revision the leader
    /// writes that leaves it out of the record's holders then leaves it a stub of the
    /// revision, not a copy.
    pub(crate) fn held_by(&self, holder: WorkerId) {
        let mut records = self.lock();
        *records = std::mem::take(&mut *records).held_by(holder);
    }

    /// The newest revision of `task`'s record this worker holds.
    pub fn get(&self, task: &TaskId) -> Option<TaskRecord> {
        self.lock().get(task).cloned()
    }

    /// Every task this worker holds a record of.
    pub fn task_ids(&self) -> Vec<TaskId> {
        self.lock()
            .iter()
            .filter_map(|record| identify(record).ok().map(|(task, _)| task))
            .collect()
    }

    /// Each task whose record, as this worker holds it, looks claimable at
    /// `now` (see `looks_claimable`), with when it was submitted, in task id
    /// order. A compaction run counts only for a worker that
    /// `runs_compaction`.
    pub(crate) fn claimable(
        &self,
        now: WallTime,
        runs_compaction: bool,
    ) -> Vec<(WallTime, TaskId)> {
        self.lock()
            .iter()
            .filter(|record| looks_claimable(record, now))
            .filter(|record| {
                runs_compaction || record.task.as_ref().is_some_and(|task| task.compacts.is_none())
            })
            .filter_map(|record| {
                let (task, _) = identify(record).ok()?;
                let submitted_at = record.task.as_ref()?.submitted_at?;
                Some((WallTime::from(submitted_at), task))
            })
            .collect()
    }

    /// Feeds `take` a summary of each record held after `after` (every one
    /// for `None`), and of each stub of a revision held elsewhere (see
    /// [`VersionedRecords::reported`]), in task id order, until it refuses one. Says whether it
    /// took every record: `false` means the refused one, and all after it,
    /// remain.
    pub fn keys_after(&self, after: Option<&TaskId>, mut take: impl FnMut(HeldKey) -> bool) -> bool {
        self.lock()
            .reported()
            .filter_map(|record| {
                let key = match held_key(record) {
                    Ok(key) => key,
                    Err(malformed) => {
                        tracing::warn!(%malformed, "a held record does not summarise; left out of the report");
                        return None;
                    }
                };
                after.is_none_or(|after| key.task_id > *after).then_some(key)
            })
            .all(|key| take(key))
    }

    /// The records, after dropping every finished one whose retention has
    /// passed by this worker's clock, so an idle holder never serves one past
    /// its retention.
    fn lock(&self) -> MutexGuard<'_, VersionedRecords> {
        let mut records = self.records.lock().unwrap_or_else(PoisonError::into_inner);
        records.sweep(self.clock.now());
        records
    }
}

/// The kad key a task's record is stored under: its task id's bytes.
pub fn record_key(task: &TaskId) -> kad::RecordKey {
    kad::RecordKey::new(&task.as_str().as_bytes())
}

/// kad's view of [`HeldRecords`]: a put decodes the record and is refused
/// (an `Err`, so kad resets the stream instead of acknowledging it) unless
/// it is newer than what is held or an identical republish. A record that
/// names a publisher was handed over by a draining worker, and is kept
/// whatever holders it names; the leader's own writes name none. kad's error type
/// has no variant for an older record; any `Err` has the effect that
/// matters, and the real reason is logged here.
///
/// This store trusts the peers of its shard. It does not check that a writer
/// is the shard's leader or even a member, and it bounds the size of each
/// record but not how many unfinished records a holder keeps. The version
/// order stops a stale write from replacing a newer record, and the leader's
/// own memory budget bounds what an honest leader writes, but a forged or
/// flooding writer is stopped only by authenticating peers. That
/// authentication, with encrypted transport, is a gate before any real
/// workload adopts the system, not something this store can supply.
pub struct TaskRecordStore {
    held: HeldRecords,
}

impl TaskRecordStore {
    pub fn new(held: HeldRecords) -> Self {
        TaskRecordStore { held }
    }

    fn encoded(record: &TaskRecord) -> Option<kad::Record> {
        let (task, _) = identify(record).ok()?;
        Some(kad::Record::new(record_key(&task), record.encode_to_vec()))
    }
}

impl kad::store::RecordStore for TaskRecordStore {
    type RecordsIter<'a> = std::vec::IntoIter<Cow<'a, kad::Record>>;
    type ProvidedIter<'a> = std::iter::Empty<Cow<'a, kad::ProviderRecord>>;

    fn get(&self, key: &kad::RecordKey) -> Option<Cow<'_, kad::Record>> {
        let task = TaskId::new(std::str::from_utf8(key.as_ref()).ok()?);
        let held = self.held.get(&task)?;
        Self::encoded(&held).map(Cow::Owned)
    }

    fn put(&mut self, record: kad::Record) -> kad::store::Result<()> {
        if record.value.len() as u64 > MAX_RECORD_BYTES {
            tracing::debug!(bytes = record.value.len(), "refusing a record past the size limit");
            return Err(kad::store::Error::ValueTooLarge);
        }
        let decoded = match TaskRecord::decode(&record.value[..]) {
            Ok(decoded) => decoded,
            Err(error) => {
                tracing::debug!(%error, "refusing a value that is not a task record");
                return Err(kad::store::Error::ValueTooLarge);
            }
        };
        let task = match identify(&decoded) {
            Ok((task, _)) if record_key(&task) == record.key => task,
            _ => {
                tracing::debug!("refusing a record that is malformed or stored under another key");
                return Err(kad::store::Error::ValueTooLarge);
            }
        };
        let origin = if record.publisher.is_some() {
            Origin::HandOff
        } else {
            Origin::Leader
        };
        match self.held.lock().put_from(decoded, origin, self.held.clock.now()) {
            Ok(_) => Ok(()),
            Err(refusal) => {
                tracing::debug!(task = task.as_str(), %refusal, "refusing a record");
                Err(kad::store::Error::MaxRecords)
            }
        }
    }

    fn remove(&mut self, key: &kad::RecordKey) {
        if let Ok(task) = std::str::from_utf8(key.as_ref()) {
            self.held.lock().remove(&TaskId::new(task));
        }
    }

    fn records(&self) -> Self::RecordsIter<'_> {
        let records: Vec<Cow<'_, kad::Record>> = self
            .held
            .lock()
            .iter()
            .filter_map(Self::encoded)
            .map(Cow::Owned)
            .collect();
        records.into_iter()
    }

    fn add_provider(&mut self, _: kad::ProviderRecord) -> kad::store::Result<()> {
        Err(kad::store::Error::MaxProvidedKeys)
    }

    fn providers(&self, _: &kad::RecordKey) -> Vec<kad::ProviderRecord> {
        Vec::new()
    }

    fn provided(&self) -> Self::ProvidedIter<'_> {
        std::iter::empty()
    }

    fn remove_provider(&mut self, _: &kad::RecordKey, _: &PeerId) {}
}
