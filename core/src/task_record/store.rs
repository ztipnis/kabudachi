use std::collections::BTreeMap;

use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::{TaskId, WorkerId};
use crate::task_record::version::{RecordVersion, VersionOrder};
use crate::time::{Duration, Instant};

/// What putting a record into a [`VersionedRecords`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Put {
    /// It was newer than what was held, or nothing was held: it is kept now.
    Stored,
    /// It was exactly the record already held: a republish.
    Unchanged,
    /// The leader wrote a revision, newer than the copy held, whose holders do
    /// not include this one: the copy is dropped and nothing is stored.
    Retired,
}

/// Who sent a revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// The shard's leader, writing to the holders it chose.
    Leader,
    /// A draining worker handing over a copy it held.
    HandOff,
}

/// Why a record was not kept. A writer must not count a refused put as
/// stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PutRefusal {
    #[error("a newer revision of the record is held")]
    Older,
    #[error("another record with the same version is held")]
    Conflicting,
    #[error("the record lacks its version, its task or its task id")]
    Malformed,
}

/// The newest revision of each Task record a worker holds. A put never
/// replaces a record with an older one, nor with a different one of the same
/// version, unless it differs only in the voters it is placed on. A store
/// that knows its holder drops a copy the leader moves to other holders, so a
/// copy no later revision would reach does not outlive its placement.
///
/// A finished record is dropped once its retention has passed, counted on
/// this node's own clock from when it first held a finished revision of it.
/// Unfinished records are never dropped, and a put is never refused for lack
/// of room: the leader's memory budget bounds how much there is.
#[derive(Debug, Clone, Default)]
pub struct VersionedRecords {
    records: BTreeMap<TaskId, Held>,
    retention: Option<Duration>,
    holder: Option<WorkerId>,
}

#[derive(Debug, Clone)]
struct Held {
    record: TaskRecord,
    finished_since: Option<Instant>,
}

/// Whether `record` is `held` placed on other holders, and nothing else
/// differs.
fn only_placed_elsewhere(held: &TaskRecord, record: &TaskRecord) -> bool {
    let mut replaced = record.clone();
    replaced.placement.clone_from(&held.placement);
    replaced == *held
}

impl VersionedRecords {
    /// A store that drops a finished record `retention` after it first held
    /// a finished revision of it, or keeps finished records with `None`.
    pub fn with_retention(retention: Option<Duration>) -> Self {
        VersionedRecords {
            records: BTreeMap::new(),
            retention,
            holder: None,
        }
    }

    /// The store of `holder`: a leader's revision whose holders leave `holder`
    /// out retires its copy instead of being stored (see [`Put::Retired`]).
    #[must_use]
    pub fn held_by(mut self, holder: WorkerId) -> Self {
        self.holder = Some(holder);
        self
    }

    /// Puts `record`, at `now` by this node's monotonic clock, after dropping
    /// every finished record whose retention has passed.
    pub fn put(&mut self, record: TaskRecord, now: Instant) -> Result<Put, PutRefusal> {
        self.put_from(record, Origin::Leader, now)
    }

    /// Like [`Self::put`], for a revision sent by `origin`. A leader's
    /// revision, newer than what is held, whose placement names holders but
    /// not this store's own, drops the held copy and is not stored
    /// ([`Put::Retired`]); one that is not newer is refused as by `put`. A
    /// handed-off copy is stored by version alone, whatever its placement
    /// names, and a copy of the revision already held changes nothing.
    pub fn put_from(
        &mut self,
        record: TaskRecord,
        origin: Origin,
        now: Instant,
    ) -> Result<Put, PutRefusal> {
        self.sweep(now);
        let (task, version) = identify(&record)?;
        let leaves_me_out = origin == Origin::Leader
            && self.holder.as_ref().is_some_and(|me| {
                !record.placement.is_empty()
                    && !record.placement.iter().any(|holder| WorkerId::from(holder.clone()) == *me)
            });
        let Some(held) = self.records.get(&task) else {
            if leaves_me_out {
                return Ok(Put::Retired);
            }
            self.records.insert(task, Held::new(record, None, now));
            return Ok(Put::Stored);
        };
        let (_, held_version) = identify(&held.record)?;
        match held_version.order(&version) {
            VersionOrder::Newer if leaves_me_out => {
                self.records.remove(&task);
                Ok(Put::Retired)
            }
            VersionOrder::Newer => {
                let since = held.finished_since;
                self.records.insert(task, Held::new(record, since, now));
                Ok(Put::Stored)
            }
            VersionOrder::Same if held.record == record => Ok(Put::Unchanged),
            VersionOrder::Same if only_placed_elsewhere(&held.record, &record) => {
                if origin == Origin::HandOff {
                    // The same revision, as another holder placed it: nothing
                    // to learn, and its placement is stale where this is not.
                    return Ok(Put::Unchanged);
                }
                if leaves_me_out {
                    self.records.remove(&task);
                    return Ok(Put::Retired);
                }
                // A leader that placed the record anew after its voters
                // changed writes it again to a holder that kept the first
                // write: the holder takes the new placement.
                if let Some(held) = self.records.get_mut(&task) {
                    held.record.placement = record.placement;
                }
                Ok(Put::Stored)
            }
            VersionOrder::Same => Err(PutRefusal::Conflicting),
            VersionOrder::Older => Err(PutRefusal::Older),
        }
    }

    /// Drops every finished record whose retention has passed by `now`, and
    /// says how many.
    pub fn sweep(&mut self, now: Instant) -> usize {
        let Some(retention) = self.retention else {
            return 0;
        };
        let before = self.records.len();
        self.records.retain(|_, held| {
            held.finished_since
                .is_none_or(|since| since + retention > now)
        });
        before - self.records.len()
    }

    /// When the next finished record's retention passes, if any.
    pub fn next_due(&self) -> Option<Instant> {
        let retention = self.retention?;
        self.records
            .values()
            .filter_map(|held| held.finished_since)
            .min()
            .map(|since| since + retention)
    }

    pub fn get(&self, task: &TaskId) -> Option<&TaskRecord> {
        self.records.get(task).map(|held| &held.record)
    }

    pub fn remove(&mut self, task: &TaskId) -> Option<TaskRecord> {
        self.records.remove(task).map(|held| held.record)
    }

    pub fn iter(&self) -> impl Iterator<Item = &TaskRecord> {
        self.records.values().map(|held| &held.record)
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl Held {
    /// `finished_since` is kept once set; otherwise a finished `record`
    /// starts its retention at `now`.
    fn new(record: TaskRecord, finished_since: Option<Instant>, now: Instant) -> Self {
        let finished_since = finished_since.or(record.finished.then_some(now));
        Held {
            record,
            finished_since,
        }
    }
}

/// The task and version a well-formed record names.
pub fn identify(record: &TaskRecord) -> Result<(TaskId, RecordVersion), PutRefusal> {
    let version = record.version.as_ref().ok_or(PutRefusal::Malformed)?;
    let task_id = record
        .task
        .as_ref()
        .and_then(|task| task.task_id.as_ref())
        .ok_or(PutRefusal::Malformed)?;
    Ok((TaskId::new(task_id.value.clone()), RecordVersion::from(version)))
}
