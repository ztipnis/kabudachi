use std::collections::BTreeMap;

use crate::protocol::generated::{self, TaskRecord};
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
    /// The leader wrote a revision whose holders do not include this one, and
    /// that is not older than what is held: the copy is replaced by a stub,
    /// which names the revision and its holders but holds none of the record.
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
/// that knows its holder keeps only a stub of a revision the leader moves to
/// other holders: the task, the version and the holders, no record. The stub
/// is not a copy anyone reads (it is not served, handed off or claimed from),
/// but it is reported (see [`Self::reported`]), so a reader that asks this
/// holder learns that a newer revision exists and where it went.
///
/// A finished record, or the stub of one, is dropped once its retention has
/// passed, counted on this node's own clock from when it first held a
/// finished revision of it. Unfinished records are never dropped, and a put
/// is never refused for lack of room: the leader's memory budget bounds how
/// much there is.
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
    /// Whether `record` is only the key of a revision held elsewhere.
    stub: bool,
}

/// Whether `record` is `held` placed on other holders (and so written to other
/// prior placements), and nothing else differs.
fn only_placed_elsewhere(held: &TaskRecord, record: &TaskRecord) -> bool {
    let mut replaced = record.clone();
    replaced.placement.clone_from(&held.placement);
    replaced.prior_placements.clone_from(&held.prior_placements);
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
    /// out leaves it a stub of the revision instead of a copy (see
    /// [`Put::Retired`]).
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
    /// revision, not older than what is held, whose placement names holders
    /// but not this store's own, replaces the held copy with a stub of it
    /// ([`Put::Retired`]); an older one is refused as by `put`. A
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
            return Ok(self.keep(task, record, leaves_me_out, None, now));
        };
        let (_, held_version) = identify(&held.record)?;
        let since = held.finished_since;
        match held_version.order(&version) {
            VersionOrder::Newer => Ok(self.keep(task, record, leaves_me_out, since, now)),
            // Only the key of this revision is held: a record that names this
            // holder now is the revision itself, and one that does not is the
            // same stub, perhaps placed elsewhere again.
            VersionOrder::Same if held.stub => Ok(self.keep(task, record, leaves_me_out, since, now)),
            VersionOrder::Same if held.record == record => Ok(Put::Unchanged),
            VersionOrder::Same if only_placed_elsewhere(&held.record, &record) => {
                if origin == Origin::HandOff {
                    // The same revision, as another holder placed it: nothing
                    // to learn, and its placement is stale where this is not.
                    return Ok(Put::Unchanged);
                }
                // A leader that placed the record anew after its voters
                // changed writes it again to a holder that kept the first
                // write: the holder takes the new placement, or keeps only a
                // stub of it if the new placement leaves it out.
                Ok(self.keep(task, record, leaves_me_out, since, now))
            }
            VersionOrder::Same => Err(PutRefusal::Conflicting),
            VersionOrder::Older => Err(PutRefusal::Older),
        }
    }

    /// Holds `record`, or only its stub if it leaves this holder out.
    fn keep(
        &mut self,
        task: TaskId,
        record: TaskRecord,
        leaves_me_out: bool,
        finished_since: Option<Instant>,
        now: Instant,
    ) -> Put {
        if leaves_me_out {
            self.records.insert(task, Held::stub_of(&record, finished_since, now));
            Put::Retired
        } else {
            self.records.insert(task, Held::new(record, finished_since, now));
            Put::Stored
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

    /// The record held of `task`, if this store holds one and not only a
    /// stub of it.
    pub fn get(&self, task: &TaskId) -> Option<&TaskRecord> {
        self.records.get(task).filter(|held| !held.stub).map(|held| &held.record)
    }

    /// Removes the record held of `task`; a stub stays, for it is no copy to
    /// remove and a reader still learns from it.
    pub fn remove(&mut self, task: &TaskId) -> Option<TaskRecord> {
        if self.records.get(task).is_some_and(|held| held.stub) {
            return None;
        }
        self.records.remove(task).map(|held| held.record)
    }

    /// The records held, the newest revision of each task: no stub.
    pub fn iter(&self) -> impl Iterator<Item = &TaskRecord> {
        self.records.values().filter(|held| !held.stub).map(|held| &held.record)
    }

    /// What a holder reports of its tasks: each record, and the stub of each
    /// revision held elsewhere, the newest revision of each task.
    pub fn reported(&self) -> impl Iterator<Item = &TaskRecord> {
        self.records.values().map(|held| &held.record)
    }

    /// The tasks held, as records or as stubs.
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
            stub: false,
        }
    }

    /// The stub of `record`: its task (named by its id, its definition and its
    /// coalescing key), version, holders and whether it is finished, and
    /// nothing else.
    fn stub_of(record: &TaskRecord, finished_since: Option<Instant>, now: Instant) -> Self {
        let key = TaskRecord {
            task: record.task.as_ref().map(|task| generated::Task {
                task_id: task.task_id.clone(),
                task_definition_id: task.task_definition_id.clone(),
                coalescing_key: task.coalescing_key.clone(),
                ..Default::default()
            }),
            version: record.version,
            placement: record.placement.clone(),
            finished: record.finished,
            ..Default::default()
        };
        Held {
            stub: true,
            ..Held::new(key, finished_since, now)
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
