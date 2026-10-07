use std::collections::BTreeMap;

use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::TaskId;
use crate::task_record::version::{RecordVersion, VersionOrder};
use crate::time::{Duration, Instant};

/// What putting a record into a [`VersionedRecords`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Put {
    /// It was newer than what was held, or nothing was held: it is kept now.
    Stored,
    /// It was exactly the record already held: a republish.
    Unchanged,
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
/// version.
///
/// A finished record is dropped once its retention has passed, counted on
/// this node's own clock from when it first held a finished revision of it.
/// Unfinished records are never dropped, and a put is never refused for lack
/// of room: the leader's memory budget bounds how much there is.
#[derive(Debug, Clone, Default)]
pub struct VersionedRecords {
    records: BTreeMap<TaskId, Held>,
    retention: Option<Duration>,
}

#[derive(Debug, Clone)]
struct Held {
    record: TaskRecord,
    finished_since: Option<Instant>,
}

impl VersionedRecords {
    /// A store that drops a finished record `retention` after it first held
    /// a finished revision of it, or keeps finished records with `None`.
    pub fn with_retention(retention: Option<Duration>) -> Self {
        VersionedRecords {
            records: BTreeMap::new(),
            retention,
        }
    }

    /// Puts `record`, at `now` by this node's monotonic clock, after dropping
    /// every finished record whose retention has passed.
    pub fn put(&mut self, record: TaskRecord, now: Instant) -> Result<Put, PutRefusal> {
        self.sweep(now);
        let (task, version) = identify(&record)?;
        let Some(held) = self.records.get(&task) else {
            self.records.insert(task, Held::new(record, None, now));
            return Ok(Put::Stored);
        };
        let (_, held_version) = identify(&held.record)?;
        match held_version.order(&version) {
            VersionOrder::Newer => {
                let since = held.finished_since;
                self.records.insert(task, Held::new(record, since, now));
                Ok(Put::Stored)
            }
            VersionOrder::Same if held.record == record => Ok(Put::Unchanged),
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
