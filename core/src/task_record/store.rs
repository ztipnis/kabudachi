use std::collections::BTreeMap;

use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::TaskId;
use crate::task_record::version::{RecordVersion, VersionOrder};

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
#[derive(Debug, Clone, Default)]
pub struct VersionedRecords {
    records: BTreeMap<TaskId, TaskRecord>,
}

impl VersionedRecords {
    pub fn put(&mut self, record: TaskRecord) -> Result<Put, PutRefusal> {
        let (task, version) = identify(&record)?;
        let Some(held) = self.records.get(&task) else {
            self.records.insert(task, record);
            return Ok(Put::Stored);
        };
        let (_, held_version) = identify(held)?;
        match held_version.order(&version) {
            VersionOrder::Newer => {
                self.records.insert(task, record);
                Ok(Put::Stored)
            }
            VersionOrder::Same if *held == record => Ok(Put::Unchanged),
            VersionOrder::Same => Err(PutRefusal::Conflicting),
            VersionOrder::Older => Err(PutRefusal::Older),
        }
    }

    pub fn get(&self, task: &TaskId) -> Option<&TaskRecord> {
        self.records.get(task)
    }

    pub fn remove(&mut self, task: &TaskId) -> Option<TaskRecord> {
        self.records.remove(task)
    }

    pub fn iter(&self) -> impl Iterator<Item = &TaskRecord> {
        self.records.values()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
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
