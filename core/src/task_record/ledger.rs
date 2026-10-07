use std::collections::BTreeMap;

use crate::protocol::ids::TaskId;
use crate::task_record::gate::Write;
use crate::task_record::version::{RecordVersion, VersionOrder};

/// What an answer that made no write of its own waits on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Waits {
    /// The task's writes still pending.
    Writes(Vec<Write>),
    /// A write of the task was refused and no newer one stored: answer
    /// `NotLeader` at once.
    Refused,
}

/// The writes a leader made whose outcome still bears on what it may tell.
///
/// A write is pending until its outcome arrives. One refused at its quorum
/// may or may not have landed, so until a newer revision of the task is
/// stored, nothing the leader decided about the task may be told. Outcomes
/// may arrive out of order: a refusal of a revision at or below one already
/// stored gates nothing, because the stored one supersedes it.
///
/// Nothing re-publishes a refused task, so a refused task of a still-leading
/// scheduler stays gated for as long as it leads. The ledger is dropped with
/// [`clear`](WriteLedger::clear) when the scheduler stops leading.
#[derive(Debug, Default)]
pub struct WriteLedger {
    /// Made, and no outcome yet.
    pending: Vec<Write>,
    /// Refused, and no newer revision of the task stored since.
    refused: Vec<Write>,
    /// The newest revision stored of each task that still has a write
    /// pending, to judge a refusal that arrives after it.
    stored: BTreeMap<TaskId, RecordVersion>,
}

impl WriteLedger {
    /// `writes` were made and await their outcomes.
    pub fn made(&mut self, writes: &[Write]) {
        self.pending.extend(writes.iter().cloned());
    }

    /// Records `write`'s outcome. `leading` is whether the scheduler still
    /// leads: the outcome of a revision of a term that is over is dropped.
    pub fn settled(&mut self, write: &Write, stored: bool, leading: bool) {
        self.pending.retain(|pending| pending != write);
        if leading {
            if stored {
                self.store(write);
            } else {
                self.refuse(write);
            }
        }
        if !self.pending.iter().any(|pending| pending.task_id == write.task_id) {
            self.stored.remove(&write.task_id);
        }
    }

    fn store(&mut self, write: &Write) {
        let newest = self.stored.entry(write.task_id.clone()).or_insert(write.version);
        if matches!(newest.order(&write.version), VersionOrder::Newer) {
            *newest = write.version;
        }
        let newest = *newest;
        self.refused.retain(|refused| {
            refused.task_id != write.task_id
                || matches!(newest.order(&refused.version), VersionOrder::Newer)
        });
    }

    fn refuse(&mut self, write: &Write) {
        let superseded = self.stored.get(&write.task_id).is_some_and(|newest| {
            !matches!(newest.order(&write.version), VersionOrder::Newer)
        });
        if !superseded && !self.refused.contains(write) {
            self.refused.push(write.clone());
        }
    }

    /// Forgets every write: they are revisions of a term that is over, so
    /// nothing the leader decided in it is told any more.
    pub fn clear(&mut self) {
        self.pending.clear();
        self.refused.clear();
        self.stored.clear();
    }

    /// The writes of `write`'s task still pending that are newer than it.
    pub fn pending_newer_than(&self, write: &Write) -> Vec<Write> {
        self.pending
            .iter()
            .filter(|pending| {
                pending.task_id == write.task_id
                    && matches!(write.version.order(&pending.version), VersionOrder::Newer)
            })
            .cloned()
            .collect()
    }

    /// What an answer about `task` that made no write of its own waits on.
    pub fn waits_on(&self, task: &TaskId) -> Waits {
        if self.refused.iter().any(|write| write.task_id == *task) {
            return Waits::Refused;
        }
        Waits::Writes(
            self.pending
                .iter()
                .filter(|write| write.task_id == *task)
                .cloned()
                .collect(),
        )
    }
}
