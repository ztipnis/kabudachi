use std::collections::BTreeMap;

use crate::task_record::gate::Write;
use crate::task_record::key::RecordKey;
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
    stored: BTreeMap<RecordKey, RecordVersion>,
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
        if !self.pending.iter().any(|pending| pending.key == write.key) {
            self.stored.remove(&write.key);
        }
    }

    fn store(&mut self, write: &Write) {
        let newest = self.stored.entry(write.key.clone()).or_insert(write.version);
        if matches!(newest.order(&write.version), VersionOrder::Newer) {
            *newest = write.version;
        }
        let newest = *newest;
        self.refused.retain(|refused| {
            refused.key != write.key
                || matches!(newest.order(&refused.version), VersionOrder::Newer)
        });
    }

    fn refuse(&mut self, write: &Write) {
        let superseded = self.stored.get(&write.key).is_some_and(|newest| {
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
                pending.key == write.key
                    && matches!(write.version.order(&pending.version), VersionOrder::Newer)
            })
            .cloned()
            .collect()
    }

    /// What an answer about `record` that made no write of its own waits on.
    /// A composite's answer also waits on its pages: they are the same
    /// call's terminal records, so what it tells must not outrun them.
    pub fn waits_on(&self, record: &RecordKey) -> Waits {
        if self.refused.iter().any(|write| covered_by(&write.key, record)) {
            return Waits::Refused;
        }
        Waits::Writes(
            self.pending
                .iter()
                .filter(|write| covered_by(&write.key, record))
                .cloned()
                .collect(),
        )
    }
}

/// Whether a write of `written` is part of what an answer about `record`
/// tells: the record itself, or, for a composite, one of its pages.
fn covered_by(written: &RecordKey, record: &RecordKey) -> bool {
    match (written, record) {
        (RecordKey::Page(page_of, _), RecordKey::Composite(composite)) => page_of == composite,
        _ => written == record,
    }
}
