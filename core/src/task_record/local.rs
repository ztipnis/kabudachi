use crate::protocol::generated::TaskRecord;
use crate::scheduler::{Change, Counts, Observer};
use crate::task_record::gate::Write;
use crate::task_record::store::VersionedRecords;

/// The record sink of a node that stores its own records, as the one-node
/// runtime does: every revision is put into its own store as it is
/// published, so each write is acknowledged, or refused, before the call
/// that made it returns.
#[derive(Debug, Default)]
pub struct LocalRecords {
    records: VersionedRecords,
    settled: Vec<(Write, bool)>,
}

impl LocalRecords {
    /// The store.
    pub fn records(&self) -> &VersionedRecords {
        &self.records
    }

    /// Each write since the last call, and whether the store kept it.
    pub fn take_settled(&mut self) -> Vec<(Write, bool)> {
        std::mem::take(&mut self.settled)
    }
}

impl Observer for LocalRecords {
    fn notify(&mut self, _: Change<'_>, _: Counts) {}

    fn revision(&mut self, revision: TaskRecord) {
        let write = Write::of(&revision);
        let kept = self.records.put(revision).is_ok();
        self.settled.push((write, kept));
    }
}
