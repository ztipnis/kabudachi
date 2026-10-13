use crate::protocol::generated::{CompositePage, CompositeRecord, TaskRecord};
use crate::scheduler::Observer;
use crate::task_record::PublishedRevisions;
use crate::task_record::key::ShardRecord;

/// A record sink that keeps each revision until its driver takes it, to
/// write it where it belongs.
#[derive(Debug, Default)]
pub struct RecordOutbox {
    revisions: Vec<ShardRecord>,
}

impl RecordOutbox {
    /// Every revision published since the last call, oldest first.
    pub fn take(&mut self) -> Vec<ShardRecord> {
        std::mem::take(&mut self.revisions)
    }
}

impl Observer for RecordOutbox {
    fn revision(&mut self, revision: TaskRecord) {
        self.revisions.push(revision.into());
    }

    fn composite_revision(&mut self, revision: CompositeRecord) {
        self.revisions.push(revision.into());
    }

    fn page_revision(&mut self, revision: CompositePage) {
        self.revisions.push(revision.into());
    }
}

impl PublishedRevisions for RecordOutbox {
    fn take_published(&mut self) -> Vec<ShardRecord> {
        self.take()
    }
}
