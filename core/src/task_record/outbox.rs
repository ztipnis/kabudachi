use crate::protocol::generated::TaskRecord;
use crate::scheduler::Observer;
use crate::task_record::PublishedRevisions;

/// A record sink that keeps each revision until its driver takes it, to
/// write it where it belongs.
#[derive(Debug, Default)]
pub struct RecordOutbox {
    revisions: Vec<TaskRecord>,
}

impl RecordOutbox {
    /// Every revision published since the last call, oldest first.
    pub fn take(&mut self) -> Vec<TaskRecord> {
        std::mem::take(&mut self.revisions)
    }
}

impl Observer for RecordOutbox {
    fn revision(&mut self, revision: TaskRecord) {
        self.revisions.push(revision);
    }
}

impl PublishedRevisions for RecordOutbox {
    fn take_published(&mut self) -> Vec<TaskRecord> {
        self.take()
    }
}
