//! What a leader office does with its scheduler's revisions.

mod reconciliation;

pub use reconciliation::{OfficeReconciliation, Progress, Stuck};

use crate::protocol::ids::{TaskId, WorkerId};

use super::{PlacedWrite, Write};

/// Where one revision of a record is written and how many must store it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The voters that hold the record, nearest the record's key first.
    pub holders: Vec<WorkerId>,
    /// How many of them must store it before the write counts: a majority,
    /// so any later read of `holders.len() - quorum + 1` of them meets it.
    pub quorum: usize,
}

/// How a leader's records reach its shard: where a record of a task is
/// held, and the writes that carry a revision there. Every worker of a shard
/// must place records the same way.
pub trait RecordPorts {
    /// The holders of a record of `task` among `voters`, and how many of
    /// them must store a revision; `None` when it cannot be placed there.
    fn place(&self, task: &TaskId, voters: &[WorkerId]) -> Option<Placement>;
    /// Starts each write. Each outcome comes back later, as a
    /// [`WriteOutcome`](super::WriteOutcome) for the leader to settle.
    fn write(&mut self, writes: Vec<PlacedWrite>);
    /// `write` could not be placed: its outcome is to come back as not
    /// stored, like a write no holder stored.
    fn refuse(&mut self, write: Write);
}
