//! What a leader office does with its scheduler's revisions.

use crate::protocol::ids::WorkerId;

/// Where one revision of a record is written and how many must store it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The voters that hold the record, nearest the record's key first.
    pub holders: Vec<WorkerId>,
    /// How many of them must store it before the write counts: a majority,
    /// so any later read of `holders.len() - quorum + 1` of them meets it.
    pub quorum: usize,
}
