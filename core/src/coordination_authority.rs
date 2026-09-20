//! The external coordination service used for forced recovery (README §9.1,
//! §14.3). It is off the hot path: consulted only for cold-start discovery and
//! forced-recovery decisions.

use std::collections::BTreeSet;

use crate::protocol::ids::{ShardId, WorkerId};

/// A shard the authority has never seen is at recovery epoch `0`.
pub trait CoordinationAuthority {
    /// The authority's own view of the shard's workers, possibly already
    /// narrowed by a partition affecting the authority (README §26.3). A
    /// never-seen shard yields an empty set, not an error.
    fn discover_workers(&self, shard_id: &ShardId) -> Result<BTreeSet<WorkerId>, AuthorityError>;

    /// The shard's current recovery epoch.
    fn read_recovery_epoch(&self, shard_id: &ShardId) -> Result<u64, AuthorityError>;

    /// Compare-and-swap: if `expected_recovery_epoch` matches, bumps the epoch,
    /// replaces the membership and returns the new epoch. On a mismatch it
    /// returns `CasConflict` with the actual epoch and changes nothing.
    fn force_reconfigure(
        &self,
        shard_id: &ShardId,
        expected_recovery_epoch: u64,
        replacement_members: BTreeSet<WorkerId>,
    ) -> Result<u64, AuthorityError>;
}

/// There is no `Partitioned` variant: a partition-affected authority is
/// modelled as `discover_workers` returning a smaller set (README §26.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthorityError {
    #[error("coordination authority is unavailable")]
    Unavailable,
    #[error("recovery epoch conflict: authority is at epoch {current}")]
    CasConflict { current: u64 },
}
