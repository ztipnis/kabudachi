//! A `CoordinationAuthority` stub for tests that never exercise forced
//! recovery. This chunk's electorates are statically seeded with both
//! `WorkerId`s from the start (no join protocol, no authority-driven
//! discovery — that's chunk C4), so "always unavailable" is the simplest
//! correct stand-in: `WorkerNode` only calls into the authority from
//! `attempt_forced_recovery`, which this test's driver loop never calls.
//!
//! Not `core::in_memory_authority::InMemoryAuthority` (that doesn't exist
//! yet — it's C5's), and not `core/tests/support`'s
//! `FakeCoordinationAuthority` (that lives under `core/tests/`, outside the
//! public `kabudachi_core` library crate, so `net`'s tests can't reach it).

use std::collections::BTreeSet;

use kabudachi_core::coordination_authority::{AuthorityError, CoordinationAuthority};
use kabudachi_core::protocol::ids::{ShardId, WorkerId};

#[derive(Debug, Clone, Copy, Default)]
pub struct AlwaysUnavailableAuthority;

impl CoordinationAuthority for AlwaysUnavailableAuthority {
    fn discover_workers(&self, _shard_id: &ShardId) -> Result<BTreeSet<WorkerId>, AuthorityError> {
        Err(AuthorityError::Unavailable)
    }

    fn read_recovery_epoch(&self, _shard_id: &ShardId) -> Result<u64, AuthorityError> {
        Err(AuthorityError::Unavailable)
    }

    fn force_reconfigure(
        &self,
        _shard_id: &ShardId,
        _expected_recovery_epoch: u64,
        _replacement_members: BTreeSet<WorkerId>,
    ) -> Result<u64, AuthorityError> {
        Err(AuthorityError::Unavailable)
    }
}
