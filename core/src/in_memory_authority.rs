//! An in-memory `CoordinationAuthority` with real compare-and-swap semantics
//! and no fault injection.
//!
//! Contrast `core/tests/support/coordination_authority.rs`'s
//! `FakeCoordinationAuthority`, which adds `set_available`/`partition_from`/
//! `flush_all` for test scenarios that need to simulate authority failure —
//! those stay test-only. This type is `Send + Sync` (an `Arc<Mutex<_>>`
//! rather than that fake's `Rc<RefCell<_>>`) so it can cross the async task
//! boundaries `net`'s bootstrap orchestrator and `bindings`'s single-process
//! runtime both drive worker nodes across.
//!
//! A shard this authority has never seen reads as epoch 0 with no members
//! (see `CoordinationAuthority::discover_workers`'s doc) — which is exactly
//! what README §27 Phase 2's bootstrap cascade step (c) treats as "no
//! authority, unavailable, or empty result": a freshly constructed
//! `InMemoryAuthority` with nothing ever registered against a shard is a
//! legitimate, always-reachable `CoordinationAuthority` that simply always
//! answers "empty" for that shard, so the cascade self-elects without
//! needing a dedicated "no authority" type alongside it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use crate::coordination_authority::{AuthorityError, CoordinationAuthority};
use crate::protocol::ids::{ShardId, WorkerId};

/// A shard's stored state. A shard with no entry is "never seen".
struct ShardState {
    recovery_epoch: u64,
    members: BTreeSet<WorkerId>,
}

struct Inner {
    shards: BTreeMap<ShardId, ShardState>,
}

/// An in-memory `CoordinationAuthority` with real compare-and-swap semantics.
/// `Clone` shares state: every clone reads and writes the same shards.
#[derive(Clone)]
pub struct InMemoryAuthority {
    inner: Arc<Mutex<Inner>>,
}

impl InMemoryAuthority {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                shards: BTreeMap::new(),
            })),
        }
    }
}

impl Default for InMemoryAuthority {
    fn default() -> Self {
        Self::new()
    }
}

impl CoordinationAuthority for InMemoryAuthority {
    fn discover_workers(&self, shard_id: &ShardId) -> Result<BTreeSet<WorkerId>, AuthorityError> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(inner
            .shards
            .get(shard_id)
            .map(|state| state.members.clone())
            .unwrap_or_default())
    }

    fn read_recovery_epoch(&self, shard_id: &ShardId) -> Result<u64, AuthorityError> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(inner
            .shards
            .get(shard_id)
            .map(|state| state.recovery_epoch)
            .unwrap_or(0))
    }

    fn force_reconfigure(
        &self,
        shard_id: &ShardId,
        expected_recovery_epoch: u64,
        replacement_members: BTreeSet<WorkerId>,
    ) -> Result<u64, AuthorityError> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let current = inner
            .shards
            .get(shard_id)
            .map(|state| state.recovery_epoch)
            .unwrap_or(0);

        if current != expected_recovery_epoch {
            return Err(AuthorityError::CasConflict { current });
        }

        let new_epoch = current + 1;
        inner.shards.insert(
            shard_id.clone(),
            ShardState {
                recovery_epoch: new_epoch,
                members: replacement_members,
            },
        );

        Ok(new_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(label: &str) -> WorkerId {
        WorkerId::new(label)
    }

    fn set(labels: &[&str]) -> BTreeSet<WorkerId> {
        labels.iter().map(|l| worker(l)).collect()
    }

    #[test]
    fn a_never_seen_shard_reads_as_epoch_zero_with_no_members() {
        let authority = InMemoryAuthority::new();
        let shard_id = ShardId::new("shard-1");

        assert_eq!(authority.read_recovery_epoch(&shard_id), Ok(0));
        assert_eq!(authority.discover_workers(&shard_id), Ok(BTreeSet::new()));
    }

    #[test]
    fn force_reconfigure_succeeds_when_expected_epoch_matches() {
        let authority = InMemoryAuthority::new();
        let shard_id = ShardId::new("shard-1");

        let result = authority.force_reconfigure(&shard_id, 0, set(&["a", "b"]));

        assert_eq!(result, Ok(1));
        assert_eq!(authority.read_recovery_epoch(&shard_id), Ok(1));
        assert_eq!(authority.discover_workers(&shard_id), Ok(set(&["a", "b"])));
    }

    #[test]
    fn force_reconfigure_fails_with_cas_conflict_and_no_state_change() {
        let authority = InMemoryAuthority::new();
        let shard_id = ShardId::new("shard-1");
        assert_eq!(
            authority.force_reconfigure(&shard_id, 0, set(&["a", "b"])),
            Ok(1)
        );

        let result = authority.force_reconfigure(&shard_id, 0, set(&["c"]));

        assert_eq!(result, Err(AuthorityError::CasConflict { current: 1 }));
        assert_eq!(authority.read_recovery_epoch(&shard_id), Ok(1));
        assert_eq!(authority.discover_workers(&shard_id), Ok(set(&["a", "b"])));
    }

    #[test]
    fn two_shards_are_tracked_independently() {
        let authority = InMemoryAuthority::new();
        let shard_1 = ShardId::new("shard-1");
        let shard_2 = ShardId::new("shard-2");

        authority
            .force_reconfigure(&shard_1, 0, set(&["a"]))
            .expect("first force_reconfigure on a never-seen shard succeeds");

        assert_eq!(authority.read_recovery_epoch(&shard_2), Ok(0));
        assert_eq!(authority.discover_workers(&shard_2), Ok(BTreeSet::new()));
    }

    #[test]
    fn clone_shares_state() {
        let authority = InMemoryAuthority::new();
        let clone = authority.clone();
        let shard_id = ShardId::new("shard-1");

        clone
            .force_reconfigure(&shard_id, 0, set(&["a"]))
            .expect("first force_reconfigure on a never-seen shard succeeds");

        assert_eq!(authority.discover_workers(&shard_id), Ok(set(&["a"])));
    }

    #[test]
    fn is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InMemoryAuthority>();
    }
}
