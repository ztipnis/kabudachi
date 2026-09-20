use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use kabudachi_core::coordination_authority::{AuthorityError, CoordinationAuthority};
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::time::Duration;

/// A shard's stored state. A shard with no entry is "never seen"; `flush_all`
/// removes entries rather than resetting them to epoch 0.
struct ShardState {
    recovery_epoch: u64,
    members: BTreeSet<WorkerId>,
}

struct Inner {
    available: bool,
    #[allow(dead_code)] // Reserved for a later task's "slow authority" scenario; see set_latency.
    latency: Duration,
    /// Workers the authority cannot see for any shard's `discover_workers`, modelling a partition-affected authority.
    unreachable: BTreeSet<WorkerId>,
    shards: BTreeMap<ShardId, ShardState>,
}

/// An in-memory `CoordinationAuthority` with real compare-and-swap semantics
/// plus fault injection (`set_available`, `partition_from`, `flush_all`). A
/// never-seen shard reads as epoch 0 with no members, so a fresh and a flushed
/// shard look the same. `Clone` shares state.
#[derive(Clone)]
pub struct FakeCoordinationAuthority {
    inner: Rc<RefCell<Inner>>,
}

impl FakeCoordinationAuthority {
    pub fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(Inner {
                available: true,
                latency: Duration::from_ticks(0),
                unreachable: BTreeSet::new(),
                shards: BTreeMap::new(),
            })),
        }
    }

    /// While `false`, every method returns `Unavailable`; prior state is kept (`flush_all` discards it).
    pub fn set_available(&self, available: bool) {
        self.inner.borrow_mut().available = available;
    }

    /// Stores the latency for a future "slow authority" scenario (README
    /// §26.3's "no_quorum_redis_slow"). No authority method consults it yet.
    pub fn set_latency(&self, latency: Duration) {
        self.inner.borrow_mut().latency = latency;
    }

    /// Replaces (not adds to) the set of workers the authority cannot see; pass an empty set to clear it.
    pub fn partition_from(&self, unreachable: BTreeSet<WorkerId>) {
        self.inner.borrow_mut().unreachable = unreachable;
    }

    /// Clears all shard state, like Redis `FLUSHALL` (README §15.3). Not part of
    /// the trait: only the test harness simulates it.
    pub fn flush_all(&self) {
        self.inner.borrow_mut().shards.clear();
    }
}

impl Default for FakeCoordinationAuthority {
    fn default() -> Self {
        Self::new()
    }
}

impl CoordinationAuthority for FakeCoordinationAuthority {
    fn discover_workers(&self, shard_id: &ShardId) -> Result<BTreeSet<WorkerId>, AuthorityError> {
        let inner = self.inner.borrow();
        if !inner.available {
            return Err(AuthorityError::Unavailable);
        }

        let members = inner
            .shards
            .get(shard_id)
            .map(|state| state.members.clone())
            .unwrap_or_default();

        Ok(members.difference(&inner.unreachable).cloned().collect())
    }

    fn read_recovery_epoch(&self, shard_id: &ShardId) -> Result<u64, AuthorityError> {
        let inner = self.inner.borrow();
        if !inner.available {
            return Err(AuthorityError::Unavailable);
        }

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
        let mut inner = self.inner.borrow_mut();
        if !inner.available {
            return Err(AuthorityError::Unavailable);
        }

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
