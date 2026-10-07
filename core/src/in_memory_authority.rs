//! An in-memory `CoordinationAuthority` with the real semantics: TTL
//! registrations, warm-up, a compare-and-swap recovery epoch and a renewable
//! recovery fence, all timed by a `Clock`. It injects no faults.
//!
//! It serves every test and any single process that wants an authority
//! without running one. A fresh `InMemoryAuthority` is exactly what a real
//! authority looks like after losing all its data: no epochs, no
//! registrations, no fences, and warming up again.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::coordination_authority::{
    AuthorityError, CoordinationAuthority, LiveRegistrations, RecoveryEpoch,
};
use crate::protocol::ids::{ShardId, WorkerId};
use crate::time::{Clock, Duration, Instant};

/// An in-memory `CoordinationAuthority` whose registrations, fences and
/// warm-up all last `ttl` on `clock`. `Clone` shares state: every clone reads
/// and writes the same shards.
#[derive(Clone)]
pub struct InMemoryAuthority<C> {
    clock: C,
    ttl: Duration,
    /// Warm-up ends one `ttl` after this.
    created_at: Instant,
    shards: Arc<Mutex<BTreeMap<ShardId, ShardState>>>,
}

/// A shard with no entry has no epoch, registrations or fence.
#[derive(Default)]
struct ShardState {
    recovery_epoch: Option<RecoveryEpoch>,
    registrations: BTreeMap<WorkerId, Registration>,
    fence: Option<Fence>,
}

struct Registration {
    address: String,
    expires_at: Instant,
}

struct Fence {
    holder: WorkerId,
    expires_at: Instant,
}

impl<C: Clock> InMemoryAuthority<C> {
    /// An authority with no shards, warming up from `clock`'s current
    /// reading.
    pub fn new(clock: C, ttl: Duration) -> Self {
        let created_at = clock.now();
        Self {
            clock,
            ttl,
            created_at,
            shards: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn shards(&self) -> MutexGuard<'_, BTreeMap<ShardId, ShardState>> {
        self.shards.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<C: Clock> CoordinationAuthority for InMemoryAuthority<C> {
    fn register(
        &self,
        shard_id: &ShardId,
        worker_id: &WorkerId,
        address: &str,
    ) -> Result<Duration, AuthorityError> {
        let expires_at = self.clock.now() + self.ttl;
        self.shards()
            .entry(shard_id.clone())
            .or_default()
            .registrations
            .insert(
                worker_id.clone(),
                Registration {
                    address: address.to_string(),
                    expires_at,
                },
            );
        Ok(self.ttl)
    }

    fn live_registrations(&self, shard_id: &ShardId) -> Result<LiveRegistrations, AuthorityError> {
        let now = self.clock.now();
        let addresses = self
            .shards()
            .get(shard_id)
            .map(|state| {
                state
                    .registrations
                    .iter()
                    .filter(|(_, registration)| now < registration.expires_at)
                    .map(|(worker_id, registration)| {
                        (worker_id.clone(), registration.address.clone())
                    })
                    .collect()
            })
            .unwrap_or_default();
        let warmed_up = now - self.created_at >= self.ttl;
        Ok(LiveRegistrations::new(addresses, warmed_up))
    }

    fn read_recovery_epoch(
        &self,
        shard_id: &ShardId,
    ) -> Result<Option<RecoveryEpoch>, AuthorityError> {
        Ok(self
            .shards()
            .get(shard_id)
            .and_then(|state| state.recovery_epoch))
    }

    fn compare_and_swap_recovery_epoch(
        &self,
        shard_id: &ShardId,
        expected: Option<RecoveryEpoch>,
        new: RecoveryEpoch,
    ) -> Result<(), AuthorityError> {
        let mut shards = self.shards();
        let state = shards.entry(shard_id.clone()).or_default();
        if state.recovery_epoch != expected {
            return Err(AuthorityError::EpochConflict {
                current: state.recovery_epoch,
            });
        }
        state.recovery_epoch = Some(new);
        Ok(())
    }

    fn acquire_fence(
        &self,
        shard_id: &ShardId,
        holder: &WorkerId,
        recovery_epoch: RecoveryEpoch,
    ) -> Result<Duration, AuthorityError> {
        let now = self.clock.now();
        let mut shards = self.shards();
        let state = shards.entry(shard_id.clone()).or_default();

        if state.recovery_epoch != Some(recovery_epoch) {
            return Err(AuthorityError::EpochConflict {
                current: state.recovery_epoch,
            });
        }
        if let Some(fence) = &state.fence
            && fence.holder != *holder
            && now < fence.expires_at
        {
            return Err(AuthorityError::FenceHeld {
                remaining: fence.expires_at - now,
            });
        }
        // Fences taken before this authority started (it may have lost
        // them with its data) have all expired one TTL after it started.
        let warm_up_ends = self.created_at + self.ttl;
        if now < warm_up_ends {
            return Err(AuthorityError::FenceHeld {
                remaining: warm_up_ends - now,
            });
        }

        state.fence = Some(Fence {
            holder: holder.clone(),
            expires_at: now + self.ttl,
        });
        Ok(self.ttl)
    }
}
