//! An in-memory `CoordinationAuthority` with the real semantics: TTL
//! registrations, warm-up, a compare-and-swap shard record, a renewable
//! recovery fence and a leader hint, all timed by a `Clock`. It injects no
//! faults.
//!
//! It serves every test and any single process that wants an authority
//! without running one. A fresh `InMemoryAuthority` is exactly what a real
//! authority looks like after losing all its data: no shard records, no
//! registrations, no fences, no hints, and warming up again. [`InMemoryAuthority::flush`]
//! makes an existing one lose its data the same way, and
//! [`InMemoryAuthority::back_from_outage`] tells it that it was down and is
//! back.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::coordination_authority::{
    AuthorityError, CoordinationAuthority, LeaderHint, LiveRegistrations, ShardRecord,
};
use crate::protocol::ids::{ShardId, ShardName, WorkerId};
use crate::time::{Clock, Duration, Instant};

/// An in-memory `CoordinationAuthority` whose registrations, fences and
/// warm-up all last `ttl` on `clock`. `Clone` shares state: every clone reads
/// and writes the same shards, and sees the same flushes and outages.
#[derive(Clone)]
pub struct InMemoryAuthority<C> {
    clock: C,
    ttl: Duration,
    state: Arc<Mutex<State>>,
}

/// Everything the authority holds, and when its two warm-ups started.
struct State {
    names: BTreeMap<ShardName, NameState>,
    /// When it started or last lost its data. Fences taken before then are
    /// unknown to it, so it grants none until one TTL after this.
    data_since: Instant,
    /// When it started, last lost its data or last came back from an
    /// outage. Registrations may have lapsed, or be unknown to it, so it
    /// gives no authoritative count until one TTL after this.
    available_since: Instant,
}

/// A name with no entry has no record, registrations, fence or hint.
#[derive(Default)]
struct NameState {
    record: Option<ShardRecord>,
    registrations: BTreeMap<WorkerId, Registration>,
    fence: Option<Fence>,
    hint: Option<(LeaderHint, Instant)>,
}

struct Registration {
    shard_id: ShardId,
    address: String,
    expires_at: Instant,
}

struct Fence {
    holder: WorkerId,
    expires_at: Instant,
}

impl<C: Clock> InMemoryAuthority<C> {
    /// An authority with no names, warming up from `clock`'s current
    /// reading.
    pub fn new(clock: C, ttl: Duration) -> Self {
        let now = clock.now();
        Self {
            clock,
            ttl,
            state: Arc::new(Mutex::new(State {
                names: BTreeMap::new(),
                data_since: now,
                available_since: now,
            })),
        }
    }

    /// Loses every record, registration, fence and hint, as a real authority does
    /// when its data is flushed, and warms up again from now: no fence and
    /// no authoritative count for one TTL.
    pub fn flush(&self) {
        let now = self.clock.now();
        let mut state = self.state();
        state.names.clear();
        state.data_since = now;
        state.available_since = now;
    }

    /// The authority was down and is back now, with its data. Registrations
    /// kept expiring while it was down and no worker could renew, so it
    /// gives no authoritative count for one TTL from now. It still knows
    /// every fence, so fences need no such wait.
    pub fn back_from_outage(&self) {
        let now = self.clock.now();
        self.state().available_since = now;
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<C: Clock> CoordinationAuthority for InMemoryAuthority<C> {
    fn ttl(&self) -> Duration {
        self.ttl
    }

    fn register(
        &self,
        name: &ShardName,
        shard_id: &ShardId,
        worker_id: &WorkerId,
        address: &str,
    ) -> Result<Duration, AuthorityError> {
        let expires_at = self.clock.now() + self.ttl;
        self.state()
            .names
            .entry(name.clone())
            .or_default()
            .registrations
            .insert(
                worker_id.clone(),
                Registration {
                    shard_id: shard_id.clone(),
                    address: address.to_string(),
                    expires_at,
                },
            );
        Ok(self.ttl)
    }

    fn live_registrations(
        &self,
        name: &ShardName,
        shard_id: &ShardId,
    ) -> Result<LiveRegistrations, AuthorityError> {
        let now = self.clock.now();
        let state = self.state();
        let addresses = state
            .names
            .get(name)
            .map(|shard| {
                shard
                    .registrations
                    .iter()
                    .filter(|(_, registration)| {
                        now < registration.expires_at && registration.shard_id == *shard_id
                    })
                    .map(|(worker_id, registration)| {
                        (worker_id.clone(), registration.address.clone())
                    })
                    .collect()
            })
            .unwrap_or_default();
        let warmed_up = now - state.available_since >= self.ttl;
        Ok(LiveRegistrations::new(addresses, warmed_up))
    }

    fn read_shard(&self, name: &ShardName) -> Result<Option<ShardRecord>, AuthorityError> {
        Ok(self
            .state()
            .names
            .get(name)
            .and_then(|shard| shard.record.clone()))
    }

    fn compare_and_swap_shard(
        &self,
        name: &ShardName,
        expected: Option<&ShardRecord>,
        new: &ShardRecord,
    ) -> Result<(), AuthorityError> {
        let mut state = self.state();
        let shard = state.names.entry(name.clone()).or_default();
        if shard.record.as_ref() != expected {
            return Err(AuthorityError::ShardConflict {
                current: shard.record.clone(),
            });
        }
        shard.record = Some(new.clone());
        Ok(())
    }

    fn acquire_fence(
        &self,
        name: &ShardName,
        holder: &WorkerId,
        record: &ShardRecord,
    ) -> Result<Duration, AuthorityError> {
        let now = self.clock.now();
        let mut state = self.state();
        // Fences taken before this authority started or lost its data are
        // unknown to it, and have all expired one TTL after that.
        let warm_up_ends = state.data_since + self.ttl;
        let shard = state.names.entry(name.clone()).or_default();

        if shard.record.as_ref() != Some(record) {
            return Err(AuthorityError::ShardConflict {
                current: shard.record.clone(),
            });
        }
        if let Some(fence) = &shard.fence
            && fence.holder != *holder
            && now < fence.expires_at
        {
            return Err(AuthorityError::FenceHeld {
                remaining: fence.expires_at - now,
            });
        }
        if now < warm_up_ends {
            return Err(AuthorityError::FenceHeld {
                remaining: warm_up_ends - now,
            });
        }

        shard.fence = Some(Fence {
            holder: holder.clone(),
            expires_at: now + self.ttl,
        });
        Ok(self.ttl)
    }

    fn publish_leader_hint(
        &self,
        name: &ShardName,
        hint: &LeaderHint,
    ) -> Result<(), AuthorityError> {
        let expires_at = self.clock.now() + self.ttl;
        self.state().names.entry(name.clone()).or_default().hint = Some((hint.clone(), expires_at));
        Ok(())
    }

    fn read_leader_hint(&self, name: &ShardName) -> Result<Option<LeaderHint>, AuthorityError> {
        let now = self.clock.now();
        Ok(self
            .state()
            .names
            .get(name)
            .and_then(|shard| shard.hint.as_ref())
            .filter(|(_, expires_at)| now < *expires_at)
            .map(|(hint, _)| hint.clone()))
    }
}
