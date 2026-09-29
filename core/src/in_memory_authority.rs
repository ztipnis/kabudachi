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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    /// A clock that moves only when a test advances it. `Clone` shares the
    /// reading, so the test keeps a handle to the authority's clock.
    #[derive(Clone, Default)]
    struct TestClock {
        now: Arc<AtomicU64>,
    }

    impl TestClock {
        fn advance(&self, duration: Duration) {
            self.now.fetch_add(duration.as_ticks(), Ordering::SeqCst);
        }
    }

    impl Clock for TestClock {
        fn now(&self) -> Instant {
            Instant::at(self.now.load(Ordering::SeqCst))
        }

        fn wall_clock_millis(&self) -> u64 {
            0
        }
    }

    const TTL_MILLIS: u64 = 100;

    fn ttl() -> Duration {
        Duration::from_millis(TTL_MILLIS)
    }

    fn millis(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    fn authority() -> (InMemoryAuthority<TestClock>, TestClock) {
        let clock = TestClock::default();
        (InMemoryAuthority::new(clock.clone(), ttl()), clock)
    }

    /// Epoch `number` of one lineage, the one every test here uses unless
    /// it says otherwise.
    fn epoch(number: u64) -> RecoveryEpoch {
        RecoveryEpoch::new(number, 7)
    }

    fn shard() -> ShardId {
        ShardId::new("shard-1")
    }

    fn worker(label: &str) -> WorkerId {
        WorkerId::new(label)
    }

    fn registered_workers(authority: &InMemoryAuthority<TestClock>) -> Vec<WorkerId> {
        authority
            .live_registrations(&shard())
            .expect("the in-memory authority is always reachable")
            .addresses()
            .keys()
            .cloned()
            .collect()
    }

    #[test]
    fn a_registration_is_live_until_one_ttl_after_it_was_made() {
        let (authority, clock) = authority();
        assert_eq!(
            authority.register(&shard(), &worker("a"), "addr-a"),
            Ok(ttl())
        );

        clock.advance(millis(TTL_MILLIS - 1));
        assert_eq!(registered_workers(&authority), vec![worker("a")]);

        clock.advance(millis(1));
        assert_eq!(
            registered_workers(&authority),
            Vec::<WorkerId>::new(),
            "a registration expires at exactly one TTL after it was made"
        );
    }

    #[test]
    fn renewing_a_registration_extends_its_expiry() {
        let (authority, clock) = authority();
        authority
            .register(&shard(), &worker("a"), "addr-a")
            .expect("register succeeds");
        clock.advance(millis(60));
        authority
            .register(&shard(), &worker("a"), "addr-a")
            .expect("renewal succeeds");

        clock.advance(millis(TTL_MILLIS - 1));
        assert_eq!(registered_workers(&authority), vec![worker("a")]);

        clock.advance(millis(1));
        assert_eq!(registered_workers(&authority), Vec::<WorkerId>::new());
    }

    #[test]
    fn live_registrations_report_each_workers_address() {
        let (authority, _clock) = authority();
        authority
            .register(&shard(), &worker("a"), "addr-a")
            .expect("register succeeds");
        authority
            .register(&shard(), &worker("b"), "addr-b")
            .expect("register succeeds");
        authority
            .register(&shard(), &worker("a"), "addr-a2")
            .expect("renewal succeeds");

        let live = authority
            .live_registrations(&shard())
            .expect("the in-memory authority is always reachable");

        assert_eq!(
            live.addresses(),
            &BTreeMap::from([
                (worker("a"), "addr-a2".to_string()),
                (worker("b"), "addr-b".to_string()),
            ]),
            "a renewal replaces the worker's address"
        );
    }

    #[test]
    fn there_is_no_authoritative_count_until_one_full_ttl_after_construction() {
        let (authority, clock) = authority();
        clock.advance(millis(TTL_MILLIS - 1));
        authority
            .register(&shard(), &worker("a"), "addr-a")
            .expect("register succeeds");
        authority
            .register(&shard(), &worker("b"), "addr-b")
            .expect("register succeeds");

        let warming_up = authority.live_registrations(&shard()).unwrap();
        assert_eq!(warming_up.authoritative_count(), None);
        assert_eq!(
            warming_up.addresses().len(),
            2,
            "registrations are reported during warm-up, just not as an authoritative count"
        );

        clock.advance(millis(1));
        assert_eq!(
            authority
                .live_registrations(&shard())
                .unwrap()
                .authoritative_count(),
            Some(2)
        );
    }

    #[test]
    fn create_if_absent_succeeds_once_then_conflicts() {
        let (authority, _clock) = authority();

        assert_eq!(
            authority.compare_and_swap_recovery_epoch(&shard(), None, epoch(0)),
            Ok(())
        );
        assert_eq!(
            authority.compare_and_swap_recovery_epoch(&shard(), None, epoch(0)),
            Err(AuthorityError::EpochConflict {
                current: Some(epoch(0))
            })
        );
        assert_eq!(authority.read_recovery_epoch(&shard()), Ok(Some(epoch(0))));
    }

    #[test]
    fn a_stale_expected_epoch_conflicts_and_changes_nothing() {
        let (authority, _clock) = authority();
        authority
            .compare_and_swap_recovery_epoch(&shard(), None, epoch(4))
            .expect("create succeeds");
        authority
            .compare_and_swap_recovery_epoch(&shard(), Some(epoch(4)), epoch(5))
            .expect("bump succeeds");

        assert_eq!(
            authority.compare_and_swap_recovery_epoch(&shard(), Some(epoch(4)), epoch(6)),
            Err(AuthorityError::EpochConflict {
                current: Some(epoch(5))
            })
        );
        assert_eq!(
            authority.compare_and_swap_recovery_epoch(&shard(), None, epoch(6)),
            Err(AuthorityError::EpochConflict {
                current: Some(epoch(5))
            }),
            "create-if-absent conflicts once the epoch exists"
        );
        let other_lineage = RecoveryEpoch::new(5, 8);
        assert_eq!(
            authority.compare_and_swap_recovery_epoch(&shard(), Some(other_lineage), epoch(6)),
            Err(AuthorityError::EpochConflict {
                current: Some(epoch(5))
            }),
            "the right number of another lineage is not the epoch"
        );
        assert_eq!(authority.read_recovery_epoch(&shard()), Ok(Some(epoch(5))));
    }

    #[test]
    fn a_bump_on_a_missing_epoch_conflicts_with_none() {
        let (authority, _clock) = authority();

        assert_eq!(
            authority.compare_and_swap_recovery_epoch(&shard(), Some(epoch(0)), epoch(1)),
            Err(AuthorityError::EpochConflict { current: None })
        );
        assert_eq!(authority.read_recovery_epoch(&shard()), Ok(None));
    }

    /// An authority past its warm-up, at `epoch`.
    fn authority_at_epoch(number: u64) -> (InMemoryAuthority<TestClock>, TestClock) {
        let (authority, clock) = authority();
        clock.advance(ttl());
        authority
            .compare_and_swap_recovery_epoch(&shard(), None, epoch(number))
            .expect("create succeeds");
        (authority, clock)
    }

    #[test]
    fn no_fence_is_granted_until_warm_up_ends() {
        let (authority, clock) = authority();
        authority
            .compare_and_swap_recovery_epoch(&shard(), None, epoch(3))
            .expect("create succeeds");
        clock.advance(millis(30));

        assert_eq!(
            authority.acquire_fence(&shard(), &worker("a"), epoch(3)),
            Err(AuthorityError::FenceHeld {
                remaining: millis(TTL_MILLIS - 30)
            }),
            "a fence lost with the data could still be running"
        );
        clock.advance(millis(TTL_MILLIS - 30));
        assert_eq!(
            authority.acquire_fence(&shard(), &worker("a"), epoch(3)),
            Ok(ttl())
        );
    }

    #[test]
    fn the_holder_renews_its_own_fence() {
        let (authority, clock) = authority_at_epoch(3);
        authority
            .acquire_fence(&shard(), &worker("a"), epoch(3))
            .expect("acquire succeeds");
        clock.advance(millis(60));

        assert_eq!(
            authority.acquire_fence(&shard(), &worker("a"), epoch(3)),
            Ok(ttl())
        );

        clock.advance(millis(TTL_MILLIS - 1));
        assert_eq!(
            authority.acquire_fence(&shard(), &worker("b"), epoch(3)),
            Err(AuthorityError::FenceHeld {
                remaining: millis(1)
            }),
            "the renewal pushed the fence's expiry to one TTL after it"
        );
    }

    #[test]
    fn another_holder_is_refused_even_after_the_epoch_moves_on() {
        let (authority, clock) = authority_at_epoch(3);
        authority
            .acquire_fence(&shard(), &worker("a"), epoch(3))
            .expect("acquire succeeds");
        authority
            .compare_and_swap_recovery_epoch(&shard(), Some(epoch(3)), epoch(4))
            .expect("bump succeeds");
        clock.advance(millis(30));

        assert_eq!(
            authority.acquire_fence(&shard(), &worker("b"), epoch(4)),
            Err(AuthorityError::FenceHeld {
                remaining: millis(TTL_MILLIS - 30)
            }),
            "a new leader waits out the old leader's fence, whatever epoch it was taken at"
        );
    }

    #[test]
    fn another_holder_acquires_the_fence_once_it_has_expired() {
        let (authority, clock) = authority_at_epoch(3);
        authority
            .acquire_fence(&shard(), &worker("a"), epoch(3))
            .expect("acquire succeeds");
        clock.advance(ttl());

        assert_eq!(
            authority.acquire_fence(&shard(), &worker("b"), epoch(3)),
            Ok(ttl())
        );
        assert_eq!(
            authority.acquire_fence(&shard(), &worker("a"), epoch(3)),
            Err(AuthorityError::FenceHeld { remaining: ttl() }),
            "the fence now belongs to the new holder"
        );
    }

    #[test]
    fn acquiring_the_fence_at_a_different_epoch_conflicts() {
        let (authority, _clock) = authority_at_epoch(3);

        assert_eq!(
            authority.acquire_fence(&shard(), &worker("a"), epoch(2)),
            Err(AuthorityError::EpochConflict {
                current: Some(epoch(3))
            })
        );
        assert_eq!(
            authority.acquire_fence(&shard(), &worker("a"), epoch(4)),
            Err(AuthorityError::EpochConflict {
                current: Some(epoch(3))
            })
        );
        assert_eq!(
            authority.acquire_fence(&shard(), &worker("a"), RecoveryEpoch::new(3, 8)),
            Err(AuthorityError::EpochConflict {
                current: Some(epoch(3))
            }),
            "the right number of another lineage is not the epoch"
        );
    }

    #[test]
    fn the_holder_cannot_renew_after_the_epoch_moves_on() {
        let (authority, _clock) = authority_at_epoch(3);
        authority
            .acquire_fence(&shard(), &worker("a"), epoch(3))
            .expect("acquire succeeds");
        authority
            .compare_and_swap_recovery_epoch(&shard(), Some(epoch(3)), epoch(4))
            .expect("bump succeeds");

        assert_eq!(
            authority.acquire_fence(&shard(), &worker("a"), epoch(3)),
            Err(AuthorityError::EpochConflict {
                current: Some(epoch(4))
            })
        );
    }

    #[test]
    fn acquiring_the_fence_with_no_epoch_conflicts() {
        let (authority, _clock) = authority();

        assert_eq!(
            authority.acquire_fence(&shard(), &worker("a"), epoch(0)),
            Err(AuthorityError::EpochConflict { current: None })
        );
    }

    #[test]
    fn two_shards_are_independent() {
        let (authority, clock) = authority();
        let other_shard = ShardId::new("shard-2");
        clock.advance(ttl());
        authority
            .compare_and_swap_recovery_epoch(&shard(), None, epoch(3))
            .expect("create succeeds");
        authority
            .register(&shard(), &worker("a"), "addr-a")
            .expect("register succeeds");
        authority
            .acquire_fence(&shard(), &worker("a"), epoch(3))
            .expect("acquire succeeds");

        assert_eq!(authority.read_recovery_epoch(&other_shard), Ok(None));
        assert_eq!(
            authority.live_registrations(&other_shard),
            Ok(LiveRegistrations::new(BTreeMap::new(), true))
        );
        authority
            .compare_and_swap_recovery_epoch(&other_shard, None, epoch(0))
            .expect("the other shard's epoch is still missing");
        assert_eq!(
            authority.acquire_fence(&other_shard, &worker("b"), epoch(0)),
            Ok(ttl()),
            "the other shard's fence is free"
        );
    }

    #[test]
    fn clones_share_state() {
        let (authority, _clock) = authority();
        let clone = authority.clone();

        clone
            .compare_and_swap_recovery_epoch(&shard(), None, epoch(0))
            .expect("create succeeds");
        clone
            .register(&shard(), &worker("a"), "addr-a")
            .expect("register succeeds");

        assert_eq!(authority.read_recovery_epoch(&shard()), Ok(Some(epoch(0))));
        assert_eq!(registered_workers(&authority), vec![worker("a")]);
    }
}
