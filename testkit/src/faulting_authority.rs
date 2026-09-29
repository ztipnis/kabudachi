//! A `CoordinationAuthority` for tests that need the authority to fail, for
//! one worker or for all of them.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use kabudachi_core::coordination_authority::{
    AuthorityError, CoordinationAuthority, LiveRegistrations, RecoveryEpoch,
};
use kabudachi_core::in_memory_authority::InMemoryAuthority;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::time::{Clock, Duration, Instant};

/// One worker's connection to a shared `InMemoryAuthority`, with faults a
/// test can switch on for that worker alone, and an outage it can switch on
/// for the whole authority.
///
/// Every handle made with [`Self::for_another_worker`] reaches the same
/// authority but has its own faults, so a test can cut one worker off while
/// the rest carry on. `Clone` shares both the authority and the faults: a
/// test keeps a clone of the handle it gives a node, and flips faults on the
/// node's connection through it.
pub struct FaultingAuthority<C> {
    shared: Arc<SharedAuthority<C>>,
    faults: Arc<Mutex<Faults>>,
    gate: Arc<Gate>,
}

/// One kind of authority call, for [`FaultingAuthority::hold_next`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CallKind {
    Register,
    LiveRegistrations,
    ReadRecoveryEpoch,
    SwapRecoveryEpoch,
    AcquireFence,
}

/// One connection's held calls. Kept apart from [`Faults`] and taken before
/// any authority lock, and never while one is held: a held call waits here,
/// so it must not stop any other call, on this handle or another.
#[derive(Default)]
struct Gate {
    slots: Mutex<BTreeMap<CallKind, Slot>>,
    released: Condvar,
}

/// The hold on one kind of call.
#[derive(Default)]
struct Slot {
    /// The next call of this kind will hold.
    armed: bool,
    /// Calls of this kind waiting for a release.
    holding: u32,
    /// Counts releases, so a waiting call can tell its own was released.
    releases: u64,
}

/// What every handle reaches. `flush` swaps `current` for a fresh authority,
/// so the clock and TTL to build one are kept beside it.
///
/// Lock order: `current` first, then `availability` or a handle's `faults`,
/// one at a time. An operation reads the faults and the outage while it
/// holds `current`, and every switch that changes them takes `current`
/// first. So a switch cannot land between an operation's check and its
/// action: once `set_available(false)` returns, no operation reaches the
/// authority, and once `set_reachable(false)` returns, none through that
/// handle does.
struct SharedAuthority<C> {
    clock: C,
    ttl: Duration,
    current: Mutex<InMemoryAuthority<C>>,
    availability: Mutex<Availability>,
}

/// Whether the whole authority is up. The default is up, with no outage.
#[derive(Default)]
struct Availability {
    down: bool,
    /// When the authority last came back from an outage.
    back_up_at: Option<Instant>,
}

/// One connection's faults. The default is a healthy connection.
#[derive(Default)]
struct Faults {
    unreachable: bool,
    lose_next_race: bool,
}

impl<C> Clone for FaultingAuthority<C> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            faults: Arc::clone(&self.faults),
            gate: Arc::clone(&self.gate),
        }
    }
}

impl<C: Clock + Clone> FaultingAuthority<C> {
    /// A handle over a new authority whose registrations, fences and warm-up
    /// last `ttl` on `clock`. It starts warming up at `clock`'s current
    /// reading.
    pub fn new(clock: C, ttl: Duration) -> Self {
        let authority = InMemoryAuthority::new(clock.clone(), ttl);
        Self {
            shared: Arc::new(SharedAuthority {
                clock,
                ttl,
                current: Mutex::new(authority),
                availability: Mutex::new(Availability::default()),
            }),
            faults: Arc::new(Mutex::new(Faults::default())),
            gate: Arc::new(Gate::default()),
        }
    }

    /// A handle over the same authority for a different worker, with no
    /// faults of its own.
    pub fn for_another_worker(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            faults: Arc::new(Mutex::new(Faults::default())),
            gate: Arc::new(Gate::default()),
        }
    }

    /// While unreachable, every operation through this handle (and its
    /// clones) returns `Unavailable` and leaves the authority untouched.
    pub fn set_reachable(&self, reachable: bool) {
        let _operations_paused = lock(&self.shared.current);
        self.faults().unreachable = !reachable;
    }

    /// Takes the whole authority down, or brings it back, for every handle
    /// over it. Unlike [`Self::set_reachable`], which cuts one worker's
    /// connection while the authority carries on for the rest, this models
    /// the authority itself failing.
    ///
    /// While it is down, every operation through every handle returns
    /// `Unavailable`. The authority keeps its data, epochs and fences
    /// included, but no worker can renew, so registrations keep expiring on
    /// the clock.
    ///
    /// When it comes back, it warms up again: `live_registrations` gives no
    /// authoritative count until one TTL after that instant. An outage longer
    /// than a TTL leaves every registration lapsed, and the first workers to
    /// register again must not pass for the whole shard. Making an available
    /// authority available changes nothing.
    pub fn set_available(&self, available: bool) {
        let _operations_paused = lock(&self.shared.current);
        let mut availability = lock(&self.shared.availability);
        if !available {
            availability.down = true;
        } else if availability.down {
            availability.down = false;
            availability.back_up_at = Some(self.shared.clock.now());
        }
    }

    /// Makes this handle's next compare-and-swap that reaches the authority
    /// lose a race: a rival compare-and-swap with the same `expected` and
    /// `new` lands just before it, so the caller gets
    /// `EpochConflict { current: Some(new) }`.
    pub fn lose_next_race(&self) {
        let _operations_paused = lock(&self.shared.current);
        self.faults().lose_next_race = true;
    }

    /// Holds this handle's next `kind` call: the calling thread blocks until
    /// [`Self::release`], and the call then proceeds against the authority as
    /// it is at release time. Holding never affects other handles, nor other
    /// kinds of call on this one.
    pub fn hold_next(&self, kind: CallKind) {
        lock(&self.gate.slots).entry(kind).or_default().armed = true;
    }

    /// Releases every call held by [`Self::hold_next`] for `kind` on this
    /// handle. A hold that no call has reached yet is cancelled.
    pub fn release(&self, kind: CallKind) {
        let mut slots = lock(&self.gate.slots);
        let slot = slots.entry(kind).or_default();
        slot.armed = false;
        slot.holding = 0;
        slot.releases += 1;
        self.gate.released.notify_all();
    }

    /// True while a call of `kind` is held on this handle, so a test can wait
    /// for this before acting "during" the call.
    pub fn is_holding(&self, kind: CallKind) -> bool {
        lock(&self.gate.slots)
            .get(&kind)
            .is_some_and(|slot| slot.holding > 0)
    }

    /// Blocks while a hold on `kind` catches this call, before it reaches
    /// the authority.
    fn pass_gate(&self, kind: CallKind) {
        let mut slots = lock(&self.gate.slots);
        let slot = slots.entry(kind).or_default();
        if !std::mem::take(&mut slot.armed) {
            return;
        }
        slot.holding += 1;
        let releases = slot.releases;
        while slots[&kind].releases == releases {
            slots = self
                .gate
                .released
                .wait(slots)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Wipes the authority for every handle, like Redis `FLUSHALL` (README
    /// §15.3): all epochs, registrations and fences are gone, and warm-up
    /// starts again from the clock's current reading. Each handle's faults,
    /// and whether the authority is down, are kept.
    pub fn flush(&self) {
        let fresh = InMemoryAuthority::new(self.shared.clock.clone(), self.shared.ttl);
        *lock(&self.shared.current) = fresh;
    }

    fn faults(&self) -> MutexGuard<'_, Faults> {
        lock(&self.faults)
    }

    /// The authority, or `Unavailable` while this handle is unreachable or
    /// the authority is down. The caller keeps the authority locked until it
    /// is done, so no fault can change under it.
    fn reach(&self) -> Result<MutexGuard<'_, InMemoryAuthority<C>>, AuthorityError> {
        let authority = lock(&self.shared.current);
        let unreachable = self.faults().unreachable;
        let down = lock(&self.shared.availability).down;
        if unreachable || down {
            return Err(AuthorityError::Unavailable);
        }
        Ok(authority)
    }

    /// Whether less than one TTL has passed since the authority last came
    /// back from an outage.
    fn warming_up_after_outage(&self) -> bool {
        lock(&self.shared.availability)
            .back_up_at
            .is_some_and(|back_up_at| self.shared.clock.now() - back_up_at < self.shared.ttl)
    }
}

/// A test that panicked while holding a lock has already failed; the data
/// behind the lock is still consistent, so later calls carry on with it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<C: Clock + Clone> CoordinationAuthority for FaultingAuthority<C> {
    fn register(
        &self,
        shard_id: &ShardId,
        worker_id: &WorkerId,
        address: &str,
    ) -> Result<Duration, AuthorityError> {
        self.pass_gate(CallKind::Register);
        self.reach()?.register(shard_id, worker_id, address)
    }

    fn live_registrations(&self, shard_id: &ShardId) -> Result<LiveRegistrations, AuthorityError> {
        self.pass_gate(CallKind::LiveRegistrations);
        let authority = self.reach()?;
        let live = authority.live_registrations(shard_id)?;
        // The in-memory authority below warms up only from its construction
        // or a flush, so the warm-up after an outage is applied here.
        if self.warming_up_after_outage() {
            return Ok(LiveRegistrations::new(live.addresses().clone(), false));
        }
        Ok(live)
    }

    fn read_recovery_epoch(
        &self,
        shard_id: &ShardId,
    ) -> Result<Option<RecoveryEpoch>, AuthorityError> {
        self.pass_gate(CallKind::ReadRecoveryEpoch);
        self.reach()?.read_recovery_epoch(shard_id)
    }

    fn compare_and_swap_recovery_epoch(
        &self,
        shard_id: &ShardId,
        expected: Option<RecoveryEpoch>,
        new: RecoveryEpoch,
    ) -> Result<(), AuthorityError> {
        self.pass_gate(CallKind::SwapRecoveryEpoch);
        let authority = self.reach()?;
        let loses_race = std::mem::take(&mut self.faults().lose_next_race);
        if loses_race {
            // The rival's own outcome does not matter: if it fails, so does
            // the caller's identical swap, for the same reason.
            let _ = authority.compare_and_swap_recovery_epoch(shard_id, expected, new);
        }
        authority.compare_and_swap_recovery_epoch(shard_id, expected, new)
    }

    fn acquire_fence(
        &self,
        shard_id: &ShardId,
        holder: &WorkerId,
        recovery_epoch: RecoveryEpoch,
    ) -> Result<Duration, AuthorityError> {
        self.pass_gate(CallKind::AcquireFence);
        self.reach()?
            .acquire_fence(shard_id, holder, recovery_epoch)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    use kabudachi_core::time::Instant;

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

    /// Epoch `number` of the one lineage these tests use.
    fn epoch(number: u64) -> RecoveryEpoch {
        RecoveryEpoch::new(number, 7)
    }

    fn shard() -> ShardId {
        ShardId::new("shard-1")
    }

    fn worker(label: &str) -> WorkerId {
        WorkerId::new(label)
    }

    /// A warmed-up authority holding epoch 3, a registration for `a` and
    /// `a`'s fence, seen through the returned handle.
    fn seeded_authority() -> (FaultingAuthority<TestClock>, TestClock) {
        let clock = TestClock::default();
        let authority = FaultingAuthority::new(clock.clone(), ttl());
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
        (authority, clock)
    }

    fn assert_seeded_state(authority: &FaultingAuthority<TestClock>) {
        assert_eq!(authority.read_recovery_epoch(&shard()), Ok(Some(epoch(3))));
        assert_eq!(
            authority.live_registrations(&shard()),
            Ok(LiveRegistrations::new(
                BTreeMap::from([(worker("a"), "addr-a".to_string())]),
                true
            ))
        );
        assert_eq!(
            authority.acquire_fence(&shard(), &worker("b"), epoch(3)),
            Err(AuthorityError::FenceHeld { remaining: ttl() })
        );
    }

    fn assert_every_operation_is_unavailable(authority: &FaultingAuthority<TestClock>) {
        let unavailable = Err(AuthorityError::Unavailable);
        assert_eq!(
            authority.register(&shard(), &worker("b"), "addr-b"),
            unavailable
        );
        assert_eq!(
            authority.live_registrations(&shard()),
            Err(AuthorityError::Unavailable)
        );
        assert_eq!(
            authority.read_recovery_epoch(&shard()),
            Err(AuthorityError::Unavailable)
        );
        assert_eq!(
            authority.compare_and_swap_recovery_epoch(&shard(), Some(epoch(3)), epoch(4)),
            Err(AuthorityError::Unavailable)
        );
        assert_eq!(
            authority.acquire_fence(&shard(), &worker("a"), epoch(3)),
            unavailable
        );
    }

    #[test]
    fn becoming_reachable_again_finds_the_state_unchanged() {
        let (authority, _clock) = seeded_authority();
        authority.set_reachable(false);
        assert_every_operation_is_unavailable(&authority);

        authority.set_reachable(true);

        assert_seeded_state(&authority);
    }

    #[test]
    fn another_workers_handle_shares_the_state_but_not_the_faults() {
        let (authority, _clock) = seeded_authority();
        let other = authority.for_another_worker();

        authority.set_reachable(false);
        authority.lose_next_race();

        assert_seeded_state(&other);
        assert_eq!(
            other.compare_and_swap_recovery_epoch(&shard(), Some(epoch(3)), epoch(4)),
            Ok(()),
            "the other handle does not lose the race armed on the first"
        );
    }

    #[test]
    fn a_clone_shares_the_faults() {
        let (authority, _clock) = seeded_authority();
        let clone = authority.clone();

        clone.set_reachable(false);

        assert_every_operation_is_unavailable(&authority);
    }

    #[test]
    fn lose_next_race_loses_exactly_one_compare_and_swap() {
        let (authority, _clock) = seeded_authority();

        authority.lose_next_race();

        assert_eq!(
            authority.compare_and_swap_recovery_epoch(&shard(), Some(epoch(3)), epoch(4)),
            Err(AuthorityError::EpochConflict {
                current: Some(epoch(4))
            }),
            "a rival swapped 3 for 4 first"
        );
        assert_eq!(authority.read_recovery_epoch(&shard()), Ok(Some(epoch(4))));
        assert_eq!(
            authority.compare_and_swap_recovery_epoch(&shard(), Some(epoch(4)), epoch(5)),
            Ok(()),
            "only the next compare-and-swap loses"
        );
    }

    #[test]
    fn an_armed_race_waits_for_a_compare_and_swap_that_reaches_the_authority() {
        let (authority, _clock) = seeded_authority();
        authority.set_reachable(false);
        authority.lose_next_race();

        assert_eq!(
            authority.compare_and_swap_recovery_epoch(&shard(), Some(epoch(3)), epoch(4)),
            Err(AuthorityError::Unavailable)
        );

        authority.set_reachable(true);
        assert_eq!(
            authority.compare_and_swap_recovery_epoch(&shard(), Some(epoch(3)), epoch(4)),
            Err(AuthorityError::EpochConflict {
                current: Some(epoch(4))
            }),
            "the race armed while unreachable is lost by the first swap that gets through"
        );
    }

    #[test]
    fn flush_empties_every_handles_state_and_restarts_warm_up() {
        let (authority, clock) = seeded_authority();
        let other = authority.for_another_worker();

        authority.flush();

        assert_eq!(other.read_recovery_epoch(&shard()), Ok(None));
        assert_eq!(
            other.live_registrations(&shard()),
            Ok(LiveRegistrations::new(BTreeMap::new(), false)),
            "no registrations, and warming up again"
        );
        other
            .compare_and_swap_recovery_epoch(&shard(), None, epoch(0))
            .expect("the epoch is missing, so create-if-absent succeeds");
        // The flush lost `a`'s fence, so the authority cannot make a new
        // holder wait it out: it grants no fence until warm-up ends.
        assert_eq!(
            other.acquire_fence(&shard(), &worker("b"), epoch(0)),
            Err(AuthorityError::FenceHeld { remaining: ttl() }),
            "no fence during warm-up"
        );

        clock.advance(ttl());
        assert_eq!(
            other
                .live_registrations(&shard())
                .unwrap()
                .authoritative_count(),
            Some(0),
            "warm-up ends one TTL after the flush"
        );
        assert_eq!(
            other.acquire_fence(&shard(), &worker("b"), epoch(0)),
            Ok(ttl()),
            "a fence is granted once warm-up ends"
        );
    }

    #[test]
    fn while_the_authority_is_down_every_handle_gets_unavailable() {
        let (authority, _clock) = seeded_authority();
        let other = authority.for_another_worker();

        other.set_available(false);

        assert_every_operation_is_unavailable(&authority);
        assert_every_operation_is_unavailable(&other);
        assert_every_operation_is_unavailable(&authority.for_another_worker());
    }

    #[test]
    fn after_an_outage_there_is_no_authoritative_count_until_one_ttl_after_it_ends() {
        let (authority, clock) = seeded_authority();
        let other = authority.for_another_worker();
        authority.set_available(false);
        clock.advance(millis(2 * TTL_MILLIS));
        authority.set_available(true);

        clock.advance(millis(TTL_MILLIS - 1));
        other
            .register(&shard(), &worker("a"), "addr-a")
            .expect("the authority is back");
        let warming_up = other.live_registrations(&shard()).unwrap();
        assert_eq!(
            warming_up.authoritative_count(),
            None,
            "one TTL has not yet passed since the authority came back"
        );
        assert_eq!(
            warming_up.addresses().len(),
            1,
            "registrations are reported during warm-up, just not as an authoritative count"
        );

        clock.advance(millis(1));
        assert_eq!(
            other
                .live_registrations(&shard())
                .unwrap()
                .authoritative_count(),
            Some(1),
            "warm-up ends one TTL after the authority came back"
        );
    }

    #[test]
    fn epochs_and_fences_survive_an_outage() {
        let (authority, clock) = seeded_authority();
        authority.set_available(false);
        clock.advance(millis(40));
        authority.set_available(true);

        assert_eq!(authority.read_recovery_epoch(&shard()), Ok(Some(epoch(3))));
        assert_eq!(
            authority.acquire_fence(&shard(), &worker("b"), epoch(3)),
            Err(AuthorityError::FenceHeld {
                remaining: millis(TTL_MILLIS - 40)
            }),
            "a still holds the fence it took before the outage"
        );
    }

    #[test]
    fn making_an_available_authority_available_does_not_restart_warm_up() {
        let (authority, _clock) = seeded_authority();

        authority.set_available(true);

        assert_seeded_state(&authority);
    }

    /// Waits, on a short real-time backstop, until `authority` holds a call
    /// of `kind`.
    fn wait_until_holding(authority: &FaultingAuthority<TestClock>, kind: CallKind) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !authority.is_holding(kind) {
            assert!(
                std::time::Instant::now() < deadline,
                "no {kind:?} call was held"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn a_held_call_blocks_only_itself() {
        let (authority, _clock) = seeded_authority();
        let other = authority.for_another_worker();
        authority.hold_next(CallKind::Register);
        let held = {
            let authority = authority.clone();
            std::thread::spawn(move || authority.register(&shard(), &worker("b"), "addr-b"))
        };
        wait_until_holding(&authority, CallKind::Register);

        assert!(
            authority.live_registrations(&shard()).is_ok(),
            "another kind of call on the same handle is not held up"
        );
        assert_eq!(
            other.register(&shard(), &worker("c"), "addr-c").map(|_| ()),
            Ok(()),
            "another handle's call of the same kind is not held up"
        );
        assert_eq!(
            authority
                .register(&shard(), &worker("d"), "addr-d")
                .map(|_| ()),
            Ok(()),
            "a second call of the same kind on the same handle is not held: the hold catches one call"
        );
        assert_eq!(
            authority
                .live_registrations(&shard())
                .unwrap()
                .addresses()
                .len(),
            3,
            "the held registration has not reached the authority"
        );

        authority.release(CallKind::Register);
        assert_eq!(held.join().unwrap().map(|_| ()), Ok(()));
        assert!(!authority.is_holding(CallKind::Register));
        assert_eq!(
            authority
                .live_registrations(&shard())
                .unwrap()
                .addresses()
                .len(),
            4
        );
    }

    #[test]
    fn a_released_call_sees_the_authority_as_it_is_at_release() {
        let fresh = FaultingAuthority::new(TestClock::default(), ttl());
        let held_handle = fresh.clone();
        let rival = fresh.for_another_worker();
        held_handle.hold_next(CallKind::SwapRecoveryEpoch);
        let held = std::thread::spawn(move || {
            held_handle.compare_and_swap_recovery_epoch(&shard(), None, epoch(0))
        });
        wait_until_holding(&fresh, CallKind::SwapRecoveryEpoch);

        rival
            .compare_and_swap_recovery_epoch(&shard(), None, epoch(0))
            .expect("the rival creates the epoch while the first swap is held");
        fresh.release(CallKind::SwapRecoveryEpoch);

        assert_eq!(
            held.join().unwrap(),
            Err(AuthorityError::EpochConflict {
                current: Some(epoch(0))
            })
        );
    }

}
