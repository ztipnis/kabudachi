//! A `CoordinationAuthority` for tests that need the authority to fail, for
//! one worker or for all of them, and whose calls a test can hold or make
//! panic.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use kabudachi_core::coordination_authority::{
    AuthorityError, CoordinationAuthority, LiveRegistrations, RecoveryEpoch,
};
use kabudachi_core::election::CallKind;
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
    /// The next call of this kind will panic, after any hold.
    panics: bool,
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

    /// Makes this handle's next `kind` call panic, after any hold on it and
    /// before it reaches the authority. No lock is held while it panics, so
    /// nothing another handle waits on is poisoned.
    pub fn panic_next(&self, kind: CallKind) {
        lock(&self.gate.slots).entry(kind).or_default().panics = true;
    }

    /// True while a call of `kind` is held on this handle, so a test can wait
    /// for this before acting "during" the call.
    pub fn is_holding(&self, kind: CallKind) -> bool {
        lock(&self.gate.slots)
            .get(&kind)
            .is_some_and(|slot| slot.holding > 0)
    }

    /// Blocks while a hold on `kind` catches this call, then panics if
    /// [`Self::panic_next`] asked, all before the call reaches the authority.
    fn pass_gate(&self, kind: CallKind) {
        let mut slots = lock(&self.gate.slots);
        let slot = slots.entry(kind).or_default();
        if std::mem::take(&mut slot.armed) {
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
        let panics = std::mem::take(
            &mut slots
                .get_mut(&kind)
                .expect("the slot was made above")
                .panics,
        );
        drop(slots);
        if panics {
            panic!("a {kind:?} call panics, as FaultingAuthority::panic_next asked");
        }
    }

    /// Wipes the authority for every handle, like Redis `FLUSHALL`: all epochs,
    /// registrations and fences are gone, and warm-up starts again from the
    /// clock's current reading. Each handle's faults, and whether the authority
    /// is down, are kept.
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
        self.pass_gate(CallKind::ReadLiveRegistrations);
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
