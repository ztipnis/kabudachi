//! A `CoordinationAuthority` for tests that need the authority to fail, for
//! one worker or for all of them, and whose calls a test can hold or make
//! panic.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use kabudachi_core::coordination_authority::{
    AuthorityError, CoordinationAuthority, LeaderHint, LiveRegistrations, ShardRecord,
};
use kabudachi_core::election::CallKind;
use kabudachi_core::in_memory_authority::InMemoryAuthority;
use kabudachi_core::protocol::ids::{ShardId, ShardName, WorkerId};
use kabudachi_core::time::{Clock, Duration};

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
    /// Tells this handle apart from its clones at the gate.
    handle: u64,
}

/// One connection's held calls. Kept apart from [`Faults`] and taken before
/// any authority lock, and never while one is held: a held call waits here,
/// so it must not stop any other call, on this handle or another.
#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
    released: Condvar,
}

/// The holds on one connection, and the handles that could release them.
#[derive(Default)]
struct GateState {
    slots: BTreeMap<CallKind, Slot>,
    /// The connection's handles alive: the one it was made with and every
    /// clone of it.
    handles: usize,
    /// The handles that have a call held, each with how many.
    holding_handles: BTreeMap<u64, u32>,
}

impl GateState {
    /// Whether some handle has no call held, and so could still release one.
    /// A node's client calls through one handle, and its held call blocks a
    /// thread its runtime waits for when it shuts down: once the test has
    /// dropped every handle of its own, nothing can release that call, and
    /// the test would never end.
    fn may_be_released(&self) -> bool {
        self.handles > self.holding_handles.len()
    }
}

/// A fresh id for each handle.
fn next_handle() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// A gate with one handle.
fn new_gate() -> Arc<Gate> {
    let gate = Gate::default();
    lock(&gate.state).handles = 1;
    Arc::new(gate)
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

/// What every handle reaches.
///
/// Lock order: `authority` first, then `down` or a handle's `faults`, one
/// at a time. An operation reads the faults and the outage while it holds
/// `authority`, and every switch that changes them takes `authority`
/// first. So a switch cannot land between an operation's check and its
/// action: once `set_available(false)` returns, no operation reaches the
/// authority, and once `set_reachable(false)` returns, none through that
/// handle does.
struct SharedAuthority<C> {
    authority: Mutex<InMemoryAuthority<C>>,
    /// Whether the whole authority is down.
    down: Mutex<bool>,
}

/// One connection's faults. The default is a healthy connection.
#[derive(Default)]
struct Faults {
    unreachable: bool,
    lose_next_race: bool,
}

impl<C> Clone for FaultingAuthority<C> {
    fn clone(&self) -> Self {
        lock(&self.gate.state).handles += 1;
        Self {
            shared: Arc::clone(&self.shared),
            faults: Arc::clone(&self.faults),
            gate: Arc::clone(&self.gate),
            handle: next_handle(),
        }
    }
}

/// Dropping the last handle that could release a held call lets every held
/// call on the connection go (see [`FaultingAuthority::hold_next`]).
impl<C> Drop for FaultingAuthority<C> {
    fn drop(&mut self) {
        lock(&self.gate.state).handles -= 1;
        self.gate.released.notify_all();
    }
}

impl<C: Clock + Clone> FaultingAuthority<C> {
    /// A handle over a new authority whose registrations, fences and warm-up
    /// last `ttl` on `clock`. It starts warming up at `clock`'s current
    /// reading.
    pub fn new(clock: C, ttl: Duration) -> Self {
        Self {
            shared: Arc::new(SharedAuthority {
                authority: Mutex::new(InMemoryAuthority::new(clock, ttl)),
                down: Mutex::new(false),
            }),
            faults: Arc::new(Mutex::new(Faults::default())),
            gate: new_gate(),
            handle: next_handle(),
        }
    }

    /// A handle over the same authority for a different worker, with no
    /// faults of its own.
    pub fn for_another_worker(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            faults: Arc::new(Mutex::new(Faults::default())),
            gate: new_gate(),
            handle: next_handle(),
        }
    }

    /// While unreachable, every operation through this handle (and its
    /// clones) returns `Unavailable` and leaves the authority untouched.
    pub fn set_reachable(&self, reachable: bool) {
        let _operations_paused = lock(&self.shared.authority);
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
        let authority = lock(&self.shared.authority);
        let mut down = lock(&self.shared.down);
        if !available {
            *down = true;
        } else if *down {
            *down = false;
            authority.back_from_outage();
        }
    }

    /// Makes this handle's next compare-and-swap that reaches the authority
    /// lose a race: a rival compare-and-swap with the same `expected` and
    /// `new` lands just before it, so the caller gets
    /// `ShardConflict { current: Some(new) }`.
    pub fn lose_next_race(&self) {
        let _operations_paused = lock(&self.shared.authority);
        self.faults().lose_next_race = true;
    }

    /// Holds this handle's next `kind` call: the calling thread blocks until
    /// [`Self::release`], and the call then proceeds against the authority as
    /// it is at release time. Holding never affects other handles, nor other
    /// kinds of call on this one.
    ///
    /// A held call also proceeds once every handle of this connection that
    /// is left has a call of its own held: none is left that could release
    /// it. So a test that ends, or panics, before releasing a call its node
    /// made does not leave that call's thread blocked for ever.
    pub fn hold_next(&self, kind: CallKind) {
        lock(&self.gate.state).slots.entry(kind).or_default().armed = true;
    }

    /// Releases every call held by [`Self::hold_next`] for `kind` on this
    /// handle. A hold that no call has reached yet is cancelled.
    pub fn release(&self, kind: CallKind) {
        let mut state = lock(&self.gate.state);
        let slot = state.slots.entry(kind).or_default();
        slot.armed = false;
        slot.holding = 0;
        slot.releases += 1;
        self.gate.released.notify_all();
    }

    /// Makes this handle's next `kind` call panic, after any hold on it and
    /// before it reaches the authority. No lock is held while it panics, so
    /// nothing another handle waits on is poisoned.
    pub fn panic_next(&self, kind: CallKind) {
        lock(&self.gate.state).slots.entry(kind).or_default().panics = true;
    }

    /// True while a call of `kind` is held on this handle, so a test can wait
    /// for this before acting "during" the call.
    pub fn is_holding(&self, kind: CallKind) -> bool {
        lock(&self.gate.state)
            .slots
            .get(&kind)
            .is_some_and(|slot| slot.holding > 0)
    }

    /// Blocks while a hold on `kind` catches this call, then panics if
    /// [`Self::panic_next`] asked, all before the call reaches the authority.
    fn pass_gate(&self, kind: CallKind) {
        let mut state = lock(&self.gate.state);
        let slot = state.slots.entry(kind).or_default();
        if std::mem::take(&mut slot.armed) {
            slot.holding += 1;
            let releases = slot.releases;
            *state.holding_handles.entry(self.handle).or_default() += 1;
            // Another held call may have just lost the last handle that could
            // release it.
            self.gate.released.notify_all();
            while state.slots[&kind].releases == releases && state.may_be_released() {
                state = self
                    .gate
                    .released
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            if state.slots[&kind].releases == releases {
                // Let go for want of a handle to release it: no release
                // cleared its count.
                let slot = state.slots.get_mut(&kind).expect("the slot was made above");
                slot.holding -= 1;
            }
            let held = state
                .holding_handles
                .get_mut(&self.handle)
                .expect("counted above");
            *held -= 1;
            if *held == 0 {
                state.holding_handles.remove(&self.handle);
            }
        }
        let panics = std::mem::take(
            &mut state
                .slots
                .get_mut(&kind)
                .expect("the slot was made above")
                .panics,
        );
        drop(state);
        if panics {
            panic!("a {kind:?} call panics, as FaultingAuthority::panic_next asked");
        }
    }

    /// Wipes the authority for every handle, like Redis `FLUSHALL`: all records,
    /// registrations, fences and hints are gone, and warm-up starts again from the
    /// clock's current reading. Each handle's faults, and whether the authority
    /// is down, are kept.
    pub fn flush(&self) {
        lock(&self.shared.authority).flush();
    }

    fn faults(&self) -> MutexGuard<'_, Faults> {
        lock(&self.faults)
    }

    /// The authority, or `Unavailable` while this handle is unreachable or
    /// the authority is down. The caller keeps the authority locked until it
    /// is done, so no fault can change under it.
    fn reach(&self) -> Result<MutexGuard<'_, InMemoryAuthority<C>>, AuthorityError> {
        let authority = lock(&self.shared.authority);
        let unreachable = self.faults().unreachable;
        let down = *lock(&self.shared.down);
        if unreachable || down {
            return Err(AuthorityError::Unavailable);
        }
        Ok(authority)
    }
}

/// A test that panicked while holding a lock has already failed; the data
/// behind the lock is still consistent, so later calls carry on with it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<C: Clock + Clone> CoordinationAuthority for FaultingAuthority<C> {
    /// The TTL is configuration, not a call: it answers while the authority
    /// is down or this handle is cut off.
    fn ttl(&self) -> Duration {
        lock(&self.shared.authority).ttl()
    }

    fn register(
        &self,
        name: &ShardName,
        shard_id: &ShardId,
        worker_id: &WorkerId,
        address: &str,
    ) -> Result<Duration, AuthorityError> {
        self.pass_gate(CallKind::Register);
        self.reach()?.register(name, shard_id, worker_id, address)
    }

    fn live_registrations(
        &self,
        name: &ShardName,
        shard_id: &ShardId,
    ) -> Result<LiveRegistrations, AuthorityError> {
        self.pass_gate(CallKind::ReadLiveRegistrations);
        self.reach()?.live_registrations(name, shard_id)
    }

    fn read_shard(&self, name: &ShardName) -> Result<Option<ShardRecord>, AuthorityError> {
        self.pass_gate(CallKind::ReadRecoveryEpoch);
        self.reach()?.read_shard(name)
    }

    fn compare_and_swap_shard(
        &self,
        name: &ShardName,
        expected: Option<&ShardRecord>,
        new: &ShardRecord,
    ) -> Result<(), AuthorityError> {
        self.pass_gate(CallKind::SwapRecoveryEpoch);
        let authority = self.reach()?;
        let loses_race = std::mem::take(&mut self.faults().lose_next_race);
        if loses_race {
            // The rival's own outcome does not matter: if it fails, so does
            // the caller's identical swap, for the same reason.
            let _ = authority.compare_and_swap_shard(name, expected, new);
        }
        authority.compare_and_swap_shard(name, expected, new)
    }

    fn acquire_fence(
        &self,
        name: &ShardName,
        holder: &WorkerId,
        record: &ShardRecord,
    ) -> Result<Duration, AuthorityError> {
        self.pass_gate(CallKind::AcquireFence);
        self.reach()?.acquire_fence(name, holder, record)
    }

    fn publish_leader_hint(
        &self,
        name: &ShardName,
        hint: &LeaderHint,
    ) -> Result<(), AuthorityError> {
        self.pass_gate(CallKind::PublishLeaderHint);
        self.reach()?.publish_leader_hint(name, hint)
    }

    fn read_leader_hint(&self, name: &ShardName) -> Result<Option<LeaderHint>, AuthorityError> {
        self.pass_gate(CallKind::ReadLeaderHint);
        self.reach()?.read_leader_hint(name)
    }
}
