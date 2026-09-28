use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::ElectionMessage;
use kabudachi_core::time::{Clock, Duration, Instant};
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::support::clock::FakeClock;

/// A message on its way through the network.
pub struct ScheduledMessage {
    pub deliver_at: Instant,
    pub from: WorkerId,
    pub to: WorkerId,
    pub message: ElectionMessage,
}

struct Inner {
    clock: Rc<FakeClock>,
    registered: BTreeSet<WorkerId>,
    scheduled: Vec<ScheduledMessage>,
    drop_rate: f64,
    duplicate_rate: f64,
    reorder: bool,
    delay: Duration,
    /// The fraction of deliveries held back past `delay`, and the most
    /// ticks one is held back.
    late_rate: f64,
    late_by_at_most: Duration,
    rng: ChaCha8Rng,
    partition: Option<(BTreeSet<WorkerId>, BTreeSet<WorkerId>)>,
}

impl Inner {
    /// Panics if `worker_id` was never registered: a bug in the calling test, so fail fast.
    fn assert_registered(&self, worker_id: &WorkerId, context: &str) {
        assert!(
            self.registered.contains(worker_id),
            "FakeNetwork::{context}: worker {worker_id:?} was never registered — call \
             FakeNetwork::register() for it first"
        );
    }

    fn is_partitioned(&self, from: &WorkerId, to: &WorkerId) -> bool {
        match &self.partition {
            Some((group_a, group_b)) => {
                (group_a.contains(from) && group_b.contains(to))
                    || (group_b.contains(from) && group_a.contains(to))
            }
            None => false,
        }
    }

    /// Schedules one delivery of `message` after the configured delay,
    /// unless the partition or the drop rate discards it; the duplicate rate
    /// may schedule a second copy.
    fn schedule(&mut self, from: WorkerId, to: WorkerId, message: ElectionMessage) {
        if self.is_partitioned(&from, &to) || self.should_drop() {
            return;
        }

        let deliver_at = self.clock.now() + self.delay + self.lateness();
        self.scheduled.push(ScheduledMessage {
            deliver_at,
            from: from.clone(),
            to: to.clone(),
            message: message.clone(),
        });

        if self.should_duplicate() {
            self.scheduled.push(ScheduledMessage {
                deliver_at,
                from,
                to,
                message,
            });
        }
    }

    /// How much longer than `delay` one delivery takes: nothing, or for the
    /// `late_rate` share of them, 1 to `late_by_at_most` ticks, uniformly.
    fn lateness(&mut self) -> Duration {
        if self.late_by_at_most.as_ticks() == 0 || !self.chance(self.late_rate) {
            return Duration::from_ticks(0);
        }
        Duration::from_ticks(self.rng.random_range(1..=self.late_by_at_most.as_ticks()))
    }

    fn should_drop(&mut self) -> bool {
        self.chance(self.drop_rate)
    }

    fn should_duplicate(&mut self) -> bool {
        self.chance(self.duplicate_rate)
    }

    /// The rate's extremes never touch the PRNG, so a rate of 0 or 1 cannot
    /// shift the random sequence seen by other faults.
    fn chance(&mut self, rate: f64) -> bool {
        if rate <= 0.0 {
            false
        } else if rate >= 1.0 {
            true
        } else {
            self.rng.random_bool(rate)
        }
    }
}

/// A shared, fully synchronous simulated network: it holds sent messages
/// until they are due and hands them to whoever drives the nodes. Faults come
/// from a seeded ChaCha8 PRNG, so runs are reproducible. `Clone` shares state:
/// clones see each other's changes.
#[derive(Clone)]
pub struct FakeNetwork {
    inner: Rc<RefCell<Inner>>,
}

impl FakeNetwork {
    pub fn new(clock: Rc<FakeClock>) -> Self {
        Self {
            inner: Rc::new(RefCell::new(Inner {
                clock,
                registered: BTreeSet::new(),
                scheduled: Vec::new(),
                drop_rate: 0.0,
                duplicate_rate: 0.0,
                reorder: false,
                delay: Duration::from_ticks(0),
                late_rate: 0.0,
                late_by_at_most: Duration::from_ticks(0),
                rng: ChaCha8Rng::seed_from_u64(0),
                partition: None,
            })),
        }
    }

    /// Must be called once per worker before it sends, publishes or is sent to.
    pub fn register(&self, worker_id: WorkerId) {
        self.inner.borrow_mut().registered.insert(worker_id);
    }

    /// Fraction of later deliveries that are dropped (`0.0..=1.0`): each
    /// send, and each worker's copy of a publish, is dropped on its own.
    pub fn set_drop_rate(&self, rate: f64) {
        self.inner.borrow_mut().drop_rate = rate;
    }

    /// Fraction of later deliveries (sends, and each worker's copy of a
    /// publish) also delivered a second time; a dropped one is never
    /// duplicated.
    pub fn set_duplicate_rate(&self, rate: f64) {
        self.inner.borrow_mut().duplicate_rate = rate;
    }

    /// Whether messages due in the same `take_due()` come back in shuffled order.
    pub fn set_reorder(&self, enabled: bool) {
        self.inner.borrow_mut().reorder = enabled;
    }

    /// Delay applied to later deliveries, sent or published.
    pub fn set_delay(&self, delay: Duration) {
        self.inner.borrow_mut().delay = delay;
    }

    /// Holds back a `rate` share of later deliveries (`0.0..=1.0`) by 1 to
    /// `at_most` ticks past the configured delay, each drawn on its own: a
    /// message that arrives long after others sent with it, even after a
    /// partition that began since, as a real network can deliver one. A
    /// rate of 0 draws nothing from the PRNG.
    pub fn set_late_delivery(&self, rate: f64, at_most: Duration) {
        let mut inner = self.inner.borrow_mut();
        inner.late_rate = rate;
        inner.late_by_at_most = at_most;
    }

    pub fn seed(&self, seed: u64) {
        self.inner.borrow_mut().rng = ChaCha8Rng::seed_from_u64(seed);
    }

    /// Blocks delivery in both directions between the two groups. Messages
    /// sent across the cut are dropped, not queued for later.
    pub fn partition(&self, group_a: BTreeSet<WorkerId>, group_b: BTreeSet<WorkerId>) {
        self.inner.borrow_mut().partition = Some((group_a, group_b));
    }

    pub fn heal_partition(&self) {
        self.inner.borrow_mut().partition = None;
    }

    /// Puts `new` wherever the current partition has `old`: a process
    /// restarted under a new `WorkerId` keeps its host's place in the
    /// network.
    pub fn take_place_in_partition(&self, old: &WorkerId, new: &WorkerId) {
        if let Some((group_a, group_b)) = self.inner.borrow_mut().partition.as_mut() {
            for group in [group_a, group_b] {
                if group.remove(old) {
                    group.insert(new.clone());
                }
            }
        }
    }

    /// Whether the current partition separates `a` and `b`.
    pub fn is_partitioned(&self, a: &WorkerId, b: &WorkerId) -> bool {
        self.inner.borrow().is_partitioned(a, b)
    }

    /// Schedules `message` for delivery after the configured delay, unless
    /// the partition or the drop rate discards it; the duplicate rate may
    /// schedule a second copy.
    pub fn send(&self, from: WorkerId, to: WorkerId, message: ElectionMessage) {
        let mut inner = self.inner.borrow_mut();

        inner.assert_registered(&from, "send (from)");
        inner.assert_registered(&to, "send (to)");

        inner.schedule(from, to, message);
    }

    /// Sends `message` from `from` to every other registered worker, each
    /// copy on its own as `send` would: the partition, drop rate, delay,
    /// duplicate rate and reordering apply to each delivery separately. The
    /// simulator models no gossip mesh; whoever `from` can reach hears it.
    pub fn publish(&self, from: WorkerId, message: ElectionMessage) {
        let mut inner = self.inner.borrow_mut();

        inner.assert_registered(&from, "publish");

        let recipients: Vec<WorkerId> = inner
            .registered
            .iter()
            .filter(|to| **to != from)
            .cloned()
            .collect();
        for to in recipients {
            inner.schedule(from.clone(), to, message.clone());
        }
    }

    /// Removes and returns every message due by now, in the order they were
    /// sent (shuffled if reordering is on).
    pub fn take_due(&self) -> Vec<ScheduledMessage> {
        let mut inner = self.inner.borrow_mut();
        let now = inner.clock.now();

        let (mut due, still_pending): (Vec<_>, Vec<_>) = inner
            .scheduled
            .drain(..)
            .partition(|scheduled| scheduled.deliver_at <= now);
        inner.scheduled = still_pending;

        if inner.reorder {
            due.shuffle(&mut inner.rng);
        }
        due
    }

    /// When the earliest message still on its way is due, if any is.
    pub fn next_delivery_at(&self) -> Option<Instant> {
        self.inner
            .borrow()
            .scheduled
            .iter()
            .map(|scheduled| scheduled.deliver_at)
            .min()
    }

    /// Every sent-but-not-yet-delivered message with its recipient.
    pub fn pending(&self) -> Vec<(WorkerId, ElectionMessage)> {
        self.inner
            .borrow()
            .scheduled
            .iter()
            .map(|scheduled| (scheduled.to.clone(), scheduled.message.clone()))
            .collect()
    }
}
