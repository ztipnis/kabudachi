use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::ElectionMessage;
use kabudachi_core::time::{Clock, Duration, Instant};
use kabudachi_core::transport::PeerMessenger;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::support::clock::FakeClock;

struct ScheduledMessage {
    deliver_at: Instant,
    from: WorkerId,
    to: WorkerId,
    message: ElectionMessage,
}

struct Inner {
    clock: Rc<FakeClock>,
    registered: BTreeSet<WorkerId>,
    inboxes: BTreeMap<WorkerId, VecDeque<(WorkerId, ElectionMessage)>>,
    scheduled: Vec<ScheduledMessage>,
    drop_rate: f64,
    duplicate_rate: f64,
    reorder: bool,
    delay: Duration,
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

/// A shared, fully synchronous simulated network. Faults come from a seeded
/// ChaCha8 PRNG, so runs are reproducible. `Clone` shares state: clones see each
/// other's changes.
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
                inboxes: BTreeMap::new(),
                scheduled: Vec::new(),
                drop_rate: 0.0,
                duplicate_rate: 0.0,
                reorder: false,
                delay: Duration::from_ticks(0),
                rng: ChaCha8Rng::seed_from_u64(0),
                partition: None,
            })),
        }
    }

    /// Must be called once per worker before it sends, receives or counts in `reachable_peers`.
    pub fn register(&self, worker_id: WorkerId) {
        let mut inner = self.inner.borrow_mut();
        inner.inboxes.entry(worker_id.clone()).or_default();
        inner.registered.insert(worker_id);
    }

    /// Fraction of later sends that are dropped (`0.0..=1.0`).
    pub fn set_drop_rate(&self, rate: f64) {
        self.inner.borrow_mut().drop_rate = rate;
    }

    /// Fraction of later sends also delivered a second time; a dropped message is never duplicated.
    pub fn set_duplicate_rate(&self, rate: f64) {
        self.inner.borrow_mut().duplicate_rate = rate;
    }

    /// Whether messages due in the same `pump()` are delivered in shuffled order.
    pub fn set_reorder(&self, enabled: bool) {
        self.inner.borrow_mut().reorder = enabled;
    }

    /// Delay applied to later sends.
    pub fn set_delay(&self, delay: Duration) {
        self.inner.borrow_mut().delay = delay;
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

    /// Moves every message due by now into its recipient's inbox; returns how many.
    pub fn pump(&self) -> usize {
        let mut inner = self.inner.borrow_mut();
        let now = inner.clock.now();

        let mut due = Vec::new();
        let mut still_pending = Vec::new();
        for scheduled in inner.scheduled.drain(..) {
            if scheduled.deliver_at <= now {
                due.push(scheduled);
            } else {
                still_pending.push(scheduled);
            }
        }
        inner.scheduled = still_pending;

        if inner.reorder {
            due.shuffle(&mut inner.rng);
        }

        let count = due.len();
        for scheduled in due {
            inner
                .inboxes
                .entry(scheduled.to)
                .or_default()
                .push_back((scheduled.from, scheduled.message));
        }
        count
    }
}

impl FakeNetwork {
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

impl PeerMessenger for FakeNetwork {
    fn send(&self, from: WorkerId, to: WorkerId, message: ElectionMessage) {
        let mut inner = self.inner.borrow_mut();

        inner.assert_registered(&from, "send (from)");
        inner.assert_registered(&to, "send (to)");

        if inner.is_partitioned(&from, &to) {
            return;
        }

        if inner.should_drop() {
            return;
        }

        let deliver_at = inner.clock.now() + inner.delay;
        inner.scheduled.push(ScheduledMessage {
            deliver_at,
            from: from.clone(),
            to: to.clone(),
            message: message.clone(),
        });

        if inner.should_duplicate() {
            inner.scheduled.push(ScheduledMessage {
                deliver_at,
                from,
                to,
                message,
            });
        }
    }

    fn poll_inbox(&self, me: WorkerId) -> Vec<(WorkerId, ElectionMessage)> {
        let mut inner = self.inner.borrow_mut();
        inner.assert_registered(&me, "poll_inbox");
        inner.inboxes.entry(me).or_default().drain(..).collect()
    }

    fn reachable_peers(&self, me: WorkerId) -> BTreeSet<WorkerId> {
        let inner = self.inner.borrow();
        let mut peers = inner.registered.clone();
        peers.remove(&me);

        if let Some((group_a, group_b)) = &inner.partition {
            if group_a.contains(&me) {
                for other in group_b {
                    peers.remove(other);
                }
            } else if group_b.contains(&me) {
                for other in group_a {
                    peers.remove(other);
                }
            }
        }

        peers
    }
}
