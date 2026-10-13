//! Queues: the options a leader orders and limits the work waiting in each
//! queue by, and which queues a worker takes work from.
//!
//! Every worker of a shard should give the same options. The leader's are the
//! ones that decide which task a claim gets and whether a queue is full; a
//! worker's own only order the work it looks at first.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use crate::time::Duration;

/// The queue a task goes to, and a worker takes from, unless told otherwise.
pub const DEFAULT_QUEUE: &str = "default";

/// How a leader treats one queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueOptions {
    /// Higher goes first: a claim takes from the highest priority with work
    /// waiting.
    pub priority: i32,
    /// Among queues of one priority, how many claims this queue gets for each
    /// claim a queue of weight one gets.
    pub weight: NonZeroU32,
    /// The most tasks of this queue claimed or running at once; `None` for no
    /// limit.
    pub max_running: Option<NonZeroU32>,
    /// How long a task may wait here before it goes ahead of every priority;
    /// `None` never.
    pub starvation_age: Option<Duration>,
}

impl QueueOptions {
    /// Priority 0, weight 1, no limit, no aging.
    pub const DEFAULT: QueueOptions = QueueOptions {
        priority: 0,
        weight: NonZeroU32::MIN,
        max_running: None,
        starvation_age: None,
    };
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The options of every queue that has its own; any other queue has
/// [`QueueOptions::DEFAULT`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueuePolicy {
    queues: BTreeMap<String, QueueOptions>,
}

impl QueuePolicy {
    /// A policy that gives every queue the default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gives `queue` its own `options`.
    #[must_use]
    pub fn with_queue(mut self, queue: impl Into<String>, options: QueueOptions) -> Self {
        self.queues.insert(queue.into(), options);
        self
    }

    /// The options of `queue`: its own, or the default when it has none.
    pub fn options(&self, queue: &str) -> QueueOptions {
        self.queues.get(queue).copied().unwrap_or_default()
    }

    /// How urgent a task of `queue` that has waited `waited` is: its queue's
    /// priority, or above every priority once it has waited longer than its
    /// queue's starvation age.
    pub fn rank(&self, queue: &str, waited: Duration) -> Rank {
        let options = self.options(queue);
        match options.starvation_age {
            Some(age) if waited.as_ticks() > age.as_ticks() => Rank::Starving,
            _ => Rank::Priority(options.priority),
        }
    }
}

/// How urgent a waiting task is. `Starving` ranks above every priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rank {
    Priority(i32),
    Starving,
}

/// The queues a worker takes work from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Subscription {
    /// Every queue.
    #[default]
    Every,
    /// These queues only.
    Only(BTreeSet<String>),
}

impl Subscription {
    /// The subscription a list of queue names gives: those queues, or every
    /// queue for an empty list (as a message that names none says).
    pub fn from_names(names: &[String]) -> Self {
        if names.is_empty() {
            Subscription::Every
        } else {
            Subscription::Only(names.iter().cloned().collect())
        }
    }

    /// The names a message carries for this subscription: none for every
    /// queue.
    pub fn names(&self) -> Vec<String> {
        match self {
            Subscription::Every => Vec::new(),
            Subscription::Only(queues) => queues.iter().cloned().collect(),
        }
    }

    /// Whether a worker with this subscription takes tasks of `queue`.
    pub fn includes(&self, queue: &str) -> bool {
        match self {
            Subscription::Every => true,
            Subscription::Only(queues) => queues.contains(queue),
        }
    }

    /// The queues both this and `asked` include.
    pub fn narrowed_to(&self, asked: &Subscription) -> Subscription {
        match (self, asked) {
            (Subscription::Every, other) | (other, Subscription::Every) => other.clone(),
            (Subscription::Only(mine), Subscription::Only(theirs)) => {
                Subscription::Only(mine.intersection(theirs).cloned().collect())
            }
        }
    }
}
