//! When finished tasks are forgotten: a result TTL after they finished, or
//! never.

use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::ids::TaskId;
use crate::time::{Duration, Instant};

/// When finished tasks are forgotten: `ttl` after they finished, or never.
#[derive(Debug, Default)]
pub(super) struct Retention {
    /// How long a finished task is kept. `None` keeps every task forever.
    ttl: Option<Duration>,
    /// Finished tasks by when they finished, so only the due ones are visited.
    finished: BTreeSet<(Instant, TaskId)>,
    /// The same tasks, for asking whether one is over.
    over: BTreeSet<TaskId>,
    /// Tasks that are over but kept regardless of their TTL, with when they
    /// finished: members of a composite that is still held, whose results are
    /// read through it. Each joins `finished` when it is released.
    held: BTreeMap<TaskId, Instant>,
}

impl Retention {
    pub(super) fn set_ttl(&mut self, ttl: Option<Duration>) {
        self.ttl = ttl;
    }

    /// How long a finished task is kept, or `None` for ever.
    pub(super) fn ttl(&self) -> Option<Duration> {
        self.ttl
    }

    /// Forgets every task it holds, keeping the TTL.
    pub(super) fn clear(&mut self) {
        self.finished.clear();
        self.over.clear();
        self.held.clear();
    }

    /// `task` is over as of `now`.
    pub(super) fn record(&mut self, task: &TaskId, now: Instant) {
        self.finished.insert((now, task.clone()));
        self.over.insert(task.clone());
    }

    /// `task` is over as of `now`, and kept until [`Self::release`] however
    /// long ago that was.
    pub(super) fn hold(&mut self, task: &TaskId, now: Instant) {
        self.held.insert(task.clone(), now);
        self.over.insert(task.clone());
    }

    /// Whether `task` is over and kept until released.
    pub(super) fn is_held(&self, task: &TaskId) -> bool {
        self.held.contains_key(task)
    }

    /// The tasks that are over and kept until released.
    pub(super) fn held(&self) -> Vec<TaskId> {
        self.held.keys().cloned().collect()
    }

    /// `task` is no longer kept regardless of its TTL: it is forgotten
    /// `ttl` after it finished, which may be now.
    pub(super) fn release(&mut self, task: &TaskId) {
        if let Some(finished_at) = self.held.remove(task) {
            self.finished.insert((finished_at, task.clone()));
        }
    }

    /// Whether `task` is over and not yet forgotten.
    pub(super) fn holds(&self, task: &TaskId) -> bool {
        self.over.contains(task)
    }

    /// Removes and returns every task due to be forgotten by `now`, earliest
    /// first; none while no TTL is set.
    pub(super) fn take_due(&mut self, now: Instant) -> Vec<TaskId> {
        let Some(ttl) = self.ttl else {
            return Vec::new();
        };
        let mut due = Vec::new();
        while let Some((finished_at, _)) = self.finished.first() {
            if now - *finished_at < ttl {
                break;
            }
            let (_, task) = self.finished.pop_first().expect("just seen");
            self.over.remove(&task);
            due.push(task);
        }
        due
    }

    /// When the next task is due to be forgotten; `None` while no TTL is set.
    pub(super) fn next_due(&self) -> Option<Instant> {
        let ttl = self.ttl?;
        self.finished.first().map(|(finished_at, _)| *finished_at + ttl)
    }
}
