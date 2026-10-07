//! When finished tasks are forgotten: a result TTL after they finished, or
//! never.

use std::collections::BTreeSet;

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
}

impl Retention {
    pub(super) fn set_ttl(&mut self, ttl: Option<Duration>) {
        self.ttl = ttl;
    }

    /// `task` is over as of `now`.
    pub(super) fn record(&mut self, task: &TaskId, now: Instant) {
        self.finished.insert((now, task.clone()));
        self.over.insert(task.clone());
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
