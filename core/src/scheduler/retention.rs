//! When finished tasks are forgotten: a result TTL after they finished, the
//! retention a task chose for itself, or never.

use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::ids::TaskId;
use crate::time::{Duration, Instant};

/// When finished tasks are forgotten: the shard's `ttl` after they finished
/// (never, without one), or the task's own retention when it chose one.
#[derive(Debug, Default)]
pub(super) struct Retention {
    /// How long a finished task without its own retention is kept. `None`
    /// keeps such tasks forever.
    ttl: Option<Duration>,
    /// Finished tasks that keep the shard's retention, by when they finished,
    /// so a change of `ttl` applies to them at once and only the due ones
    /// are visited.
    by_shard: BTreeSet<(Instant, TaskId)>,
    /// Finished tasks with their own retention, by when they are due.
    by_own: BTreeSet<(Instant, TaskId)>,
    /// Every task in either, for asking whether one is over.
    over: BTreeSet<TaskId>,
    /// Tasks that are over but kept regardless of their TTL, with when they
    /// finished and their own retention, if they chose one: members of a
    /// composite that is still held, whose results are read through it. Each
    /// joins `by_shard` or `by_own` when it is released.
    held: BTreeMap<TaskId, (Instant, Option<Duration>)>,
}

impl Retention {
    pub(super) fn set_ttl(&mut self, ttl: Option<Duration>) {
        self.ttl = ttl;
    }

    /// How long a finished task that chose no retention is kept, or `None`
    /// for ever.
    pub(super) fn ttl(&self) -> Option<Duration> {
        self.ttl
    }

    /// Forgets every task it holds, keeping the TTL.
    pub(super) fn clear(&mut self) {
        self.by_shard.clear();
        self.by_own.clear();
        self.over.clear();
        self.held.clear();
    }

    /// `task` is over as of `now`, to be kept for `own` if it chose a
    /// retention (see `task_retention`).
    pub(super) fn record(&mut self, task: &TaskId, now: Instant, own: Option<Duration>) {
        self.insert(task, now, own);
        self.over.insert(task.clone());
    }

    /// `task` is over as of `now`, and kept until [`Self::release`] however
    /// long ago that was.
    pub(super) fn hold(&mut self, task: &TaskId, now: Instant, own: Option<Duration>) {
        self.held.insert(task.clone(), (now, own));
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

    /// `task` is no longer kept regardless of its TTL: it is forgotten its
    /// retention after it finished, which may be now.
    pub(super) fn release(&mut self, task: &TaskId) {
        if let Some((finished_at, own)) = self.held.remove(task) {
            self.insert(task, finished_at, own);
        }
    }

    fn insert(&mut self, task: &TaskId, finished_at: Instant, own: Option<Duration>) {
        match own {
            Some(own) => self.by_own.insert((finished_at + own, task.clone())),
            None => self.by_shard.insert((finished_at, task.clone())),
        };
    }

    /// Whether `task` is over and not yet forgotten.
    pub(super) fn holds(&self, task: &TaskId) -> bool {
        self.over.contains(task)
    }

    /// Removes and returns every task due to be forgotten by `now`, earliest
    /// first within each kind of retention; the shard's retention never
    /// forgets one while no TTL is set.
    pub(super) fn take_due(&mut self, now: Instant) -> Vec<TaskId> {
        let mut due = Vec::new();
        if let Some(ttl) = self.ttl {
            while let Some((finished_at, _)) = self.by_shard.first() {
                if now - *finished_at < ttl {
                    break;
                }
                due.push(self.by_shard.pop_first().expect("just seen").1);
            }
        }
        while let Some((due_at, _)) = self.by_own.first() {
            if *due_at > now {
                break;
            }
            due.push(self.by_own.pop_first().expect("just seen").1);
        }
        for task in &due {
            self.over.remove(task);
        }
        due
    }

    /// When the next task is due to be forgotten, if any is.
    pub(super) fn next_due(&self) -> Option<Instant> {
        let by_shard = self
            .ttl
            .and_then(|ttl| self.by_shard.first().map(|(finished_at, _)| *finished_at + ttl));
        let by_own = self.by_own.first().map(|(due_at, _)| *due_at);
        by_shard.into_iter().chain(by_own).min()
    }
}
