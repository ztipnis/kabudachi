//! Which generation of each coalescing key is waiting, which one holds the
//! key, and the payloads a waiting generation has absorbed.
//!
//! Only bookkeeping: whether a generation is pending, superseded or finished
//! is the scheduler's business, and it tells this what happened.

use std::collections::BTreeMap;

use crate::protocol::ids::{TaskDefinitionId, TaskId};

/// A coalescing key: the task definition and the flat key string, so two
/// different tasks never share a key by coincidence.
pub(crate) type Key = (String, String);

/// The coalescing key of a task of `definition` with the flat key `key`.
pub(crate) fn key(definition: &TaskDefinitionId, key: &str) -> Key {
    (definition.as_str().to_owned(), key.to_owned())
}

#[derive(Default)]
/// Which generation of each coalescing key waits, which holds the key, and what each absorbed.
pub(crate) struct Occupancy {
    /// The one generation of each key that is waiting to be claimed.
    waiting: BTreeMap<Key, TaskId>,
    /// The generation that holds each key, from being claimed until it is
    /// finished, retries included.
    holders: BTreeMap<Key, TaskId>,
    /// The generations each waiting or holding generation absorbed, oldest
    /// first. They are kept, payload and all, until it finishes.
    chains: BTreeMap<TaskId, Vec<TaskId>>,
}

impl Occupancy {
    /// `task` becomes the waiting generation of `key`. Returns the waiting
    /// generation it supersedes, if any; that generation and everything it had
    /// absorbed are now `task`'s chain.
    pub fn submit(&mut self, key: &Key, task: &TaskId) -> Option<TaskId> {
        let older = self.waiting.insert(key.clone(), task.clone())?;
        let mut chain = self.chains.remove(&older).unwrap_or_default();
        chain.push(older.clone());
        self.chains.insert(task.clone(), chain);
        Some(older)
    }

    /// The payloads `key` retains, oldest first: what the waiting generation
    /// absorbed, then the waiting generation itself, which a submission of the
    /// same key would supersede next.
    pub fn retained(&self, key: &Key) -> Vec<TaskId> {
        let Some(waiting) = self.waiting.get(key) else {
            return Vec::new();
        };
        let mut retained = self.chains.get(waiting).cloned().unwrap_or_default();
        retained.push(waiting.clone());
        retained
    }

    /// Drops the oldest generation `task` absorbed, and returns it.
    pub fn drop_oldest(&mut self, task: &TaskId) -> Option<TaskId> {
        let chain = self.chains.get_mut(task)?;
        (!chain.is_empty()).then(|| chain.remove(0))
    }

    /// Whether a generation of `key` is waiting to be claimed, that is, a
    /// newer one than whatever holds the key.
    pub fn has_waiting(&self, key: &Key) -> bool {
        self.waiting.contains_key(key)
    }

    /// Whether another generation holds `key`, so `task` has to wait.
    pub fn is_blocked(&self, key: &Key, task: &TaskId) -> bool {
        self.holders.get(key).is_some_and(|holder| holder != task)
    }

    /// What `task` absorbed, oldest first, for its worker to fold once it
    /// is claimed.
    pub fn chain(&self, task: &TaskId) -> &[TaskId] {
        self.chains.get(task).map_or(&[], Vec::as_slice)
    }

    /// `task` was claimed: it holds `key` now, and is no longer waiting. Its
    /// chain stays, so a retry of `task` folds it again.
    pub fn start(&mut self, key: &Key, task: &TaskId) {
        if self.waiting.get(key) == Some(task) {
            self.waiting.remove(key);
        }
        self.holders.insert(key.clone(), task.clone());
    }

    /// `task` is over, however it ended: it frees `key`, and what it absorbed
    /// is no longer needed. Returns those generations, to be forgotten in
    /// their turn.
    pub fn finish(&mut self, key: &Key, task: &TaskId) -> Vec<TaskId> {
        if self.holders.get(key) == Some(task) {
            self.holders.remove(key);
        }
        if self.waiting.get(key) == Some(task) {
            self.waiting.remove(key);
        }
        self.chains.remove(task).unwrap_or_default()
    }
}
