//! Which generation of each coalescing key is waiting, which one holds the
//! key, the payloads a waiting generation has absorbed, and which key has a
//! compaction run folding the front of its chain.
//!
//! Only bookkeeping: whether a generation is pending, superseded or finished
//! is the scheduler's business, and it tells this what happened.

use std::collections::BTreeMap;

use crate::protocol::digest::Digest;
use crate::protocol::ids::{TaskDefinitionId, TaskId};

/// A coalescing key: the task definition and the flat key string, so two
/// different tasks never share a key by coincidence.
pub(crate) type Key = (String, String);

/// The coalescing key of a task of `definition` with the flat key `key`.
pub(crate) fn key(definition: &TaskDefinitionId, key: &str) -> Key {
    (definition.as_str().to_owned(), key.to_owned())
}

/// One entry of a waiting generation's chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChainItem {
    /// A superseded generation, whose payload is its task's input.
    Absorbed(TaskId),
    /// Generations a compaction folded into one payload.
    Folded(Folded),
}

/// Several generations' payloads, folded oldest first into one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Folded {
    /// The generations folded, oldest first.
    pub generations: Vec<TaskId>,
    pub payload: Vec<u8>,
    pub digest: Digest,
}

impl ChainItem {
    /// The generations this entry holds, oldest first.
    pub fn generations(&self) -> Vec<TaskId> {
        match self {
            ChainItem::Absorbed(task) => vec![task.clone()],
            ChainItem::Folded(folded) => folded.generations.clone(),
        }
    }
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
    chains: BTreeMap<TaskId, Vec<ChainItem>>,
    /// The compaction run each key has, queued or claimed.
    compactions: BTreeMap<Key, Compaction>,
}

/// A compaction run of a key.
struct Compaction {
    task: TaskId,
    /// Whether a worker holds it, which holds the key's waiting generation
    /// back: its chain must not be claimed while its front is being folded.
    claimed: bool,
}

impl Occupancy {
    /// `task` becomes the waiting generation of `key`. Returns the waiting
    /// generation it supersedes, if any; that generation and everything it had
    /// absorbed are now `task`'s chain.
    pub fn submit(&mut self, key: &Key, task: &TaskId) -> Option<TaskId> {
        let older = self.waiting.insert(key.clone(), task.clone())?;
        let mut chain = self.chains.remove(&older).unwrap_or_default();
        chain.push(ChainItem::Absorbed(older.clone()));
        self.chains.insert(task.clone(), chain);
        Some(older)
    }

    /// Sets what `key` holds, as a leader rebuilding from records found it:
    /// its waiting generation, the generation holding it, and each one's
    /// chain, oldest first.
    pub fn restore(
        &mut self,
        key: &Key,
        waiting: Option<TaskId>,
        holder: Option<TaskId>,
        chains: BTreeMap<TaskId, Vec<ChainItem>>,
    ) {
        match waiting {
            Some(task) => self.waiting.insert(key.clone(), task),
            None => self.waiting.remove(key),
        };
        match holder {
            Some(task) => self.holders.insert(key.clone(), task),
            None => self.holders.remove(key),
        };
        self.chains.extend(chains);
    }

    /// Forgets every key, holder and chain.
    pub fn clear(&mut self) {
        *self = Occupancy::default();
    }

    /// The payloads `key` retains, oldest first: what the waiting generation
    /// absorbed, then the waiting generation itself, which a submission of the
    /// same key would supersede next.
    pub fn retained(&self, key: &Key) -> Vec<ChainItem> {
        let Some(waiting) = self.waiting.get(key) else {
            return Vec::new();
        };
        let mut retained = self.chains.get(waiting).cloned().unwrap_or_default();
        retained.push(ChainItem::Absorbed(waiting.clone()));
        retained
    }

    /// Drops the oldest entry of `task`'s chain, and returns it.
    pub fn drop_oldest(&mut self, task: &TaskId) -> Option<ChainItem> {
        let chain = self.chains.get_mut(task)?;
        (!chain.is_empty()).then(|| chain.remove(0))
    }

    /// Whether a generation of `key` is waiting to be claimed, that is, a
    /// newer one than whatever holds the key.
    pub fn has_waiting(&self, key: &Key) -> bool {
        self.waiting.contains_key(key)
    }

    /// Whether `task` has to wait: another generation holds `key`, or `task`
    /// is the waiting generation and a worker holds a compaction run of the
    /// key.
    pub fn is_blocked(&self, key: &Key, task: &TaskId) -> bool {
        self.holders.get(key).is_some_and(|holder| holder != task)
            || (self.waiting.get(key) == Some(task)
                && self.compactions.get(key).is_some_and(|run| run.claimed))
    }

    /// The generation of `key` that is waiting to be claimed.
    pub fn waiting_of(&self, key: &Key) -> Option<&TaskId> {
        self.waiting.get(key)
    }

    /// The keys that have a waiting generation.
    pub fn waiting_keys(&self) -> Vec<Key> {
        self.waiting.keys().cloned().collect()
    }

    /// What `key`'s waiting generation absorbed, oldest first; empty if
    /// nothing waits.
    pub fn waiting_chain(&self, key: &Key) -> &[ChainItem] {
        self.waiting.get(key).map_or(&[], |waiting| self.chain(waiting))
    }

    /// The oldest entries of `key`'s waiting chain to fold: as many as fit
    /// `budget` bytes of payload, and at least two, or `None`.
    pub fn prefix_to_compact(
        &self,
        key: &Key,
        payload_len: impl Fn(&ChainItem) -> u64,
        budget: u64,
    ) -> Option<Vec<ChainItem>> {
        let mut taken = 0;
        let prefix: Vec<ChainItem> = self
            .waiting_chain(key)
            .iter()
            .take_while(|item| {
                taken += payload_len(item);
                taken <= budget
            })
            .cloned()
            .collect();
        (prefix.len() >= 2).then_some(prefix)
    }

    /// Replaces the first `count` entries of `key`'s waiting chain with
    /// `folded` and returns them. The caller has checked that they are the
    /// entries a compaction folded.
    pub fn fold_front(&mut self, key: &Key, count: usize, folded: Folded) -> Vec<ChainItem> {
        let Some(waiting) = self.waiting.get(key) else {
            return Vec::new();
        };
        let chain = self.chains.entry(waiting.clone()).or_default();
        let count = count.min(chain.len());
        chain
            .splice(..count, [ChainItem::Folded(folded)])
            .collect()
    }

    /// The compaction run `key` has, queued or claimed.
    pub fn compaction_of(&self, key: &Key) -> Option<&TaskId> {
        self.compactions.get(key).map(|run| &run.task)
    }

    /// Whether a worker holds `key`'s compaction run.
    pub fn compaction_is_claimed(&self, key: &Key) -> bool {
        self.compactions.get(key).is_some_and(|run| run.claimed)
    }

    /// `task`, a queued compaction run, is now `key`'s.
    pub fn begin_compaction(&mut self, key: &Key, task: &TaskId) {
        self.compactions.insert(
            key.clone(),
            Compaction {
                task: task.clone(),
                claimed: false,
            },
        );
    }

    /// Sets `key`'s compaction run to `task`, as a leader rebuilding from
    /// records found it: held by a worker, or still queued.
    pub fn restore_compaction(&mut self, key: &Key, task: &TaskId, claimed: bool) {
        self.compactions.insert(
            key.clone(),
            Compaction {
                task: task.clone(),
                claimed,
            },
        );
    }

    /// A worker claimed `key`'s compaction run.
    pub fn compaction_claimed(&mut self, key: &Key, task: &TaskId) {
        if let Some(run) = self.compactions.get_mut(key).filter(|run| run.task == *task) {
            run.claimed = true;
        }
    }

    /// `task`, however it ended, is no longer `key`'s compaction run.
    pub fn end_compaction(&mut self, key: &Key, task: &TaskId) {
        if self.compaction_of(key) == Some(task) {
            self.compactions.remove(key);
        }
    }

    /// What `task` absorbed, oldest first, for its worker to fold once it
    /// is claimed.
    pub fn chain(&self, task: &TaskId) -> &[ChainItem] {
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
    /// is no longer needed. Returns those entries, to be forgotten in their
    /// turn.
    pub fn finish(&mut self, key: &Key, task: &TaskId) -> Vec<ChainItem> {
        if self.holders.get(key) == Some(task) {
            self.holders.remove(key);
        }
        if self.waiting.get(key) == Some(task) {
            self.waiting.remove(key);
        }
        self.chains.remove(task).unwrap_or_default()
    }
}
