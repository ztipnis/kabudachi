//! Which rebuilt records may be installed: a coalescing key whose generations disagree is held back whole.

use std::collections::{BTreeMap, BTreeSet};

use crate::coalescing::{self, ChainItem, Key};
use crate::protocol::generated::{CoalescingLink, TaskRecord, chain_entry};
use crate::protocol::ids::{IdGenerator, TaskId};
use crate::protocol::messages::Task;
use crate::protocol::messages::prelude::*;
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, WallTime};

use super::super::{Observer, Scheduler};
use super::install::chain_of;

/// What `Scheduler::settle` made of the records it was given.
pub(super) struct Settled {
    pub(super) install: Vec<TaskRecord>,
    pub(super) held_back: Vec<TaskRecord>,
    /// Supersessions to finish once the records are installed: the older
    /// generation is already installed, pending, and its record does not name
    /// the newer one (older, newer).
    pub(super) finish_live: Vec<(TaskId, TaskId)>,
}

/// Whether a generation that absorbed another agrees with that one's record.
enum Supersession {
    /// The older record names the newer one, or the generation absorbed
    /// nothing.
    Holds,
    /// The newer generation's first revision was stored but the older one's
    /// superseded revision was not, and the older one is still pending (the
    /// index of its record).
    Unfinished(usize),
    /// The same, but the older generation is already installed and pending.
    UnfinishedInstalled(TaskId),
    /// The older record is not known, or cannot be reconciled with the newer
    /// one: the key is not known yet.
    Broken,
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Decides which of `records`, each the newest known of its task, may be
    /// installed. A coalescing generation that absorbed another is live only
    /// if the absorbed one's record is known and agrees (see
    /// [`Supersession`]); a key where that fails is held back whole, since
    /// the generations of a key decide each other's occupancy. A pending
    /// generation that a newer one absorbed, whose own superseded revision
    /// was never written, is finished here: the older record becomes
    /// superseded, naming its successor, and is republished like any other.
    pub(super) fn settle(&self, mut records: Vec<TaskRecord>) -> Settled {
        let index: BTreeMap<TaskId, usize> = records
            .iter()
            .enumerate()
            .filter_map(|(at, record)| Some((record.task.as_ref()?.task_id(), at)))
            .collect();
        let key_of = |record: &TaskRecord| {
            let task = record.task.as_ref()?;
            Some(coalescing::key(
                &task.task_definition_id(),
                task.coalescing_key.as_deref()?,
            ))
        };
        // A key with a generation whose record is unknown is held whole, like
        // one found broken: the generations decide each other's fate, and one
        // not yet known may have absorbed a generation known already.
        let mut broken: BTreeSet<Key> = self.reconciliation.unknown_keys();
        let mut unfinished: Vec<(usize, TaskId)> = Vec::new();
        let mut finish_live: Vec<(TaskId, TaskId)> = Vec::new();
        for record in &records {
            let (Some(key), Some(newer)) = (key_of(record), record.task.as_ref()) else {
                continue;
            };
            match self.supersession_of(record, &records, &index) {
                Supersession::Holds => {}
                Supersession::Unfinished(older) => unfinished.push((older, newer.task_id())),
                Supersession::UnfinishedInstalled(older) => {
                    finish_live.push((older, newer.task_id()));
                }
                Supersession::Broken => {
                    broken.insert(key);
                }
            }
        }
        let stamped_at = WallTime::now(&self.clock);
        for (older, newer) in unfinished {
            if key_of(&records[older]).is_some_and(|key| broken.contains(&key)) {
                continue;
            }
            let record = &mut records[older];
            if let Some(run) = record.runs.last_mut() {
                run.transition_to(TaskRunState::Superseded, stamped_at)
                    .expect("a pending run can always be superseded");
            }
            record.link.get_or_insert_default().superseded_by = Some(newer.into());
        }
        finish_live.retain(|(_, newer)| {
            index
                .get(newer)
                .and_then(|&at| key_of(&records[at]))
                .is_some_and(|key| !broken.contains(&key))
        });
        let (held_back, install) = records
            .into_iter()
            .partition(|record| key_of(record).is_some_and(|key| broken.contains(&key)));
        Settled {
            install,
            held_back,
            finish_live,
        }
    }

    /// Whether `newer`, a generation that absorbed others, agrees with the
    /// record of the generation it replaced.
    fn supersession_of(
        &self,
        newer: &TaskRecord,
        records: &[TaskRecord],
        index: &BTreeMap<TaskId, usize>,
    ) -> Supersession {
        let Some(link) = newer.link.as_ref().filter(|link| !link.absorbed.is_empty()) else {
            return Supersession::Holds;
        };
        if newer.finished {
            return Supersession::Holds;
        }
        let Some(newer_id) = newer.task.as_ref().map(Task::task_id) else {
            return Supersession::Holds;
        };
        let predecessor = TaskId::from(link.absorbed.last().expect("not empty").clone());
        // The generations a compaction folded were released with it, and their
        // records may be forgotten by now.
        let folded = folded_ids(newer);
        // A chain entry the leader would fold must have its record: the
        // chain's size, release and fold look it up as a held task.
        let known = |task_id: &TaskId| {
            index.contains_key(task_id) || self.tasks.contains_key(task_id) || folded.contains(task_id)
        };
        if !retained_ids(newer)
            .iter()
            .chain([&predecessor])
            .all(known)
        {
            return Supersession::Broken;
        }
        let names_newer = |link: Option<&CoalescingLink>| {
            link.and_then(|link| link.superseded_by.clone()).map(TaskId::from)
                == Some(newer_id.clone())
        };
        let Some(&at) = index.get(&predecessor) else {
            if names_newer(self.links.get(&predecessor)) {
                return Supersession::Holds;
            }
            if folded.contains(&predecessor) && !self.tasks.contains_key(&predecessor) {
                return Supersession::Holds;
            }
            // The predecessor was installed before this generation's record
            // was known: if nothing claimed it, the supersession is finished
            // live; a claimed one cannot have been absorbed.
            let unclaimed = self.current_run.get(&predecessor).is_some_and(|run_id| {
                matches!(
                    self.runs[run_id].current_state(),
                    TaskRunState::Scheduled | TaskRunState::Queued
                ) && self.runs[run_id].selected_worker.is_none()
            });
            let named_other = self
                .links
                .get(&predecessor)
                .is_some_and(|link| link.superseded_by.is_some());
            if unclaimed && !named_other && !self.retention.holds(&predecessor) {
                return Supersession::UnfinishedInstalled(predecessor);
            }
            tracing::error!(
                newer = newer_id.as_str(),
                older = predecessor.as_str(),
                "a generation absorbed an installed one that is claimed or finished"
            );
            return Supersession::Broken;
        };
        let older = &records[at];
        if names_newer(older.link.as_ref()) {
            return Supersession::Holds;
        }
        let pending = older.runs.last().is_some_and(|run| {
            matches!(
                run.current_state(),
                TaskRunState::Scheduled | TaskRunState::Queued
            ) && run.selected_worker.is_none()
        });
        let named_other = older
            .link
            .as_ref()
            .is_some_and(|link| link.superseded_by.is_some());
        if pending && !named_other && !older.finished {
            return Supersession::Unfinished(at);
        }
        // A supersession only ever absorbs a generation that had not been
        // claimed, and writes its revision 0 before touching the older one.
        tracing::error!(
            newer = newer_id.as_str(),
            older = predecessor.as_str(),
            "a generation absorbed one whose record shows it claimed or finished"
        );
        Supersession::Broken
    }
}

/// The generations a compaction folded into the chain `record` carries.
fn folded_ids(record: &TaskRecord) -> BTreeSet<TaskId> {
    chain_of(record)
        .iter()
        .filter_map(|item| match item {
            ChainItem::Folded(folded) => Some(folded.generations.iter().cloned()),
            ChainItem::Absorbed(_) => None,
        })
        .flatten()
        .collect()
}

/// The generations whose payloads `record` still carries.
fn retained_ids(record: &TaskRecord) -> BTreeSet<TaskId> {
    record
        .retained_chain
        .iter()
        .filter_map(|entry| match entry.entry.as_ref()? {
            chain_entry::Entry::Absorbed(absorbed) => {
                absorbed.task_id.clone().map(TaskId::from)
            }
            chain_entry::Entry::Folded(_) => None,
        })
        .collect()
}
