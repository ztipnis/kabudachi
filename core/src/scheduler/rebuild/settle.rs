//! Which rebuilt records may be installed: a coalescing key whose generations disagree is held back whole.

use std::collections::{BTreeMap, BTreeSet};

use crate::coalescing::{self, ChainItem, Key};
use crate::protocol::generated::{CoalescingLink, TaskRecord, chain_entry};
use crate::protocol::ids::{IdGenerator, TaskId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{Task, TaskRun};
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
        let Some((newer_id, predecessor)) = absorbing_generation(newer) else {
            return Supersession::Holds;
        };
        // The generations a compaction folded were released with it, and their
        // records may be forgotten by now.
        let folded = folded_ids(newer);
        // A chain entry the leader would fold must have its record: the
        // chain's size, release and fold look it up as a held task.
        let known = |task_id: &TaskId| {
            index.contains_key(task_id)
                || self.tasks.contains_key(task_id)
                || folded.contains(task_id)
        };
        if !retained_ids(newer).iter().chain([&predecessor]).all(known) {
            return Supersession::Broken;
        }
        match index.get(&predecessor) {
            Some(&at) => against_record(&newer_id, &predecessor, at, &records[at]),
            None => self.against_installed(&newer_id, predecessor, &folded),
        }
    }

    /// The supersession when the predecessor's record is not in the batch: it
    /// was installed before this generation's record was known. If nothing
    /// claimed it, the supersession is finished live; a claimed one cannot have
    /// been absorbed.
    fn against_installed(
        &self,
        newer: &TaskId,
        predecessor: TaskId,
        folded: &BTreeSet<TaskId>,
    ) -> Supersession {
        if names(self.links.get(&predecessor), newer) {
            return Supersession::Holds;
        }
        if folded.contains(&predecessor) && !self.tasks.contains_key(&predecessor) {
            return Supersession::Holds;
        }
        let unclaimed = self
            .current_run
            .get(&predecessor)
            .is_some_and(|run_id| is_unclaimed_pending(&self.runs[run_id]));
        if unclaimed
            && !names_any(self.links.get(&predecessor))
            && !self.retention.holds(&predecessor)
        {
            return Supersession::UnfinishedInstalled(predecessor);
        }
        tracing::error!(
            newer = newer.as_str(),
            older = predecessor.as_str(),
            "a generation absorbed an installed one that is claimed or finished"
        );
        Supersession::Broken
    }
}

/// The generation `record` is and the last one it absorbed, when it
/// absorbed any and is not finished.
fn absorbing_generation(record: &TaskRecord) -> Option<(TaskId, TaskId)> {
    let link = record
        .link
        .as_ref()
        .filter(|link| !link.absorbed.is_empty())?;
    if record.finished {
        return None;
    }
    let newer = record.task.as_ref().map(Task::task_id)?;
    let predecessor = TaskId::from(link.absorbed.last().expect("not empty").clone());
    Some((newer, predecessor))
}

/// The supersession when the predecessor's record, `older`, is in the
/// batch at `at`. A supersession only ever absorbs a generation that had
/// not been claimed, and writes its revision 0 before touching the older
/// one, so an older record that is neither superseded by `newer` nor still
/// pending is broken.
fn against_record(
    newer: &TaskId,
    predecessor: &TaskId,
    at: usize,
    older: &TaskRecord,
) -> Supersession {
    if names(older.link.as_ref(), newer) {
        return Supersession::Holds;
    }
    let pending = older.runs.last().is_some_and(is_unclaimed_pending);
    if pending && !names_any(older.link.as_ref()) && !older.finished {
        return Supersession::Unfinished(at);
    }
    tracing::error!(
        newer = newer.as_str(),
        older = predecessor.as_str(),
        "a generation absorbed one whose record shows it claimed or finished"
    );
    Supersession::Broken
}

/// Whether `link` names `newer` as the generation that superseded it.
fn names(link: Option<&CoalescingLink>, newer: &TaskId) -> bool {
    link.and_then(|link| link.superseded_by.clone())
        .map(TaskId::from)
        .as_ref()
        == Some(newer)
}

/// Whether `link` names any generation as the one that superseded it.
fn names_any(link: Option<&CoalescingLink>) -> bool {
    link.is_some_and(|link| link.superseded_by.is_some())
}

/// Whether `run` waits to start and no worker was ever selected for it.
fn is_unclaimed_pending(run: &TaskRun) -> bool {
    matches!(
        run.current_state(),
        TaskRunState::Scheduled | TaskRunState::Queued
    ) && run.selected_worker.is_none()
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
            chain_entry::Entry::Absorbed(absorbed) => absorbed.task_id.clone().map(TaskId::from),
            chain_entry::Entry::Folded(_) => None,
        })
        .collect()
}
