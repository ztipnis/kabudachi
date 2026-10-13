//! What the scheduler publishes: each changed task's whole record,
//! versioned, at the end of every call that changed it, and what it forgets
//! once a finished task's time to live has passed.

use crate::coalescing::ChainItem;
use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::generated::{
    AbsorbedGeneration, ChainEntry, FoldedPayload, TaskRecord, chain_entry,
};
use crate::protocol::ids::{IdGenerator, TaskId};
use crate::task_record::{RecordKey, RecordVersion};
use crate::time::{Clock, Duration, WallTime};

use super::composite::ITEM_PAGES;
use super::{Observer, Scheduler};

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    pub(super) fn publish_changed(&mut self) {
        let Some(grant) = self.leadership.grant().filter(|_| self.is_leader()) else {
            return;
        };
        self.publish_unpublished(grant.recovery_epoch, grant.term);
    }

    /// Publishes every task changed since its last revision, as a revision
    /// of `term` at `recovery_epoch`, and returns how many.
    pub(super) fn publish_unpublished(
        &mut self,
        recovery_epoch: RecoveryEpoch,
        term: u64,
    ) -> usize {
        let published_at = WallTime::now(&self.clock);
        let mut published = 0;
        let batch = std::mem::take(&mut self.unpublished);
        let mut changed: Vec<TaskId> = batch.iter().cloned().collect();
        // A superseded generation is published after the generation that
        // superseded it, so a write order that holds the older revision back
        // until the newer one is stored finds the newer one first.
        changed.sort_by_key(|task_id| {
            self.links
                .get(task_id)
                .and_then(|link| link.superseded_by.clone())
                .is_some_and(|newer| batch.contains(&TaskId::from(newer)))
        });
        for task_id in changed {
            if !self.tasks.contains_key(&task_id) {
                continue;
            }
            let version = self.leadership.next_version(recovery_epoch, term);
            let record = self.record_of(&task_id, version, published_at);
            self.observer.revision(record);
            published += 1;
        }
        published + self.publish_composites(recovery_epoch, term, published_at)
    }

    /// Publishes the pages and then the records of the composites changed
    /// since their last revision, so a reader that finds a composite's record
    /// finds every page it names. Returns how many it published.
    fn publish_composites(
        &mut self,
        recovery_epoch: RecoveryEpoch,
        term: u64,
        published_at: WallTime,
    ) -> usize {
        let pages = std::mem::take(&mut self.composites.unpublished_pages);
        let records = std::mem::take(&mut self.composites.unpublished);
        let mut published = 0;
        let mut released = false;
        for (id, page) in pages {
            let Some(composite) = self.composites.held.get(&id) else {
                continue;
            };
            let item_page = page >= ITEM_PAGES;
            // An item page the leader no longer has was stored already, and
            // is written again only to say its reduce is over.
            if item_page && !composite.holds_item_page(page) && !self.composites.is_finished(&id) {
                continue;
            }
            let version = self.leadership.next_version(recovery_epoch, term);
            let record = self.page_record_of(&id, page, version, published_at);
            self.observer.page_revision(record);
            published += 1;
            if item_page {
                self.item_page_published(&id, page, version);
                released |= self.observer.stores_at_once() && self.release_item_page(&id, page);
            }
        }
        // The bytes given back may end SlowDown, which no later call need.
        if released {
            self.update_pressure();
        }
        for id in records {
            if !self.composites.held.contains_key(&id) {
                continue;
            }
            let version = self.leadership.next_version(recovery_epoch, term);
            let record = self.composite_record_of(&id, version, published_at);
            self.observer.composite_revision(record);
            published += 1;
        }
        published
    }

    /// The whole record of `task_id` as the scheduler holds it now.
    pub(super) fn record_of(
        &self,
        task_id: &TaskId,
        version: RecordVersion,
        published_at: WallTime,
    ) -> TaskRecord {
        let retained_chain = self
            .occupancy
            .chain(task_id)
            .iter()
            .map(|item| self.chain_entry_of(item))
            .collect();
        TaskRecord {
            version: Some(version.into()),
            task: Some(self.tasks[task_id].clone()),
            runs: self.runs_of_task[task_id]
                .iter()
                .map(|run| self.runs[run].clone())
                .collect(),
            retained_chain,
            input_digest: Some(self.input_digests[task_id].clone().into()),
            link: self.links.get(task_id).cloned(),
            placement: Vec::new(),
            prior_placements: Vec::new(),
            published_at: Some(published_at.into()),
            finished: self.retention.holds(task_id),
            retained_by_composite: self.retention.is_held(task_id),
        }
    }

    pub(super) fn chain_entry_of(&self, item: &ChainItem) -> ChainEntry {
        let entry = match item {
            ChainItem::Absorbed(absorbed) => chain_entry::Entry::Absorbed(AbsorbedGeneration {
                task_id: Some(absorbed.clone().into()),
                serialized_input: self.tasks[absorbed].serialized_input.clone(),
                input_digest: Some(self.input_digests[absorbed].clone().into()),
            }),
            ChainItem::Folded(folded) => chain_entry::Entry::Folded(FoldedPayload {
                generations: folded.generations.iter().cloned().map(Into::into).collect(),
                serialized_input: folded.payload.clone(),
                input_digest: Some(folded.digest.clone().into()),
            }),
        };
        ChainEntry { entry: Some(entry) }
    }

    /// Publishes a new revision of each of `keys` it holds, unchanged but for
    /// its version, so its driver can write it where it now belongs. Records it
    /// does not hold, or holds uncertain, are skipped. Only a leader
    /// publishes: any other returns 0. Returns how many it published.
    pub fn republish(&mut self, keys: &[RecordKey]) -> usize {
        if !self.is_leader() {
            return 0;
        }
        let mut republished = 0;
        let mut seen = std::collections::BTreeSet::new();
        for key in keys {
            if !seen.insert(key) || !self.holds(key) {
                continue;
            }
            match key {
                RecordKey::Task(task) if !self.reconciliation.is_uncertain(task) => {
                    self.unpublished.insert(task.clone());
                    republished += 1;
                }
                RecordKey::Task(_) => {}
                RecordKey::Composite(id) => {
                    self.composites.unpublished.insert(id.clone());
                    republished += 1;
                }
                RecordKey::Page(id, page) => {
                    self.composites.unpublished_pages.insert((id.clone(), *page));
                    republished += 1;
                }
            }
        }
        self.end_call();
        republished
    }

    /// Forgets every task that finished at least `result_ttl` ago, with its
    /// run, and returns how many. Cheap when nothing is due, so every
    /// mutating call ends with it.
    pub(super) fn forget_due(&mut self) -> usize {
        if let Some(ttl) = self.retention.ttl() {
            let members = self.composites.forget_due(ttl, self.clock.now());
            if !members.is_empty() {
                // Holders learn their retention may begin before the leader
                // forgets the members it may forget at once.
                self.release_members(members);
                self.publish_changed();
            }
        }
        let Some(due) = self.retention.next_due() else {
            return 0;
        };
        let now = self.clock.now();
        if due > now {
            return 0;
        }
        let mut forgotten = 0;
        for task_id in self.retention.take_due(now) {
            self.current_run.remove(&task_id);
            self.losses.remove(&task_id);
            for run_id in self.runs_of_task.remove(&task_id).unwrap_or_default() {
                self.runs.remove(&run_id);
                self.run_decided_at.remove(&run_id);
            }
            self.tasks.remove(&task_id);
            self.input_digests.remove(&task_id);
            self.links.remove(&task_id);
            self.unpublished.remove(&task_id);
            forgotten += 1;
        }
        forgotten
    }

    /// How long a task is kept after it finishes, or `None` to keep tasks
    /// forever. Takes effect at once.
    pub fn set_result_ttl(&mut self, ttl: Option<Duration>) {
        self.retention.set_ttl(ttl);
        self.end_call();
    }
}
