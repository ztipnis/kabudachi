//! Compaction: how a long waiting chain of a coalescing key is folded before
//! its newest generation runs.
//!
//! A compaction is an internal task of its own, with its own record, that
//! names the entries at the front of one key's waiting chain by their
//! generations and digests. A worker that runs compaction folds those payloads
//! with the task's merge, oldest first, and returns the folded payload to the
//! leader (the one kind of run whose result is not just a digest). The leader
//! swaps the entries for one folded entry only if the chain still starts with
//! exactly them: a supersession only appends to the chain, so it does not
//! stop the swap, but dropping the oldest payloads, or the waiting generation
//! ending, does, and the result is then discarded. Nothing is lost or folded
//! twice either way, because the entries a fold replaces are still in the
//! chain until it is swapped in.
//!
//! A compaction run is never retried and never counts as a loss; a lost one
//! is simply over, and the check that made it runs again. A failed one stops
//! the key being compacted until its waiting generation changes. A queued
//! compaction does not hold its key: when the waiting generation is claimed
//! first, the compaction ends and that generation folds its whole chain; only
//! a claimed compaction holds the generation back.

use std::collections::BTreeSet;

use crate::coalescing::{self, ChainItem, Folded, Key};
use crate::protocol::digest::Digest;
use crate::protocol::generated::{CompactionPrefix, PrefixEntry};
use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId, mint_task_id};
use crate::protocol::messages::prelude::*;
use crate::protocol::records::{NewTask, TaskRunRecord, first_attempt, new_task};
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, Instant, WallTime};

use super::{
    COALESCED_PAYLOAD_TOO_LARGE, COMPACTION_SOFT_BYTES, Claim, ClaimRejection, Compacted, Event,
    MAX_SUBMISSION_BYTES, Observer, ReportRejection, Scheduler,
};

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// The workers that run compaction, as their node's heartbeats report
    /// them. With none, no compaction is made, and the newest generation of a
    /// key folds its whole chain itself when claimed.
    pub fn set_compaction_runners(&mut self, runners: BTreeSet<WorkerId>) {
        if self.compaction_runners == runners {
            return;
        }
        self.compaction_runners = runners;
        self.compact_every_due_key();
        self.end_call();
    }

    /// `worker` folded the entries the compaction run `run_id` was given into
    /// `folded`. If its key's waiting chain still starts with those entries
    /// they become one folded entry and the generations folded are released;
    /// otherwise the result is discarded. Either way the run is over. Only
    /// the run's worker may report it, as with `complete`.
    pub fn complete_compaction(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        folded: Vec<u8>,
    ) -> Result<Compacted, ReportRejection> {
        let outcome = self.certify_compaction(worker, run_id, folded);
        self.end_call();
        outcome
    }

    fn certify_compaction(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        folded: Vec<u8>,
    ) -> Result<Compacted, ReportRejection> {
        self.require_leader()?;
        if let Some(run) = self.runs.get(run_id)
            && !self.is_compaction(&run.task_id())
        {
            return Err(ReportRejection::NotAuthoritative);
        }
        self.start_if_claimed_compaction(worker, run_id);
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        let digest = Digest::blake3(&folded);
        let run = self.owned_run(worker, run_id, TaskRunState::Running)?;
        run.transition_to(TaskRunState::Succeeded, stamped_at)
            .expect("a Running run can always succeed");
        run.result_digest = Some(digest.clone().into());
        let task_id = run.task_id();
        self.mark_decided(run_id);
        let key = self
            .compaction_key_of(&task_id)
            .expect("a compaction run's task names its key");
        let applied = self.apply_fold(&task_id, &key, folded, digest, now);
        self.record_finished(&task_id, now);
        self.fail_waiting_if_too_large(&key, now);
        self.compact_if_due(&key);
        Ok(Compacted {
            task_id,
            task_run_id: run_id.clone(),
            applied,
        })
    }

    /// Swaps the entries compaction `task_id` named for `folded`, if `key`'s
    /// waiting chain still starts with them. Says whether it did.
    fn apply_fold(
        &mut self,
        task_id: &TaskId,
        key: &Key,
        folded: Vec<u8>,
        digest: Digest,
        now: Instant,
    ) -> bool {
        let Some(count) = self.matching_prefix_len(task_id) else {
            return false;
        };
        let generations = self.occupancy.waiting_chain(key)[..count]
            .iter()
            .flat_map(ChainItem::generations)
            .collect();
        let item = Folded {
            generations,
            payload: folded,
            digest,
        };
        let taken = item.payload.len() as u64;
        let replaced = self.occupancy.fold_front(key, count, item);
        let given_back: u64 = replaced.iter().map(|item| self.item_len(item)).sum();
        self.budget.give_back(given_back);
        self.budget.take(taken);
        // A generation inside an earlier fold was released when that fold was
        // swapped in.
        for item in &replaced {
            if let ChainItem::Absorbed(generation) = item {
                self.forget_later(generation, now);
            }
        }
        if let Some(waiting) = self.occupancy.waiting_of(key).cloned() {
            self.unpublished.insert(waiting);
        }
        self.update_pressure();
        true
    }

    /// If `key`'s waiting generation, with the chain a fold left it, no
    /// longer fits one claim, it can never run: it fails, and its chain is
    /// released, which frees the key for new submissions.
    fn fail_waiting_if_too_large(&mut self, key: &Key, now: Instant) {
        let Some(waiting) = self.occupancy.waiting_of(key).cloned() else {
            return;
        };
        let task = &self.tasks[&waiting];
        let claim_size = self.claim_bytes(
            task.serialized_input.len(),
            &task.queue,
            task.task_definition_id().as_str(),
            task.coalescing_key.as_deref(),
            self.occupancy.chain(&waiting).iter(),
        );
        if claim_size <= MAX_SUBMISSION_BYTES {
            return;
        }
        let stamped_at = WallTime::now(&self.clock);
        let run_id = self.current_run[&waiting].clone();
        let run = self
            .runs
            .get_mut(&run_id)
            .expect("every current run is stored");
        run.transition_to(TaskRunState::Failed, stamped_at)
            .expect("a pending run can always fail");
        run.failure_kind = COALESCED_PAYLOAD_TOO_LARGE.to_owned();
        self.waiting.leave(&waiting);
        self.record_finished(&waiting, now);
        self.mark_decided(&run_id);
        self.events.push(Event::CoalescedPayloadTooLarge {
            task_id: waiting,
            task_run_id: run_id,
        });
    }

    /// Sets what each key's compaction run is from the tasks held: a
    /// compaction that is not over is its key's, and holds the key's waiting
    /// generation back once a worker holds it.
    pub(super) fn restore_compactions(&mut self) {
        let alive: Vec<(Key, TaskId, bool)> = self
            .tasks
            .keys()
            .filter(|task_id| !self.retention.holds(task_id))
            .filter_map(|task_id| {
                let key = self.compaction_key_of(task_id)?;
                let claimed = matches!(
                    self.runs[&self.current_run[task_id]].current_state(),
                    TaskRunState::Claimed | TaskRunState::Running
                );
                Some((key, task_id.clone(), claimed))
            })
            .collect();
        for (key, task_id, claimed) in alive {
            self.occupancy.restore_compaction(&key, &task_id, claimed);
        }
    }

    /// Whether `task_id` is a compaction run's task.
    pub(super) fn is_compaction(&self, task_id: &TaskId) -> bool {
        self.tasks
            .get(task_id)
            .is_some_and(|task| task.compacts.is_some())
    }

    /// The coalescing key the compaction run `task_id` folds the chain of.
    pub(super) fn compaction_key_of(&self, task_id: &TaskId) -> Option<Key> {
        let task = self.tasks.get(task_id)?;
        let prefix = task.compacts.as_ref()?;
        Some(coalescing::key(&task.task_definition_id(), &prefix.coalescing_key))
    }

    /// How many entries at the front of its key's waiting chain compaction
    /// `task_id` names, if the chain still starts with exactly them.
    fn matching_prefix_len(&self, task_id: &TaskId) -> Option<usize> {
        let key = self.compaction_key_of(task_id)?;
        let prefix = self.tasks[task_id].compacts.as_ref()?;
        let chain = self.occupancy.waiting_chain(&key);
        let entries = &prefix.entries;
        let matches = !entries.is_empty()
            && entries.len() <= chain.len()
            && entries.iter().zip(chain).all(|(entry, item)| {
                let digest: crate::protocol::generated::Digest = self.item_digest(item).into();
                entry.generations.iter().cloned().map(TaskId::from).eq(item.generations())
                    && entry.digest.as_ref() == Some(&digest)
            });
        matches.then_some(entries.len())
    }

    fn item_digest(&self, item: &ChainItem) -> Digest {
        match item {
            ChainItem::Absorbed(task_id) => self.input_digests[task_id].clone(),
            ChainItem::Folded(folded) => folded.digest.clone(),
        }
    }

    /// The payloads compaction `task_id` hands its worker, oldest first, or
    /// `None` when its key's chain no longer starts with the entries it
    /// names, so there is nothing for it to fold.
    pub(super) fn compaction_claim_chain(&self, task_id: &TaskId) -> Option<Vec<Vec<u8>>> {
        let count = self.matching_prefix_len(task_id)?;
        let key = self.compaction_key_of(task_id)?;
        Some(
            self.occupancy.waiting_chain(&key)[..count]
                .iter()
                .map(|item| self.item_payload(item).to_vec())
                .collect(),
        )
    }

    /// Whether `worker` may be handed compaction `task_id` now. A queued
    /// compaction whose chain has changed under it is ended here instead.
    pub(super) fn may_claim_compaction(&mut self, worker: &WorkerId, task_id: &TaskId) -> bool {
        if !self.compaction_runners.contains(worker) {
            return false;
        }
        if self.compaction_claim_chain(task_id).is_none() {
            self.cancel_compaction(task_id);
            return false;
        }
        true
    }

    pub(super) fn claim_compaction(
        &mut self,
        worker: &WorkerId,
        task_id: &TaskId,
    ) -> Result<Claim, ClaimRejection> {
        if !self.compaction_runners.contains(worker) {
            return Err(ClaimRejection::CannotRun);
        }
        if !self.may_claim_compaction(worker, task_id) {
            return Err(ClaimRejection::Finished);
        }
        Ok(self.claim_queued(worker, task_id))
    }

    /// Ends the compaction run `task_id`, which no worker holds yet: its
    /// chain changed, or its generation was claimed whole. No client hears of
    /// it.
    pub(super) fn cancel_compaction(&mut self, task_id: &TaskId) {
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        let run_id = self.current_run[task_id].clone();
        self.runs
            .get_mut(&run_id)
            .expect("every current run is stored")
            .transition_to(TaskRunState::Cancelled, stamped_at)
            .expect("a queued run can always be cancelled");
        self.waiting.leave(task_id);
        self.record_finished(task_id, now);
        self.mark_decided(&run_id);
    }

    /// A report on a compaction run that is only claimed: the run begins at
    /// the same time, since nothing else needs to know it started.
    pub(super) fn start_if_claimed_compaction(&mut self, worker: &WorkerId, run_id: &TaskRunId) {
        let claimed = self.runs.get(run_id).is_some_and(|run| {
            run.current_state() == TaskRunState::Claimed && self.is_compaction(&run.task_id())
        });
        if claimed {
            let _ = self.start_owned(worker, run_id);
        }
    }

    /// A compaction run failed: its key is left alone until the generation
    /// it was made for is replaced.
    pub(super) fn note_failed_compaction(&mut self, task_id: &TaskId) {
        let Some(key) = self.compaction_key_of(task_id) else {
            return;
        };
        if let Some(waiting) = self.occupancy.waiting_of(&key).cloned() {
            self.failed_compactions.insert(key, waiting);
        }
    }

    /// Makes a compaction run for every key whose chain has outgrown its
    /// threshold.
    pub(super) fn compact_every_due_key(&mut self) {
        for key in self.occupancy.waiting_keys() {
            self.compact_if_due(&key);
        }
    }

    /// Makes a compaction run for `key` if its waiting chain is past the
    /// threshold (or memory is past its soft limit), a worker runs
    /// compaction, and none is under way. The front it names is as many
    /// entries as one claim carries, and at least two.
    pub(super) fn compact_if_due(&mut self, key: &Key) {
        if self.compaction_runners.is_empty() || !self.is_leader() {
            return;
        }
        if let Some(queued) = self.occupancy.compaction_of(key).cloned()
            && !self.occupancy.compaction_is_claimed(key)
            && self.matching_prefix_len(&queued).is_none()
        {
            self.cancel_compaction(&queued);
        }
        let Some(waiting) = self.occupancy.waiting_of(key).cloned() else {
            return;
        };
        if self.occupancy.compaction_of(key).is_some()
            || self.failed_compactions.get(key) == Some(&waiting)
        {
            return;
        }
        let retained: u64 = self
            .occupancy
            .waiting_chain(key)
            .iter()
            .map(|item| self.item_len(item))
            .sum();
        if retained <= COMPACTION_SOFT_BYTES && !self.budget.over_soft_limit() {
            return;
        }
        let Some(prefix) = self.occupancy.prefix_to_compact(
            key,
            |item| self.item_len(item),
            MAX_SUBMISSION_BYTES,
        ) else {
            return;
        };
        self.create_compaction(key, &waiting, &prefix);
    }

    /// A queued compaction run of `key` naming `prefix`, the front of the
    /// chain of `waiting`, whose definition, version and queue it takes (the
    /// worker finds the merge by definition).
    fn create_compaction(&mut self, key: &Key, waiting: &TaskId, prefix: &[ChainItem]) {
        let stamped_at = WallTime::now(&self.clock);
        let entries = prefix
            .iter()
            .map(|item| PrefixEntry {
                generations: item.generations().into_iter().map(Into::into).collect(),
                digest: Some(self.item_digest(item).into()),
            })
            .collect();
        let model = &self.tasks[waiting];
        let mut new = NewTask::new(
            mint_task_id(&self.ids),
            stamped_at,
            model.task_definition_id(),
            model.source_version,
            Vec::new(),
            model.queue.clone(),
        );
        // A lost compaction is not replayed: the check that made it runs again.
        new.ephemeral = true;
        let mut task = new_task(new);
        task.compacts = Some(CompactionPrefix {
            coalescing_key: key.1.clone(),
            entries,
        });
        let run = first_attempt(&task, &self.ids, stamped_at, TaskRunState::Queued);
        let task_id = task.task_id();
        self.current_run.insert(task_id.clone(), run.task_run_id());
        self.runs_of_task
            .insert(task_id.clone(), vec![run.task_run_id()]);
        self.runs.insert(run.task_run_id(), run);
        self.input_digests
            .insert(task_id.clone(), Digest::blake3(&[]));
        self.tasks.insert(task_id.clone(), task);
        self.waiting.admit(&task_id, None, None);
        self.mark_current_decided(&task_id);
        self.occupancy.begin_compaction(key, &task_id);
    }
}
