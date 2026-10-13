//! How workers claim tasks: the oldest pending ones in a batch, or one by name,
//! and what a claim carries.

use std::collections::{BTreeMap, BTreeSet};

use crate::blob::BlobKey;
use crate::manifest::{InvalidManifest, Manifest, Unresolved};
use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::Task;
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::queues::{QueuePolicy, Subscription};
use crate::time::{Clock, Duration, Instant, WallTime};

use super::fairness::Front;
use super::{Observer, Scheduler};

/// A worker's permission to run a task: the Task itself and the run it now
/// owns.
#[derive(Debug, Clone, PartialEq)]
pub struct Claim {
    pub task: Task,
    pub task_run_id: TaskRunId,
    /// Which attempt this run is: 1 for the first, then one more per retry.
    pub attempt_number: u32,
    /// The run's reconnect timeout: its task's own, or else the shard's
    /// default from this leader's grant.
    pub reconnect_timeout: Duration,
    /// The serialized inputs of the generations this one superseded, oldest
    /// first, for the worker to fold before running the task.
    /// Empty unless the task is a coalescing one that absorbed others.
    pub chain: Vec<Vec<u8>>,
}

impl Claim {
    /// The manifest the task's input travels as, if it travels as blobs.
    pub fn input_manifest(&self) -> Result<Option<Manifest>, InvalidManifest> {
        self.task
            .input_manifest
            .as_ref()
            .map(Manifest::try_from)
            .transpose()
    }

    /// Puts the task's whole input in `task.serialized_input`, joined from the
    /// blobs its manifest names, which `blob` looks up, and checked against
    /// them. A task whose input is inline is left as it is.
    pub fn resolve_input(
        &mut self,
        blob: impl FnMut(&BlobKey) -> Option<Vec<u8>>,
    ) -> Result<(), Unresolved> {
        let Some(manifest) = self.input_manifest().map_err(|_| Unresolved::Malformed)? else {
            return Ok(());
        };
        self.task.serialized_input = manifest.join(blob)?;
        self.task.input_manifest = None;
        Ok(())
    }
}

/// Why a claim request was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClaimRejection {
    #[error("this node is not the leader")]
    NotLeader,
    #[error("no such task")]
    TaskUnknown,
    #[error("the task is not due yet")]
    NotReady,
    #[error("the task's run was already claimed")]
    AlreadySelected,
    #[error("the task's run has finished")]
    Finished,
    #[error("a newer generation of the task's coalescing key replaced it")]
    Superseded,
    #[error("another generation of the task's coalescing key is running")]
    KeyBusy,
    /// The task is a compaction run and the asking worker has not said it
    /// runs them.
    #[error("the worker does not run compaction")]
    CannotRun,
    /// The task's queue is not one the claiming worker takes.
    #[error("the task's queue is not one the worker takes")]
    NotSubscribed,
    /// The task's queue already has as many tasks claimed or running as its
    /// `max_running` allows.
    #[error("the task's queue is full")]
    QueueFull,
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Replaces what the scheduler knows of each worker's queues. A worker not
    /// in `subscriptions` is held to none: it may take any queue.
    pub fn set_subscriptions(&mut self, subscriptions: BTreeMap<WorkerId, Subscription>) {
        self.subscriptions = subscriptions;
    }

    /// The queues `worker` takes.
    fn subscription_of(&self, worker: &WorkerId) -> Subscription {
        self.subscriptions
            .get(worker)
            .cloned()
            .unwrap_or(Subscription::Every)
    }

    /// Whether the current state of `run` is claimed or running.
    fn is_active(&self, run: &TaskRunId) -> bool {
        self.runs.get(run).is_some_and(|run| {
            matches!(
                run.current_state(),
                TaskRunState::Claimed | TaskRunState::Running
            )
        })
    }

    /// Counts the current run of `task_id`, if it is not a compaction, as one
    /// of its queue's runs, and drops the finished ones.
    pub(super) fn count_running(&mut self, task_id: &TaskId) {
        if self.is_compaction(task_id) {
            return;
        }
        let queue = self.tasks[task_id].queue.clone();
        let run = self.current_run[task_id].clone();
        let mut runs = self.running.remove(&queue).unwrap_or_default();
        runs.retain(|run| self.is_active(run));
        runs.insert(run);
        self.running.insert(queue, runs);
    }

    /// Whether `queue` has as many runs claimed or running as its
    /// `max_running` allows.
    fn queue_is_full(&self, queue: &str) -> bool {
        let Some(max) = self.queue_policy.options(queue).max_running else {
            return false;
        };
        let running = self
            .running
            .get(queue)
            .map_or(0, |runs| runs.iter().filter(|run| self.is_active(run)).count());
        running >= max.get() as usize
    }

    /// How this scheduler orders and limits each queue while it leads. Every
    /// worker of a shard should give the same policy: the leader's decides.
    pub fn set_queue_policy(&mut self, policy: QueuePolicy) {
        self.queue_policy = policy;
    }

    /// A worker asks for up to `limit` pending tasks at once, in queue order
    /// (see [`Self::claim_oldest_fitting`]). Fewer, or none, are returned if
    /// fewer are pending.
    pub fn claim_oldest(
        &mut self,
        worker: &WorkerId,
        limit: usize,
    ) -> Result<Vec<Claim>, ClaimRejection> {
        self.claim_oldest_fitting(worker, &Subscription::Every, limit, |_| true)
    }

    /// Like [`Self::claim_oldest`], from the queues `asked` names. Each next
    /// task is the front of the most urgent queue: a task past its queue's
    /// starvation age first (oldest first), then the highest priority, and
    /// among queues of one priority each in turn by weight (see
    /// `fairness::Turns`). Each candidate is shown to `fits` before it is
    /// claimed. The first one `fits` refuses ends the batch and stays pending,
    /// first in line for the next call; except that one refused while nothing
    /// is claimed yet, which would never fit, is passed over so the tasks
    /// behind it still go out. No claim outgrows a frame, so that last rule
    /// only guards a caller whose limit is smaller. It is how a caller that
    /// must deliver the claims keeps them within what it can deliver.
    pub fn claim_oldest_fitting(
        &mut self,
        worker: &WorkerId,
        asked: &Subscription,
        limit: usize,
        fits: impl FnMut(&Claim) -> bool,
    ) -> Result<Vec<Claim>, ClaimRejection> {
        let outcome = self.claim_while_fitting(worker, asked, limit, fits);
        self.end_call();
        outcome
    }

    fn claim_while_fitting(
        &mut self,
        worker: &WorkerId,
        asked: &Subscription,
        limit: usize,
        mut fits: impl FnMut(&Claim) -> bool,
    ) -> Result<Vec<Claim>, ClaimRejection> {
        if !self.check_leader() {
            return Err(ClaimRejection::NotLeader);
        }
        self.release_due();
        let mut claims = Vec::new();
        let mut passed = BTreeSet::new();
        // Time is only allowed to take effect once per call: each candidate
        // is claimed as it is, whatever the clock has done since.
        while claims.len() < limit {
            let Some(task_id) = self.next_to_claim(worker, asked, &passed) else {
                break;
            };
            let claim = self.claim_to_be(&task_id);
            if fits(&claim) {
                self.mark_claimed(worker, &task_id);
                claims.push(claim);
            } else if claims.is_empty() {
                passed.insert(task_id);
            } else {
                break;
            }
        }
        Ok(claims)
    }

    /// The task `worker` should be handed next from the queues `asked` names,
    /// skipping `passed`, and takes its queue's turn.
    fn next_to_claim(
        &mut self,
        worker: &WorkerId,
        asked: &Subscription,
        passed: &BTreeSet<TaskId>,
    ) -> Option<TaskId> {
        let now = self.clock.now();
        let wanted = self.subscription_of(worker).narrowed_to(asked);
        let queues: Vec<String> = self
            .waiting
            .queues()
            .filter(|queue| wanted.includes(queue))
            .map(str::to_owned)
            .collect();
        let mut fronts = Vec::new();
        for queue in &queues {
            if let Some((ticket, since, task_id)) = self.first_claimable_in(worker, queue, passed) {
                fronts.push((queue.as_str(), ticket, now - since, task_id));
            }
        }
        let waiting = &self.waiting;
        self.turns.forget_empty(|queue| waiting.has_queued(queue));
        let views: Vec<Front<'_>> = fronts
            .iter()
            .map(|(queue, ticket, waited, _)| Front {
                queue,
                ticket: *ticket,
                waited: *waited,
            })
            .collect();
        let chosen = self.turns.pick(&self.queue_policy, &views)?;
        Some(fronts.swap_remove(chosen).3)
    }

    /// The oldest task of `queue` that `worker` could be handed now, with its
    /// ticket and when it started waiting: not passed over, not held back by
    /// another generation of its coalescing key or for want of knowing one,
    /// and a compaction only for a worker that may run it. In a full queue
    /// only a compaction qualifies, as it does not count against the limit.
    fn first_claimable_in(
        &mut self,
        worker: &WorkerId,
        queue: &str,
        passed: &BTreeSet<TaskId>,
    ) -> Option<(u64, Instant, TaskId)> {
        let full = self.queue_is_full(queue);
        let mut after = None;
        loop {
            let (ticket, since, task_id) = self.waiting.queued_in_after(queue, after)?;
            after = Some(ticket);
            if full && !self.is_compaction(&task_id) {
                continue;
            }
            if passed.contains(&task_id) || self.is_blocked(&task_id) || self.is_held_back(&task_id)
            {
                continue;
            }
            if self.is_compaction(&task_id) && !self.may_claim_compaction(worker, &task_id) {
                continue;
            }
            return Some((ticket, since, task_id));
        }
    }

    /// A worker asks for `task_id`. Of several workers asking for the same
    /// task, the first is accepted and the rest are refused.
    pub fn request_claim(
        &mut self,
        worker: &WorkerId,
        task_id: &TaskId,
    ) -> Result<Claim, ClaimRejection> {
        let outcome = self.claim_task(worker, task_id);
        self.end_call();
        outcome
    }

    fn claim_task(&mut self, worker: &WorkerId, task_id: &TaskId) -> Result<Claim, ClaimRejection> {
        if !self.check_leader() {
            return Err(ClaimRejection::NotLeader);
        }
        self.release_due();
        if self.reconciliation.is_uncertain(task_id) {
            return Err(ClaimRejection::NotReady);
        }
        let run_id = self
            .current_run
            .get(task_id)
            .ok_or(ClaimRejection::TaskUnknown)?
            .clone();
        match self.runs[&run_id].current_state() {
            TaskRunState::Queued if self.is_held_back(task_id) => Err(ClaimRejection::NotReady),
            TaskRunState::Queued if self.is_blocked(task_id) => Err(ClaimRejection::KeyBusy),
            TaskRunState::Queued
                if !self
                    .subscription_of(worker)
                    .includes(&self.tasks[task_id].queue) =>
            {
                Err(ClaimRejection::NotSubscribed)
            }
            TaskRunState::Queued if self.is_compaction(task_id) => {
                self.claim_compaction(worker, task_id)
            }
            TaskRunState::Queued if self.queue_is_full(&self.tasks[task_id].queue) => {
                Err(ClaimRejection::QueueFull)
            }
            TaskRunState::Queued => Ok(self.claim_queued(worker, task_id)),
            TaskRunState::Scheduled => Err(ClaimRejection::NotReady),
            TaskRunState::Claimed | TaskRunState::Running => Err(ClaimRejection::AlreadySelected),
            TaskRunState::Superseded => Err(ClaimRejection::Superseded),
            _ => Err(ClaimRejection::Finished),
        }
    }

    /// Claims `task_id`, whose current run must be `Queued`, for `worker`.
    pub(super) fn claim_queued(&mut self, worker: &WorkerId, task_id: &TaskId) -> Claim {
        let claim = self.claim_to_be(task_id);
        self.mark_claimed(worker, task_id);
        claim
    }

    /// The claim that claiming `task_id`, whose current run must be
    /// `Queued`, would hand out. Changes nothing.
    fn claim_to_be(&self, task_id: &TaskId) -> Claim {
        let run_id = self.current_run[task_id].clone();
        let chain = match self.coalescing_key_of(task_id) {
            None if self.is_compaction(task_id) => {
                self.compaction_claim_chain(task_id).unwrap_or_default()
            }
            Some(_) => self
                .occupancy
                .chain(task_id)
                .iter()
                .map(|item| self.item_payload(item).to_vec())
                .collect(),
            None => Vec::new(),
        };
        Claim {
            task: self.tasks[task_id].clone(),
            attempt_number: self.runs[&run_id].attempt_number(),
            reconnect_timeout: self.reconnect_timeout_of(task_id),
            task_run_id: run_id,
            chain,
        }
    }

    /// How long after its worker falls silent `task_id`'s current run may be
    /// replayed: the task's own reconnect timeout, or else the shard's, from
    /// the grant. Asked only while leading, so a grant is held.
    pub(super) fn reconnect_timeout_of(&self, task_id: &TaskId) -> Duration {
        match self.tasks[task_id].reconnect_timeout_ms {
            0 => self.leadership.shard_reconnect_timeout(),
            own => Duration::from_millis(own),
        }
    }

    /// Records that `worker` claimed `task_id`, whose current run must be
    /// `Queued`. Starting is what expiry is about, so the task's expiry is
    /// dropped.
    fn mark_claimed(&mut self, worker: &WorkerId, task_id: &TaskId) {
        let stamped_at = WallTime::now(&self.clock);
        let run_id = self.current_run[task_id].clone();
        let run = self
            .runs
            .get_mut(&run_id)
            .expect("every current run is stored");
        run.transition_to(TaskRunState::Claimed, stamped_at)
            .expect("a Queued run can always be claimed");
        run.selected_worker = Some(worker.clone().into());
        run.claimed_at = Some(stamped_at.into());
        self.waiting.claimed(task_id);
        if let Some(key) = self.coalescing_key_of(task_id) {
            self.occupancy.start(&key, task_id);
        } else if let Some(key) = self.compaction_key_of(task_id) {
            self.occupancy.compaction_claimed(&key, task_id);
        }
        self.mark_decided(&run_id);
        self.count_running(task_id);
    }
}
