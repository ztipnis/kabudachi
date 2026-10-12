//! How workers claim tasks: the oldest pending ones in a batch, or one by name,
//! and what a claim carries.

use crate::blob::BlobKey;
use crate::manifest::{InvalidManifest, Manifest, Unresolved};
use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::Task;
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, Duration, WallTime};

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
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// A worker asks for up to `limit` of the oldest pending tasks at once.
    /// Fewer, or none, are returned if fewer are pending.
    pub fn claim_oldest(
        &mut self,
        worker: &WorkerId,
        limit: usize,
    ) -> Result<Vec<Claim>, ClaimRejection> {
        self.claim_oldest_fitting(worker, limit, |_| true)
    }

    /// Like [`Self::claim_oldest`], but each candidate, oldest first, is
    /// shown to `fits` before it is claimed. The first one `fits` refuses
    /// ends the batch and stays pending, first in line for the next call;
    /// except that one refused while nothing is claimed yet, which would
    /// never fit, is passed over so the tasks behind it still go out. No
    /// claim outgrows a frame, so that last rule only guards a caller whose
    /// limit is smaller. It is how a caller that must deliver the claims
    /// keeps them within what it can deliver.
    pub fn claim_oldest_fitting(
        &mut self,
        worker: &WorkerId,
        limit: usize,
        fits: impl FnMut(&Claim) -> bool,
    ) -> Result<Vec<Claim>, ClaimRejection> {
        let outcome = self.claim_while_fitting(worker, limit, fits);
        self.end_call();
        outcome
    }

    fn claim_while_fitting(
        &mut self,
        worker: &WorkerId,
        limit: usize,
        mut fits: impl FnMut(&Claim) -> bool,
    ) -> Result<Vec<Claim>, ClaimRejection> {
        if !self.check_leader() {
            return Err(ClaimRejection::NotLeader);
        }
        self.release_due();
        let mut claims = Vec::new();
        let mut after = None;
        // Time is only allowed to take effect once per call: each candidate
        // is claimed as it is, whatever the clock has done since.
        while claims.len() < limit {
            let Some((position, task_id)) = self.next_unblocked_after(after) else {
                break;
            };
            after = Some(position);
            if self.is_compaction(&task_id) && !self.may_claim_compaction(worker, &task_id) {
                continue;
            }
            let claim = self.claim_to_be(&task_id);
            if fits(&claim) {
                self.mark_claimed(worker, &task_id);
                claims.push(claim);
            } else if !claims.is_empty() {
                break;
            }
        }
        Ok(claims)
    }

    /// The oldest queued task, past queue position `after` if given, that
    /// no other generation of its coalescing key holds back, and whose key is
    /// not held back for want of knowing one of its generations.
    fn next_unblocked_after(&self, after: Option<u64>) -> Option<(u64, TaskId)> {
        self.waiting
            .queued_after(after)
            .find(|(_, task_id)| !self.is_blocked(task_id) && !self.is_held_back(task_id))
            .map(|(position, task_id)| (position, task_id.clone()))
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
            TaskRunState::Queued if self.is_compaction(task_id) => {
                self.claim_compaction(worker, task_id)
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
        self.waiting.claimed(task_id);
        if let Some(key) = self.coalescing_key_of(task_id) {
            self.occupancy.start(&key, task_id);
        } else if let Some(key) = self.compaction_key_of(task_id) {
            self.occupancy.compaction_claimed(&key, task_id);
        }
        self.mark_decided(&run_id);
    }
}
