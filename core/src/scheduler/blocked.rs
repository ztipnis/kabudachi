//! Tasks a worker cannot run at their input version: the worker gives the
//! claim back and the leader queues the task for another worker, or, when the
//! worker says the input can never be migrated, ends the task. A task left
//! blocked past its TTL fails too.
//!
//! The leader never reads a payload or compares versions here: it keeps the
//! reason the worker wrote and counts time.

use std::collections::BTreeSet;

use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, Duration, Instant, WallTime};

use super::failures::cut_to_fit;
use super::{Event, MAX_FAILURE_MESSAGE_BYTES, MIGRATION_ERROR, Observer, Scheduler};

/// How long a task may wait blocked before it fails, unless the shard sets
/// another.
pub const DEFAULT_VERSION_BLOCK_TTL: Duration = Duration::from_secs(60 * 60);

/// A task some worker could not run at its input version and left for
/// another.
#[derive(Debug, Clone)]
pub(super) struct BlockedTask {
    /// When the first such report came, by the leader's wall clock.
    pub since: WallTime,
    /// Why the latest report could not run it, as the worker wrote it.
    pub reason: String,
    /// The workers that reported it. Not stored: a successor starts empty.
    pub by: BTreeSet<WorkerId>,
}

/// What a blocked report did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blocked {
    pub task_id: TaskId,
    /// The claimed run that went back to the queue, if the reporter held one.
    pub released: Option<TaskRunId>,
    /// The task ended FAILED (`MigrationError`).
    pub ended: bool,
}

/// Why a blocked report was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BlockRejection {
    #[error("this node is not the leader")]
    NotLeader,
    #[error("no such task")]
    UnknownTask,
    #[error("the task is not known for certain yet")]
    NotReady,
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// `worker` cannot run `task_id`'s input at its version, for `reason`. A
    /// run it had claimed goes back to the queue, unchanged but for its worker.
    /// If `incompatible`, the task then ends FAILED (MigrationError) and is not
    /// retried. Otherwise this leader no longer hands that worker the task, and
    /// the first such report starts the block TTL.
    pub fn report_blocked(
        &mut self,
        worker: &WorkerId,
        task_id: &TaskId,
        reason: String,
        incompatible: bool,
    ) -> Result<Blocked, BlockRejection> {
        let outcome = self.block(worker, task_id, reason, incompatible);
        self.end_call();
        outcome
    }

    fn block(
        &mut self,
        worker: &WorkerId,
        task_id: &TaskId,
        reason: String,
        incompatible: bool,
    ) -> Result<Blocked, BlockRejection> {
        if !self.check_leader() {
            return Err(BlockRejection::NotLeader);
        }
        if self.reconciliation.is_uncertain(task_id) {
            return Err(BlockRejection::NotReady);
        }
        let run_id = self
            .current_run
            .get(task_id)
            .ok_or(BlockRejection::UnknownTask)?
            .clone();
        let mut blocked = Blocked {
            task_id: task_id.clone(),
            released: None,
            ended: false,
        };
        // A compaction is the leader's own task: it is never given back.
        if self.is_compaction(task_id) {
            return Ok(blocked);
        }
        let reason = cut_to_fit(reason, MAX_FAILURE_MESSAGE_BYTES);
        let claimed_by_reporter = {
            let run = &self.runs[&run_id];
            run.current_state() == TaskRunState::Claimed
                && run.selected_worker().as_ref() == Some(worker)
        };
        if claimed_by_reporter {
            self.release_claim(task_id, &run_id);
            blocked.released = Some(run_id.clone());
        }
        if self.current_state(task_id) != Some(TaskRunState::Queued) {
            return Ok(blocked);
        }
        if incompatible {
            self.blocked.remove(task_id);
            self.ended_reasons.insert(task_id.clone(), reason.clone());
            let now = self.clock.now();
            self.fail_pending(task_id, MIGRATION_ERROR, now, |task_id, task_run_id| {
                Event::MigrationFailed {
                    task_id,
                    task_run_id,
                    reason,
                }
            });
            blocked.ended = true;
        } else {
            let since = WallTime::now(&self.clock);
            let entry = self.blocked.entry(task_id.clone()).or_insert_with(|| BlockedTask {
                since,
                reason: String::new(),
                by: BTreeSet::new(),
            });
            entry.reason = reason;
            entry.by.insert(worker.clone());
            self.unpublished.insert(task_id.clone());
        }
        Ok(blocked)
    }

    /// Puts `task_id`'s claimed run `run_id` back in the queue, as if it had
    /// not been claimed: no attempt is spent and no run is added. It waits at
    /// the back of its queue, like any requeued attempt, and without an
    /// expiry: a claim already ended the expiry, which is only about starting
    /// the task, and the block TTL bounds the wait instead.
    fn release_claim(&mut self, task_id: &TaskId, run_id: &TaskRunId) {
        let stamped_at = WallTime::now(&self.clock);
        let run = self
            .runs
            .get_mut(run_id)
            .expect("every current run is stored");
        run.transition_to(TaskRunState::Queued, stamped_at)
            .expect("a claimed run can be given back");
        run.selected_worker = None;
        run.claimed_at = None;
        let queue = self.tasks[task_id].queue.clone();
        self.waiting.requeue(task_id, &queue, self.clock.now());
        self.mark_decided(run_id);
        self.release_coalescing_generation(task_id);
    }

    /// A coalescing generation that was claimed and given back before any
    /// attempt ran no longer holds its key: it waits again, as a rebuilding
    /// leader would find it, and a newer generation of the key supersedes it
    /// as it would any waiting one. A generation that ran before (a retry)
    /// keeps the key, as retries do.
    fn release_coalescing_generation(&mut self, task_id: &TaskId) {
        if self.runs_of_task[task_id].len() != 1 {
            return;
        }
        let Some(key) = self.coalescing_key_of(task_id) else {
            return;
        };
        let Some(newer) = self.occupancy.release(&key, task_id) else {
            return;
        };
        self.supersede(task_id, &newer);
        let mut absorbed = self
            .links
            .get(task_id)
            .map(|link| link.absorbed.clone())
            .unwrap_or_default();
        absorbed.push(task_id.clone().into());
        let link = self.links.entry(newer).or_default();
        absorbed.append(&mut link.absorbed);
        link.absorbed = absorbed;
    }

    /// Whether `worker` told this leader it cannot run `task_id`.
    pub(super) fn blocked_by(&self, task_id: &TaskId, worker: &WorkerId) -> bool {
        self.blocked
            .get(task_id)
            .is_some_and(|blocked| blocked.by.contains(worker))
    }

    /// How long a task may wait blocked before it fails. Every worker of a
    /// shard should give the same: the leader's decides.
    pub fn set_version_block_ttl(&mut self, ttl: Duration) {
        self.version_block_ttl = ttl;
    }

    /// Fails every task whose block has lasted the TTL while it still waits,
    /// and says how many. The block starts at a wall time its record keeps,
    /// so the TTL compares wall times and survives a change of leader.
    pub(super) fn fail_blocked_past_ttl(&mut self, now: Instant) -> usize {
        let wall_now = WallTime::now(&self.clock);
        let ttl_millis = self.version_block_ttl.as_ticks();
        let due: Vec<TaskId> = self
            .blocked
            .iter()
            .filter(|(_, blocked)| blocked.since.plus_millis(ttl_millis) <= wall_now)
            .map(|(task, _)| task.clone())
            .filter(|task| self.current_state(task) == Some(TaskRunState::Queued))
            .collect();
        for task in &due {
            let reason = self
                .blocked
                .remove(task)
                .map(|blocked| blocked.reason)
                .unwrap_or_default();
            self.ended_reasons.insert(task.clone(), reason.clone());
            self.fail_pending(task, MIGRATION_ERROR, now, |task_id, task_run_id| {
                Event::MigrationFailed {
                    task_id,
                    task_run_id,
                    reason,
                }
            });
        }
        due.len()
    }

    /// The instant on the monotonic clock at which the first blocked task
    /// reaches its TTL, if any is blocked.
    pub(super) fn next_block_deadline(&self) -> Option<Instant> {
        let now = self.clock.now();
        let wall_now = WallTime::now(&self.clock);
        self.blocked
            .iter()
            .filter(|(task, _)| self.current_state(task) == Some(TaskRunState::Queued))
            .map(|(_, blocked)| blocked.since.deadline(self.version_block_ttl, now, wall_now))
            .min()
    }

    /// What blocks `task_id`, while its current run waits in the queue. A task
    /// some other worker has claimed since, or that has ended, is not blocked:
    /// nothing is waiting on the TTL, and its record shows no block.
    pub(super) fn queued_block(&self, task_id: &TaskId) -> Option<&BlockedTask> {
        self.blocked
            .get(task_id)
            .filter(|_| self.current_state(task_id) == Some(TaskRunState::Queued))
    }

    /// The state of `task_id`'s current run, if the task is held.
    fn current_state(&self, task_id: &TaskId) -> Option<TaskRunState> {
        let run = self.current_run.get(task_id)?;
        Some(self.runs[run].current_state())
    }
}
