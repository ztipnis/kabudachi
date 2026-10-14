//! How a task's current run ends: finishing frees its key and its memory and
//! starts its time to live, expiry ends a run that waited too long, supersession
//! ends one a newer generation replaced, and a retry or replay queues the next
//! attempt.

use prost::Message;

use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId};
use crate::protocol::messages::prelude::*;
use crate::protocol::records::{TaskRunRecord, retry_of};
use crate::protocol::task::TaskRunState;
use crate::task_record::HISTORY_TOO_LARGE_FAILURE_KIND;
use crate::time::{Clock, Instant, WallTime};

use super::{Event, Observer, Scheduler, UNVERSIONED};

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Takes a task that is over out of every place it was waiting in.
    pub(super) fn record_finished(&mut self, task_id: &TaskId, now: Instant) {
        self.release(task_id, now);
        if let Some(key) = self.compaction_key_of(task_id) {
            self.occupancy.end_compaction(&key, task_id);
        }
        if let Some(key) = self.coalescing_key_of(task_id) {
            // Whatever it absorbed is no longer needed, so it can be forgotten
            // in its turn.
            for item in self.occupancy.finish(&key, task_id) {
                self.release_item(&item, now);
            }
        }
        self.update_pressure();
        self.conclude(task_id);
        self.member_finished(task_id);
    }

    /// `task_id` is over and holds no payload any more: the task is forgotten
    /// `result_ttl` from now. A member of a composite this scheduler holds is
    /// kept until the composite is forgotten, since the composite's results
    /// name the blobs its members stored.
    pub(super) fn forget_later(&mut self, task_id: &TaskId, now: Instant) {
        let composite = self.tasks.get(task_id).and_then(|task| task.composite_id());
        if composite.is_some_and(|composite| self.composites.held.contains_key(&composite)) {
            self.retention.hold(task_id, now);
        } else {
            self.retention.record(task_id, now);
        }
        self.unpublished.insert(task_id.clone());
    }

    /// After composites were installed: a member kept for a composite this
    /// scheduler neither holds nor waits to learn of has nothing to be kept
    /// for.
    pub(super) fn release_orphaned_members(&mut self) {
        let orphans: Vec<TaskId> = self
            .retention
            .held()
            .into_iter()
            .filter(|task_id| {
                let composite = self.tasks.get(task_id).and_then(|task| task.composite_id());
                !composite.is_some_and(|composite| {
                    self.composites.held.contains_key(&composite) || self.reconciliation.is_uncertain_composite(&composite)
                })
            })
            .collect();
        self.release_members(orphans);
    }

    /// The members of composites just forgotten are kept no longer: they are
    /// forgotten when their own TTL has passed, and holders are told with
    /// one last revision that their retention may begin.
    pub(super) fn release_members(&mut self, members: Vec<TaskId>) {
        for task_id in members {
            if self.retention.is_held(&task_id) {
                self.retention.release(&task_id);
                self.unpublished.insert(task_id);
            }
        }
    }

    /// Expires `task_id`, which `take_expired` found still waiting, so its
    /// current run is pending.
    pub(super) fn expire(&mut self, task_id: &TaskId, now: Instant) {
        let stamped_at = WallTime::now(&self.clock);
        let run_id = self.current_run[task_id].clone();
        let run = self
            .runs
            .get_mut(&run_id)
            .expect("every current run is stored");
        // The waiting room drops a task's expiry when the task is claimed or
        // leaves, so a task it reports as expired has not started: its run is
        // still `Scheduled` or `Queued`. Without this, a broken invariant
        // would expire a claimed run, which the transition table allows.
        assert!(
            matches!(
                run.current_state(),
                TaskRunState::Scheduled | TaskRunState::Queued
            ),
            "a waiting task's run is pending"
        );
        run.transition_to(TaskRunState::Expired, stamped_at)
            .expect("a pending run can always expire");
        self.record_finished(task_id, now);
        self.mark_decided(&run_id);
        self.events.push(Event::Expired {
            task_id: task_id.clone(),
            task_run_id: run_id,
        });
    }

    /// `older`, a pending generation, is replaced by `newer`, which has
    /// absorbed its payload: it is over, but not forgotten until `newer` is.
    pub(super) fn supersede(&mut self, older: &TaskId, newer: &TaskId) {
        let stamped_at = WallTime::now(&self.clock);
        let run_id = self.current_run[older].clone();
        self.runs
            .get_mut(&run_id)
            .expect("every current run is stored")
            .transition_to(TaskRunState::Superseded, stamped_at)
            .expect("a pending run can always be superseded");
        self.waiting.leave(older);
        self.links.entry(older.clone()).or_default().superseded_by = Some(newer.clone().into());
        self.mark_decided(&run_id);
        self.events.push(Event::Superseded {
            task_id: older.clone(),
            task_run_id: run_id,
            by: newer.clone(),
        });
        self.conclude(older);
    }

    /// Queues the attempt that follows `previous`, a terminal run of
    /// `task_id`, and makes it the one authoritative run. If the record would
    /// then be past the limit, no attempt is created: the task is over (the
    /// caller finishes it), a `RecordFull` event is raised, and a run that
    /// has no failure kind of its own gets one saying why.
    pub(super) fn queue_next_attempt(
        &mut self,
        task_id: &TaskId,
        previous: &TaskRunId,
    ) -> Option<TaskRunId> {
        let stamped_at = WallTime::now(&self.clock);
        let next = retry_of(&self.runs[previous], &self.ids, stamped_at);
        let record = self.record_of(task_id, UNVERSIONED, stamped_at);
        // Adding the run to the record also adds its field tag and length prefix.
        let next_len = next.encoded_len();
        let run_framing = 1 + prost::encoding::encoded_len_varint(next_len as u64);
        let grown = (record.encoded_len() + run_framing + next_len) as u64;
        if grown > Self::record_limit() {
            let run = self
                .runs
                .get_mut(previous)
                .expect("a previous run is stored");
            if run.failure_kind.is_empty() {
                run.failure_kind = HISTORY_TOO_LARGE_FAILURE_KIND.to_owned();
                self.mark_decided(previous);
            }
            self.events.push(Event::RecordFull {
                task_id: task_id.clone(),
            });
            return None;
        }
        let next_id = next.task_run_id();
        self.current_run.insert(task_id.clone(), next_id.clone());
        self.runs_of_task
            .entry(task_id.clone())
            .or_default()
            .push(next_id.clone());
        self.runs.insert(next_id.clone(), next);
        let queue = self.tasks[task_id].queue.clone();
        self.waiting.requeue(task_id, &queue, self.clock.now());
        self.mark_decided(&next_id);
        Some(next_id)
    }
}
