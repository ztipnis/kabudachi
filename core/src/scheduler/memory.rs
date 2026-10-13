//! The memory tasks hold: the limits, the check that refuses a submission past
//! the hard one, what dropping and releasing payloads gives back, and the
//! SlowDown signal.

use crate::coalescing::{self, ChainItem};
use crate::manifest;
use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId};
use crate::protocol::records::TaskRunRecord as _;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, Instant};

use super::{Event, Observer, Scheduler, Submission, SubmitRejection};

/// When the scheduler asks for less work, and when it refuses it, counted in
/// the serialized bytes of every task that has not finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryLimits {
    /// Past this, `SlowDown` is raised: whoever submits in bulk should pause.
    pub soft: u64,
    /// A submission that would take usage past this is refused.
    pub hard: u64,
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Sets the memory limits, or removes them with `None`. Set them before
    /// anything is recorded: they are not checked against what is already in
    /// use, and no `SlowDown` event is raised for them.
    ///
    /// # Panics
    ///
    /// If the soft limit is above the hard one.
    pub fn set_memory_limits(&mut self, limits: Option<MemoryLimits>) {
        self.budget.set_limits(limits);
    }

    /// The limits [`Self::set_memory_limits`] set, if any.
    pub fn memory_limits(&self) -> Option<MemoryLimits> {
        self.budget.limits()
    }

    /// Serialized bytes of every task that has not finished, and of every
    /// superseded one still needed by the generation that absorbed it.
    pub fn memory_in_use(&self) -> u64 {
        self.budget.in_use()
    }

    /// A worker read `bytes` of results as the input of the member run `run`,
    /// which is claimed or running: they count against the limits, so
    /// `SlowDown` and the refusal of what would not fit see them, until the
    /// run is decided. Holding again for the same run replaces what it held.
    pub fn hold_member_input(&mut self, run: &TaskRunId, bytes: u64) {
        let holds = self
            .runs
            .get(run)
            .is_some_and(|run| matches!(run.current_state(), TaskRunState::Claimed | TaskRunState::Running));
        if !holds {
            return;
        }
        if let Some(before) = self.member_inputs.insert(run.clone(), bytes) {
            self.budget.give_back(before);
        }
        self.budget.take(bytes);
        self.update_pressure();
    }

    /// `run` was decided: if it is over, or waits to run again, it holds no
    /// results any more.
    pub(super) fn release_decided_member_input(&mut self, run: &TaskRunId) {
        if !self.member_inputs.contains_key(run) {
            return;
        }
        if !matches!(self.runs[run].current_state(), TaskRunState::Claimed | TaskRunState::Running) {
            self.release_member_input(run);
        }
    }

    /// The run `run` no longer holds the results it read: they stop counting
    /// against the limits.
    pub(super) fn release_member_input(&mut self, run: &TaskRunId) {
        if let Some(bytes) = self.member_inputs.remove(run) {
            self.budget.give_back(bytes);
            self.update_pressure();
        }
    }

    /// Refuses `submission` if it would take memory past the hard limit and
    /// cannot be made to fit by dropping what it is allowed to drop.
    pub(super) fn check_room(
        &self,
        submission: &Submission,
        needed: u64,
    ) -> Result<(), SubmitRejection> {
        self.budget.check_room(needed, || {
            match (&submission.coalescing_key, submission.drop_oldest) {
                (Some(key), true) => {
                    let key = coalescing::key(&submission.definition_id, key);
                    self.occupancy
                        .retained(&key)
                        .iter()
                        .map(|item| self.item_len(item))
                        .sum()
                }
                _ => 0,
            }
        })
    }

    /// If `task_id` took memory past the hard limit, which `check_room` only
    /// allows for a task that may drop its key's retained payloads, drops
    /// them oldest first until it fits.
    pub(super) fn drop_oldest_until_it_fits(&mut self, task_id: &TaskId, now: Instant) {
        while self.budget.over_hard_limit() {
            let Some(dropped) = self.occupancy.drop_oldest(task_id) else {
                break;
            };
            self.release_item(&dropped, now);
        }
    }

    /// Raises or clears `SlowDown` if memory use has crossed the relevant
    /// limit.
    pub(super) fn update_pressure(&mut self) {
        let event = self.budget.update_pressure();
        let crossed_soft_limit = matches!(event, Some(Event::SlowDown { active: true }));
        self.announce(event);
        if crossed_soft_limit {
            self.compact_every_due_key();
        }
    }

    /// Records for the caller the event the budget returned, if
    /// any.
    fn announce(&mut self, event: Option<Event>) {
        let Some(event) = event else {
            return;
        };
        self.events.push(event);
    }

    /// `task_id`'s payload is no longer needed: it stops counting against
    /// memory, and the task is forgotten `result_ttl` from now.
    pub(super) fn release(&mut self, task_id: &TaskId, now: Instant) {
        let payload = self.payload_len(task_id);
        self.budget.give_back(payload);
        self.forget_later(task_id, now);
    }

    /// An entry of a chain is no longer needed. An absorbed generation is
    /// released; a folded payload stops counting against memory (the
    /// generations it replaced were released when it replaced them).
    pub(super) fn release_item(&mut self, item: &ChainItem, now: Instant) {
        match item {
            ChainItem::Absorbed(task_id) => self.release(task_id, now),
            ChainItem::Folded(folded) => self.budget.give_back(folded.payload.len() as u64),
        }
    }

    /// What the task's input weighs, inline or as blobs, which is what memory
    /// use counts.
    fn payload_len(&self, task_id: &TaskId) -> u64 {
        manifest::payload_bytes(&self.tasks[task_id])
    }
}
