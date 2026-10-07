//! Work a scheduler cannot take yet, kept in order until it can.

use std::collections::BTreeSet;

use crate::protocol::ids::{IdGenerator, TaskId};
use crate::time::Clock;

use super::{
    CancelRejection, Cancellation, ContinuationRejection, Observer, Scheduler, Submission,
    SubmitRejection, Submitted,
};

/// What its scheduler could not take yet: submissions made before it led,
/// and ends of continuations it refused for want of leadership. Once it
/// leads, [`Self::hand_over`] ends the continuations, then records the
/// submissions in the order they were made within a coalescing key. A
/// submission whose record would not yet fit (it carries the input of the
/// generation it supersedes) stays here until a later hand-over finds room,
/// and so do the later submissions of its key, which must not be recorded
/// ahead of it; every other submission is recorded regardless. Every
/// submission is checked against the hard memory limit together with the
/// input already waiting here.
#[derive(Debug, Default)]
pub struct Backlog {
    /// Submissions not yet recorded, in the order they were made.
    queued: Vec<Submitted>,
    /// The input bytes `queued` holds.
    queued_bytes: u64,
    /// Continuations whose end was refused, in the order they were refused.
    unended: Vec<TaskId>,
}

/// The coalescing key a submission competes under: its task definition and
/// its flat key, the same pair the scheduler keys generations by. `None` for a
/// submission that does not coalesce.
fn coalescing_key_of(submission: &Submission) -> Option<(&str, &str)> {
    let key = submission.coalescing_key.as_deref()?;
    Some((submission.definition_id.as_str(), key))
}

impl Backlog {
    /// Gives `submission` its task id at once. A scheduler that leads
    /// records it now, after recording what waits here, unless an earlier
    /// submission of its coalescing key still waits; otherwise it waits
    /// here. Refused only when it is too large to claim or would pass the
    /// hard limit with everything waiting here.
    pub fn submit<C: Clock, I: IdGenerator, O: Observer>(
        &mut self,
        scheduler: &mut Scheduler<C, I, O>,
        submission: Submission,
    ) -> Result<TaskId, SubmitRejection> {
        let submitted = scheduler.mint(submission);
        if scheduler.is_leader() {
            // A leader may have freed room since a refusal, so what waits is
            // recorded first.
            self.record_queued(scheduler);
            if !self.waits_with_key_of(&submitted.submission) {
                // What is held counts against the hard limit here as it does
                // for a waiting submission.
                scheduler.check_submission(&submitted.submission, self.queued_bytes)?;
                return scheduler.submit_minted(submitted);
            }
        }
        scheduler.check_submission(&submitted.submission, self.queued_bytes)?;
        self.queued_bytes += submitted.submission.serialized_input.len() as u64;
        let task = submitted.task_id.clone();
        self.queued.push(submitted);
        Ok(task)
    }

    /// Cancels `task`: one still waiting here is dropped, so it never runs,
    /// and answers `Cancelled` as a task that never started; any other is
    /// the scheduler's to cancel.
    pub fn cancel<C: Clock, I: IdGenerator, O: Observer>(
        &mut self,
        scheduler: &mut Scheduler<C, I, O>,
        task: &TaskId,
    ) -> Result<Cancellation, CancelRejection> {
        if let Some(at) = self.queued.iter().position(|queued| queued.task_id == *task) {
            let dropped = self.queued.remove(at);
            self.queued_bytes -= dropped.submission.serialized_input.len() as u64;
            return Ok(Cancellation::Cancelled { was_running: false });
        }
        scheduler.cancel(task)
    }

    /// Ends the continuation of `task`. Refused for want of leadership, it
    /// is kept and ended by the first hand-over once the scheduler leads,
    /// so the task is not left continuing for ever; the refusal is still
    /// returned.
    pub fn end_continuation<C: Clock, I: IdGenerator, O: Observer>(
        &mut self,
        scheduler: &mut Scheduler<C, I, O>,
        task: &TaskId,
    ) -> Result<bool, ContinuationRejection> {
        let ended = scheduler.end_continuation(task);
        if ended.is_err() && !self.unended.contains(task) {
            self.unended.push(task.clone());
        }
        ended
    }

    /// Once the scheduler leads: ends the continuations kept, which frees
    /// the memory they held, then records what waits here. Does nothing
    /// while it does not lead. Call it after every change that may free
    /// capacity or a key.
    pub fn hand_over<C: Clock, I: IdGenerator, O: Observer>(
        &mut self,
        scheduler: &mut Scheduler<C, I, O>,
    ) {
        if !scheduler.is_leader() {
            return;
        }
        for task in std::mem::take(&mut self.unended) {
            // `Ok(false)` is a task that finished or was cancelled meanwhile and has
            // nothing to end; the scheduler leads (checked above), so `NotLeader`
            // cannot occur.
            let _ = scheduler.end_continuation(&task);
        }
        self.record_queued(scheduler);
    }

    /// Whether a submission of `submission`'s coalescing key still waits here.
    fn waits_with_key_of(&self, submission: &Submission) -> bool {
        coalescing_key_of(submission).is_some_and(|key| {
            self.queued
                .iter()
                .any(|queued| coalescing_key_of(&queued.submission) == Some(key))
        })
    }

    /// Records the submissions not yet recorded: those queued before the
    /// scheduler led, and those held behind a refused submission of their
    /// key. One the scheduler refuses (its record would pass the size limit
    /// while it carries the input of the generation it supersedes) stays
    /// here rather than being lost, and the next hand-over tries again. So do
    /// the later submissions of its coalescing key, in order, because
    /// recording one ahead of it would reverse the generations; submissions of
    /// other keys, and those that do not coalesce, are recorded without
    /// waiting for it. A refused submission without a key holds nothing back.
    fn record_queued<C: Clock, I: IdGenerator, O: Observer>(
        &mut self,
        scheduler: &mut Scheduler<C, I, O>,
    ) {
        let mut held: BTreeSet<(String, String)> = BTreeSet::new();
        for submitted in std::mem::take(&mut self.queued) {
            let key = coalescing_key_of(&submitted.submission)
                .map(|(definition, key)| (definition.to_owned(), key.to_owned()));
            if key.as_ref().is_some_and(|key| held.contains(key)) {
                self.queued.push(submitted);
                continue;
            }
            let kept = submitted.clone();
            if scheduler.submit_minted(submitted).is_err() {
                held.extend(key);
                self.queued.push(kept);
            }
        }
        self.queued_bytes = self
            .queued
            .iter()
            .map(|submitted| submitted.submission.serialized_input.len() as u64)
            .sum();
    }
}
