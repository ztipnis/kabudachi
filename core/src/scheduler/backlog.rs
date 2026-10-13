//! Work a scheduler cannot take yet, kept in order until it can.

use std::collections::BTreeSet;

use crate::protocol::ids::{CompositeId, IdGenerator, TaskId};
use crate::time::Clock;

use super::composite::{CompositeState, CompositeStatus, MemberCounts, SubmittedComposite};
use super::{
    CancelRejection, Cancellation, Observer, Scheduler, Submission, SubmitRejection, Submitted,
};

/// What its scheduler could not take yet: submissions and composites made
/// before it led. Once it leads, [`Self::hand_over`] records the
/// submissions in the order they were made within a coalescing key, then the
/// composites in the order they were made. A
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
    /// Composites not yet recorded, in the order they were made.
    queued_composites: Vec<SubmittedComposite>,
    /// Composites cancelled before they were recorded, whose clients are told
    /// once the scheduler leads: a scheduler that is taking over forgets what
    /// it said before it did.
    dropped_composites: Vec<SubmittedComposite>,
    /// The input bytes `queued` holds, and the record bytes of
    /// `queued_composites`.
    queued_bytes: u64,
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
    /// here. Refused when it is too large to claim or would pass the hard
    /// limit with everything waiting here; while the scheduler leads and no
    /// submission of its key waits, the scheduler's own refusal
    /// (`RecordTooLarge`, `KeyNotReady`, `KeyBackpressure`) is returned
    /// instead.
    pub fn submit<C: Clock, I: IdGenerator, O: Observer>(
        &mut self,
        scheduler: &mut Scheduler<C, I, O>,
        submission: Submission,
    ) -> Result<TaskId, SubmitRejection> {
        let submitted = scheduler.mint(submission);
        self.submit_minted(scheduler, submitted)
    }

    /// Records or queues `submitted`, which already has its task id, as
    /// [`Self::submit`] does.
    pub fn submit_minted<C: Clock, I: IdGenerator, O: Observer>(
        &mut self,
        scheduler: &mut Scheduler<C, I, O>,
        submitted: Submitted,
    ) -> Result<TaskId, SubmitRejection> {
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
        self.queued_bytes += submitted.submission.payload_bytes();
        let task = submitted.task_id.clone();
        self.queued.push(submitted);
        Ok(task)
    }

    /// Records or queues `minted`, a composite that already has its id, as
    /// [`Self::submit_minted`] does a task: a scheduler that leads records it
    /// now, after what waits here; otherwise it waits here, counted with
    /// everything else waiting against the hard limit. A step that takes an
    /// input is refused either way.
    pub fn submit_composite<C: Clock, I: IdGenerator, O: Observer>(
        &mut self,
        scheduler: &mut Scheduler<C, I, O>,
        minted: SubmittedComposite,
    ) -> Result<CompositeId, SubmitRejection> {
        if scheduler.is_leader() {
            self.record_queued(scheduler);
            scheduler.check_composite(&minted, self.queued_bytes)?;
            return scheduler.submit_composite(minted);
        }
        scheduler.check_composite(&minted, self.queued_bytes)?;
        self.queued_bytes += minted.memory_bytes();
        let id = minted.composite_id.clone();
        self.queued_composites.push(minted);
        Ok(id)
    }

    /// Cancels the composite `id`: one still waiting here is dropped, so it
    /// never runs, and its client is told it was cancelled once the scheduler
    /// leads; any other is the scheduler's to cancel.
    pub fn cancel_composite<C: Clock, I: IdGenerator, O: Observer>(
        &mut self,
        scheduler: &mut Scheduler<C, I, O>,
        id: &CompositeId,
    ) -> Result<Cancellation, CancelRejection> {
        if let Some(at) = self.queued_composites.iter().position(|queued| queued.composite_id == *id) {
            let dropped = self.queued_composites.remove(at);
            self.queued_bytes -= dropped.memory_bytes();
            self.dropped_composites.push(dropped);
            return Ok(Cancellation::Cancelled { was_running: false });
        }
        scheduler.cancel_composite(id)
    }

    /// Where the composite `id` stands: one waiting here is running with all
    /// its members pending.
    pub fn composite_status<C: Clock, I: IdGenerator, O: Observer>(
        &self,
        scheduler: &Scheduler<C, I, O>,
        id: &CompositeId,
    ) -> Option<CompositeStatus> {
        if let Some(waiting) = self.queued_composites.iter().find(|queued| queued.composite_id == *id) {
            return Some(CompositeStatus {
                state: CompositeState::Running,
                next_stage: 0,
                counts: MemberCounts { pending: waiting.step.stages.len() as u64, ..MemberCounts::default() },
            });
        }
        scheduler.composite_status(id)
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
            self.queued_bytes -= dropped.submission.payload_bytes();
            return Ok(Cancellation::Cancelled { was_running: false });
        }
        scheduler.cancel(task)
    }

    /// Once the scheduler leads: records what waits here. Does nothing
    /// while it does not lead. Call it after every change that may free
    /// capacity or a key.
    pub fn hand_over<C: Clock, I: IdGenerator, O: Observer>(
        &mut self,
        scheduler: &mut Scheduler<C, I, O>,
    ) {
        if !scheduler.is_leader() {
            return;
        }
        self.record_queued(scheduler);
        for dropped in std::mem::take(&mut self.dropped_composites) {
            scheduler.announce_dropped(&dropped);
        }
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
        for composite in std::mem::take(&mut self.queued_composites) {
            // A composite the scheduler refuses (its record would not fit
            // yet) stays here and the next hand-over tries again.
            if scheduler.submit_composite(composite.clone()).is_err() {
                self.queued_composites.push(composite);
            }
        }
        self.queued_bytes = self
            .queued
            .iter()
            .map(|submitted| submitted.submission.payload_bytes())
            .sum::<u64>()
            + self.queued_composites.iter().map(SubmittedComposite::memory_bytes).sum::<u64>();
    }
}
