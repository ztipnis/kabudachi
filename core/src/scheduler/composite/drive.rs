//! How the leader drives a composite: it starts a flow's first stage or every
//! member of a group, settles each member as its task ends, starts the next
//! stage on the result of the one before, and ends the composite when its
//! policy says so.
//!
//! The leader never decodes user data: a stage's result is the digest of the
//! blob it was stored under, and the next stage is given the digests.

use std::collections::BTreeMap;

use crate::manifest::{Failure, Manifest, ManifestEntry};
use crate::protocol::digest::Digest;
use crate::protocol::ids::{CompositeId, IdGenerator, TaskId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, Duration, WallTime};

use super::held::Composite;
use super::{
    BoundInput, BoundStep, CompositeFinished, CompositeKind, CompositeParent, CompositeState,
    CompositeStatus, InputShape, MemberOutcome, PAGE_ENTRIES, Stage, Step, SubmittedComposite, failure_of,
    pages_manifest,
};
use crate::blob::BlobKey;
use crate::scheduler::{Event, Observer, Scheduler, Submission, SubmitRejection, Submitted};

impl BoundStep {
    /// The submission of this step as a Task. The input is the step's own
    /// bytes when it is bound to a value; when it is bound to blobs, none, since
    /// those belong to the composite and its member's worker reads them through
    /// the composite's record; otherwise the manifest of what it is given.
    fn submission(&self, input: Option<Manifest>) -> Submission {
        let serialized_input = match &self.input {
            BoundInput::Value(bytes) => bytes.clone(),
            BoundInput::Chunked(_) | BoundInput::Previous => Vec::new(),
        };
        let mut submission = Submission::new(
            self.definition.clone(),
            self.version,
            serialized_input,
            self.options.queue.clone(),
        )
        .with_retries(self.options.retries);
        submission.ephemeral = self.options.ephemeral;
        submission.non_retriable = self.options.non_retriable;
        submission.reconnect_timeout = self
            .options
            .reconnect_timeout
            .map(|timeout| Duration::from_millis(u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)));
        match (&self.input, input) {
            (BoundInput::Previous, Some(manifest)) => submission.with_input_manifest(manifest),
            _ => submission,
        }
    }
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Gives `step` a fresh composite id and stamps it with the wall clock
    /// now. Records nothing, so a client mints before it knows who leads, and
    /// asking again after a refusal names the same composite.
    pub fn mint_composite(&self, step: Step, client: Option<WorkerId>) -> SubmittedComposite {
        SubmittedComposite {
            composite_id: self.ids.next_composite_id(),
            submitted_at: WallTime::now(&self.clock),
            step,
            client,
        }
    }

    /// Records a composite a client submitted and starts it: a flow's first
    /// stage, or every member of a group or map. Only a leader records one.
    /// An id it already holds is a client asking again: it is answered with
    /// the id and nothing changes. A step that would need an input is
    /// refused, since nothing runs before a submitted composite.
    pub fn submit_composite(&mut self, submitted: SubmittedComposite) -> Result<CompositeId, SubmitRejection> {
        let outcome = self.record_composite(submitted);
        self.end_call();
        outcome
    }

    fn record_composite(&mut self, submitted: SubmittedComposite) -> Result<CompositeId, SubmitRejection> {
        if !self.check_leader() {
            return Err(SubmitRejection::NotLeader);
        }
        let SubmittedComposite { composite_id, submitted_at, step, client } = submitted;
        if self.composites.held.contains_key(&composite_id) {
            return Ok(composite_id);
        }
        if step.takes_input() {
            return Err(SubmitRejection::StepTakesInput);
        }
        self.check_composite_room(&composite_id, &step)?;
        self.create_composite(composite_id.clone(), step, None, client, None, submitted_at);
        Ok(composite_id)
    }

    /// Refuses a composite whose record would not fit one record, or whose
    /// description would take memory past the hard limit.
    fn check_composite_room(&self, id: &CompositeId, step: &Step) -> Result<(), SubmitRejection> {
        let size = super::records::first_record_len(id, step);
        if size > Self::record_limit() {
            return Err(SubmitRejection::RecordTooLarge { size, limit: Self::record_limit() });
        }
        self.budget.check_room(size, || 0)
    }

    /// Holds a new composite, publishes it, and starts what it starts with.
    pub(in crate::scheduler) fn create_composite(
        &mut self,
        id: CompositeId,
        step: Step,
        parent: Option<CompositeParent>,
        client: Option<WorkerId>,
        input: Option<(Manifest, InputShape)>,
        submitted_at: WallTime,
    ) {
        let bytes = super::records::first_record_len(&id, &step);
        self.budget.take(bytes);
        let root = match &parent {
            Some(CompositeParent::Composite { id: parent, .. }) => self.composites.held[parent].root.clone(),
            _ => id.clone(),
        };
        let starts: Vec<u64> = match step.kind {
            CompositeKind::Flow => vec![0],
            _ => (0..step.stages.len() as u64).collect(),
        };
        let empty = step.stages.is_empty();
        self.composites.held.insert(
            id.clone(),
            Composite {
                step,
                parent,
                root,
                client,
                input,
                state: CompositeState::Running,
                next_stage: 0,
                outcomes: BTreeMap::new(),
                failure: None,
                submitted_at,
                bytes,
            },
        );
        self.composites.unpublished.insert(id.clone());
        if empty {
            // A map over an empty list has no member to wait for.
            self.finish_composite(&id, CompositeState::Succeeded, None);
        }
        for index in starts {
            self.start_stage(&id, index);
        }
        self.update_pressure();
    }

    /// Starts stage or member `index` of `id`: a Task for a task step, a
    /// child composite for a composite step, each on the input the step takes.
    /// Starting what already exists changes nothing, and nothing starts in a
    /// composite that is over.
    pub(in crate::scheduler) fn start_stage(&mut self, id: &CompositeId, index: u64) {
        let composite = &self.composites.held[id];
        if composite.state != CompositeState::Running {
            return;
        }
        let input = self.stage_input(id, index);
        match composite.step.stages[index as usize].clone() {
            Stage::Task(bound) => self.submit_member(id, index, &bound, input.map(|(manifest, _)| manifest)),
            Stage::Composite(step) => {
                let child = id.child(index);
                if self.composites.held.contains_key(&child) {
                    return;
                }
                let submitted_at = WallTime::now(&self.clock);
                let parent = CompositeParent::Composite { id: id.clone(), index };
                self.create_composite(child, step, Some(parent), None, input, submitted_at);
            }
        }
    }

    /// What stage or member `index` of `id` is given: a flow's later stage
    /// takes the stage before's result; anything else takes the composite's
    /// own input, if it has one.
    fn stage_input(&self, id: &CompositeId, index: u64) -> Option<(Manifest, InputShape)> {
        let composite = &self.composites.held[id];
        if composite.step.kind == CompositeKind::Flow && index > 0 {
            let previous = composite.outcomes.get(&(index - 1))?;
            return Some(self.result_of(previous));
        }
        composite.input.clone()
    }

    /// A settled stage's result, as the next stage's input.
    fn result_of(&self, outcome: &MemberOutcome) -> (Manifest, InputShape) {
        match outcome {
            MemberOutcome::Value { digest, producer } => (
                Manifest::new(vec![ManifestEntry::Blob(BlobKey::from(digest))], 0),
                InputShape::Value(producer.clone()),
            ),
            MemberOutcome::Child(child) => (self.results_manifest(child), InputShape::List),
            MemberOutcome::Failed(_) => unreachable!("no stage starts after a failed one"),
        }
    }

    /// The manifest of a held composite's pages.
    fn results_manifest(&self, id: &CompositeId) -> Manifest {
        pages_manifest(id, self.composites.held[id].pages())
    }

    /// Records member `index`'s Task, unless the scheduler holds it already
    /// (a takeover driving the composite again). A Task the scheduler refuses
    /// settles the member as failed with the refusal's kind.
    fn submit_member(&mut self, id: &CompositeId, index: u64, bound: &BoundStep, input: Option<Manifest>) {
        let task_id = id.member_task(index);
        if self.tasks.contains_key(&task_id) {
            return;
        }
        let submitted = Submitted::received(
            task_id,
            WallTime::now(&self.clock),
            bound.submission(input).as_member_of(id.clone(), index),
            &self.clock,
        );
        if let Err(refused) = self.record_submission(submitted) {
            let failure = Failure { kind: refused.kind().to_owned(), message: refused.to_string() };
            self.settle_member(id, index, MemberOutcome::Failed(failure));
        }
    }

    /// Called whenever a task finishes: if it is a member of a composite this
    /// scheduler holds, the member settles with what the task ended as.
    pub(in crate::scheduler) fn member_finished(&mut self, task_id: &TaskId) {
        let Some(composite) = self.tasks[task_id].composite_id() else {
            return;
        };
        let index = self.tasks[task_id].composite_index;
        let outcome = self.member_outcome_of(task_id);
        self.settle_member(&composite, index, outcome);
    }

    /// What a finished task ended as, for its composite: its result, its
    /// continuation's results, or why it failed.
    fn member_outcome_of(&self, task_id: &TaskId) -> MemberOutcome {
        let run = &self.runs[&self.current_run[task_id]];
        match run.current_state() {
            TaskRunState::Succeeded => {
                let continuation = CompositeId::continuation_of(&run.task_run_id());
                match self.composites.held.get(&continuation) {
                    Some(child) if child.state == CompositeState::Succeeded => MemberOutcome::Child(continuation),
                    Some(child) => MemberOutcome::Failed(child.failure_or_cancelled()),
                    None => MemberOutcome::Value {
                        digest: run
                            .result_digest
                            .as_ref()
                            .and_then(|digest| Digest::try_from(digest).ok())
                            .expect("a succeeded run has a digest"),
                        producer: self.tasks[task_id].task_definition_id(),
                    },
                }
            }
            state => MemberOutcome::Failed(failure_of(state, &run.failure_kind)),
        }
    }

    /// Member `index` of `id` settled as `outcome`: it is recorded in its
    /// page, and the composite moves on. Once the composite is over (failed,
    /// cancelled) an outcome is still recorded, and changes nothing else.
    pub(in crate::scheduler) fn settle_member(&mut self, id: &CompositeId, index: u64, outcome: MemberOutcome) {
        let Some(composite) = self.composites.held.get_mut(id) else {
            // The composite is not known for certain yet: the member's own
            // record holds the outcome, and reconciliation reads it there.
            return;
        };
        if composite.outcomes.contains_key(&index) {
            return;
        }
        composite.outcomes.insert(index, outcome.clone());
        let running = composite.state == CompositeState::Running;
        self.composites.unpublished.insert(id.clone());
        self.composites.unpublished_pages.insert((id.clone(), (index / PAGE_ENTRIES) as u32));
        if running {
            self.advance(id, index, outcome);
        }
    }

    fn advance(&mut self, id: &CompositeId, index: u64, outcome: MemberOutcome) {
        let composite = &self.composites.held[id];
        let stages = composite.step.stages.len() as u64;
        let settled = composite.outcomes.len() as u64;
        match (composite.step.kind, outcome) {
            (_, MemberOutcome::Failed(failure)) if composite.fails_on_first_failure() => {
                self.finish_composite(id, CompositeState::Failed, Some(failure));
            }
            (CompositeKind::Flow, _) if index + 1 < stages => {
                self.composites.held.get_mut(id).expect("held above").next_stage = index + 1;
                self.start_stage(id, index + 1);
            }
            (CompositeKind::Flow, _) => self.finish_composite(id, CompositeState::Succeeded, None),
            _ if settled == stages => self.finish_composite(id, CompositeState::Succeeded, None),
            _ => {}
        }
    }

    /// `id` is over as `state`: its budget is given back, its client (or the
    /// task it continues) is told, and its parent composite settles the
    /// member it is.
    pub(in crate::scheduler) fn finish_composite(
        &mut self,
        id: &CompositeId,
        state: CompositeState,
        failure: Option<Failure>,
    ) {
        let now = self.clock.now();
        let composite = self.composites.held.get_mut(id).expect("only a held composite finishes");
        if composite.state != CompositeState::Running {
            return;
        }
        composite.state = state;
        composite.failure = failure.clone();
        let (bytes, parent, client) = (composite.bytes, composite.parent.clone(), composite.client.clone());
        let pages = composite.pages();
        self.budget.give_back(bytes);
        self.composites.record_finished(id, now);
        self.composites.unpublished.insert(id.clone());
        let parent_task = match &parent {
            Some(CompositeParent::Task(task)) => Some(task.clone()),
            _ => None,
        };
        if client.is_some() || parent_task.is_some() {
            self.events.push(Event::CompositeFinished(CompositeFinished {
                composite_id: id.clone(),
                state,
                failure,
                pages,
                client,
                parent_task,
            }));
        }
        match parent {
            Some(CompositeParent::Composite { id: parent, index }) => {
                let outcome = match state {
                    CompositeState::Succeeded => MemberOutcome::Child(id.clone()),
                    _ => MemberOutcome::Failed(self.composites.held[id].failure_or_cancelled()),
                };
                self.settle_member(&parent, index, outcome);
            }
            Some(CompositeParent::Task(task)) => {
                self.finish_continuation(&task);
            }
            None => {}
        }
        self.update_pressure();
    }

    /// Where the composite `id` stands, or `None` for one this scheduler
    /// does not hold.
    pub fn composite_status(&self, id: &CompositeId) -> Option<CompositeStatus> {
        let composite = self.composites.held.get(id)?;
        Some(CompositeStatus {
            state: composite.state,
            next_stage: composite.next_stage,
            counts: composite.counts(),
        })
    }
}
