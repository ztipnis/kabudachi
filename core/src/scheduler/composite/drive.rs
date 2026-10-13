//! How the leader drives a composite: it starts a flow's first stage or every
//! member of a group, settles each member as its task ends, starts the next
//! stage on the result of the one before, and ends the composite when its
//! policy says so.
//!
//! The leader never decodes user data: a stage's result is the digest of the
//! blob it was stored under, and the next stage is given the digests.

use std::collections::BTreeMap;

use prost::Message as _;

use crate::manifest::{Failure, Manifest, ManifestEntry};
use crate::protocol::digest::Digest;
use crate::protocol::generated;
use crate::protocol::ids::{CompositeId, IdGenerator, TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, Duration, WallTime};

use crate::blob::BlobKey;
use crate::scheduler::{
    CancelRejection, Cancellation, Event, Observer, Scheduler, Submission, SubmitRejection, Submitted,
};

use super::held::Composite;
use super::{
    BoundInput, BoundStep, CompositeFinished, CompositeKind, CompositeParent, CompositeState,
    CompositeStatus, EXPANSION_DEFINITION, InputShape, LOST_KIND, MemberOutcome, PAGE_ENTRIES, Stage,
    Step, SubmittedComposite, failure_of, pages_manifest,
};

/// The results of a composite a stage is to take were not found.
struct ResultsGone;

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
        if self.composites.held.contains_key(&submitted.composite_id) {
            return Ok(submitted.composite_id);
        }
        self.check_composite(&submitted, 0)?;
        let SubmittedComposite { composite_id, submitted_at, step, client } = submitted;
        self.create_composite(composite_id.clone(), step, None, client, None, submitted_at);
        Ok(composite_id)
    }

    /// Whether `submitted` could be recorded now with `queued_bytes` more
    /// promised to composites and tasks not yet recorded: it needs no input,
    /// its record fits one record, and what starting it takes of memory (see
    /// [`SubmittedComposite::memory_bytes`]) fits under the hard limit with
    /// them. Records nothing, so it needs no leadership.
    pub fn check_composite(&self, submitted: &SubmittedComposite, queued_bytes: u64) -> Result<(), SubmitRejection> {
        if submitted.step.takes_input() {
            return Err(SubmitRejection::StepTakesInput);
        }
        let size = submitted.record_bytes();
        if size > Self::record_limit() {
            return Err(SubmitRejection::RecordTooLarge { size, limit: Self::record_limit() });
        }
        self.budget.check_room(submitted.memory_bytes() + queued_bytes, || 0)
    }

    /// Tells the client of `dropped`, a composite cancelled before the
    /// scheduler ever recorded it, that it is over, as it is told of one
    /// cancelled while it runs.
    pub(in crate::scheduler) fn announce_dropped(&mut self, dropped: &SubmittedComposite) {
        if let Some(client) = dropped.client.clone() {
            self.events.push(Event::CompositeFinished(CompositeFinished {
                composite_id: dropped.composite_id.clone(),
                state: CompositeState::Cancelled,
                failure: None,
                pages: 0,
                client: Some(client),
                parent_task: None,
                settled_by: Vec::new(),
            }));
        }
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
        let retained_by = self.retainer_of(parent.as_ref());
        let starts: Vec<u64> = match step.kind {
            CompositeKind::Flow => vec![0],
            _ => (0..step.stages.len() as u64).collect(),
        };
        let empty = step.stages.is_empty();
        let expands = step.expands();
        self.composites.held.insert(
            id.clone(),
            Composite {
                step,
                parent,
                root,
                retained_by,
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
        if expands {
            self.submit_expansion(&id);
        } else {
            for index in starts {
                self.start_stage(&id, index);
            }
        }
        self.update_pressure();
    }

    /// Records the expansion run that splits the list an expanding map is
    /// given. The run is the map's member 0; the map's members proper come
    /// from the step it returns.
    pub(super) fn submit_expansion(&mut self, id: &CompositeId) {
        let composite = &self.composites.held[id];
        let Stage::Task(template) = &composite.step.stages[0] else {
            unreachable!("a map that expands has one task member");
        };
        let (source, shape) = composite
            .input
            .clone()
            .expect("only a stage after another expands, and it is given that stage's result");
        let expansion = generated::Expansion {
            source: Some(source.into()),
            then: Some(generated::expansion::Then::Map(template.into())),
            producer: match shape {
                InputShape::Value(producer) => Some(producer.into()),
                InputShape::List => None,
            },
            on_error: generated::OnError::from(composite.step.on_error) as i32,
        };
        let submission = Submission::new(
            TaskDefinitionId::new(EXPANSION_DEFINITION),
            0,
            expansion.encode_to_vec(),
            template.options.queue.clone(),
        )
        .as_member_of(id.clone(), 0);
        let submitted = Submitted::received(id.member_task(0), WallTime::now(&self.clock), submission, &self.clock);
        if let Err(refused) = self.record_submission(submitted) {
            let failure = Failure { kind: refused.kind().to_owned(), message: refused.to_string() };
            self.settle_member(id, 0, MemberOutcome::Failed(failure));
        }
    }

    /// Starts the composite `CompositeId::continuation_of(run)` from the step
    /// stored on `run`, a certified run of `task_id`, with the task as its
    /// parent. The step was committed by the revision that certified the run;
    /// the composite is derived from it, so the same run always makes the
    /// same composite and one already held is kept as it is.
    pub(in crate::scheduler) fn commit_continuation(&mut self, task_id: &TaskId, run: &TaskRunId) {
        let id = CompositeId::continuation_of(run);
        if self.composites.held.contains_key(&id) {
            return;
        }
        let Some(step) = self.continuation_of_run(run) else {
            // Nothing to drive: the stored step cannot be read (reported when read).
            return;
        };
        let client = self.tasks[task_id].client();
        let parent = CompositeParent::Task(task_id.clone());
        let submitted_at = WallTime::now(&self.clock);
        self.create_composite(id, step, Some(parent), client, None, submitted_at);
    }

    /// The composite that keeps one with this `parent` until it is
    /// forgotten itself (see `Composite::retained_by`).
    pub(super) fn retainer_of(&self, parent: Option<&CompositeParent>) -> Option<CompositeId> {
        match parent? {
            CompositeParent::Composite { id, .. } => Some(id.clone()),
            CompositeParent::Task(task) => self.tasks.get(task)?.composite_id(),
        }
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
        let stage = composite.step.stages[index as usize].clone();
        let started = match &stage {
            Stage::Task(_) => self.tasks.contains_key(&id.member_task(index)),
            Stage::Composite(_) => self.composites.held.contains_key(&id.child(index)),
        };
        if started {
            return;
        }
        let Ok(input) = self.stage_input(id, index) else {
            let failure = Failure {
                kind: LOST_KIND.to_owned(),
                message: "the results of the stage before were not found".to_owned(),
            };
            self.finish_composite(id, CompositeState::Failed, Some(failure));
            return;
        };
        match stage {
            Stage::Task(bound) => self.submit_member(id, index, &bound, input.map(|(manifest, _)| manifest)),
            Stage::Composite(step) => {
                let child = id.child(index);
                let submitted_at = WallTime::now(&self.clock);
                let parent = CompositeParent::Composite { id: id.clone(), index };
                self.create_composite(child, step, Some(parent), None, input, submitted_at);
            }
        }
    }

    /// What stage or member `index` of `id` is given: a flow's later stage
    /// takes the stage before's result; anything else takes the composite's
    /// own input, if it has one. Fails when the stage before ended in a
    /// composite whose results this scheduler no longer holds.
    fn stage_input(&self, id: &CompositeId, index: u64) -> Result<Option<(Manifest, InputShape)>, ResultsGone> {
        let composite = &self.composites.held[id];
        if composite.step.kind == CompositeKind::Flow && index > 0 {
            let Some(previous) = composite.outcomes.get(&(index - 1)) else {
                return Ok(None);
            };
            return self.result_of(previous).map(Some);
        }
        Ok(composite.input.clone())
    }

    /// A settled stage's result, as the next stage's input.
    fn result_of(&self, outcome: &MemberOutcome) -> Result<(Manifest, InputShape), ResultsGone> {
        match outcome {
            MemberOutcome::Value { digest, producer } => Ok((
                Manifest::new(vec![ManifestEntry::Blob(BlobKey::from(digest))], 0),
                InputShape::Value(producer.clone()),
            )),
            MemberOutcome::Child(child) => Ok((self.results_manifest(child).ok_or(ResultsGone)?, InputShape::List)),
            MemberOutcome::Failed(_) => unreachable!("no stage starts after a failed one"),
        }
    }

    /// The manifest of a held composite's pages, if it is held.
    /// An expanding map's results are those of the members its expansion run
    /// returned.
    fn results_manifest(&self, id: &CompositeId) -> Option<Manifest> {
        let composite = self.composites.held.get(id)?;
        if composite.step.expands() {
            if let Some(MemberOutcome::Child(members)) = composite.outcomes.get(&0) {
                return self.results_manifest(members);
            }
        }
        Some(pages_manifest(id, composite.pages()))
    }

    /// Records member `index`'s Task. A Task the scheduler refuses settles
    /// the member as failed with the refusal's kind.
    fn submit_member(&mut self, id: &CompositeId, index: u64, bound: &BoundStep, input: Option<Manifest>) {
        let submitted = Submitted::received(
            id.member_task(index),
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
        if let Some(outcome) = self.member_outcome_of(task_id) {
            let outer = self.composites.settling.len();
            self.composites.settling.push(task_id.clone());
            self.settle_member(&composite, index, outcome);
            self.composites.settling.truncate(outer);
        }
    }

    /// What a finished task ended as, for its composite: its result, its
    /// continuation's results, or why it failed. `None` while the
    /// continuation it returned is still running.
    pub(super) fn member_outcome_of(&self, task_id: &TaskId) -> Option<MemberOutcome> {
        let run = &self.runs[&self.current_run[task_id]];
        let outcome = match run.current_state() {
            TaskRunState::Succeeded if run.continuation.is_some() => {
                let continuation = CompositeId::continuation_of(&run.task_run_id());
                match self.composites.held.get(&continuation) {
                    Some(child) if child.state == CompositeState::Running => return None,
                    Some(child) if child.state == CompositeState::Succeeded => MemberOutcome::Child(continuation),
                    Some(child) => MemberOutcome::Failed(child.failure_or_cancelled()),
                    None => MemberOutcome::Failed(Failure {
                        kind: LOST_KIND.to_owned(),
                        message: "the record of the step the task returned was not found".to_owned(),
                    }),
                }
            }
            TaskRunState::Succeeded => match run.result_digest.as_ref().and_then(|digest| Digest::try_from(digest).ok()) {
                Some(digest) => MemberOutcome::Value { digest, producer: self.tasks[task_id].task_definition_id() },
                None => MemberOutcome::Failed(Failure {
                    kind: LOST_KIND.to_owned(),
                    message: "the run succeeded without a readable result digest".to_owned(),
                }),
            },
            state => MemberOutcome::Failed(failure_of(state, &run.failure_kind, &run.failure_message)),
        };
        Some(outcome)
    }

    /// Member `index` of `id` settled as `outcome`: it is recorded in its
    /// page, and the composite moves on. Once the composite is over (failed,
    /// cancelled) an outcome is still recorded, and changes nothing else.
    pub(in crate::scheduler) fn settle_member(&mut self, id: &CompositeId, index: u64, outcome: MemberOutcome) {
        if !self.record_member(id, index, outcome.clone()) {
            return;
        }
        // A composite waiting on a record that is not known yet moves on
        // when recovery has it, from what its members' records say.
        let running = self.composites.held[id].state == CompositeState::Running;
        if running && !(self.reconciliation.has_unknown() && self.holds_composite_back(id)) {
            self.advance(id, index, outcome);
        }
    }

    /// Records member `index`'s `outcome` in its page and marks the page and
    /// the composite changed, without moving the composite on. Says whether
    /// it did: not for a composite this scheduler does not hold (the
    /// member's own record holds the outcome, and reconciliation reads it
    /// there) or for a member already settled.
    pub(super) fn record_member(&mut self, id: &CompositeId, index: u64, outcome: MemberOutcome) -> bool {
        let Some(composite) = self.composites.held.get_mut(id) else {
            return false;
        };
        if composite.outcomes.contains_key(&index) {
            return false;
        }
        let pages_before = composite.pages();
        composite.outcomes.insert(index, outcome);
        let pages_after = composite.pages();
        self.composites.unpublished.insert(id.clone());
        self.composites.unpublished_pages.insert((id.clone(), (index / PAGE_ENTRIES) as u32));
        // The record now names every page up to the newest, so the pages
        // below the one that grew it are published, empty, with it.
        for page in pages_before..pages_after {
            self.composites.unpublished_pages.insert((id.clone(), page));
        }
        true
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
        // A cancelled composite is marked cancelled before its members are
        // cancelled, so that they settle nothing; it finishes once they are.
        let ended = self.composites.is_finished(id);
        let composite = self.composites.held.get_mut(id).expect("only a held composite finishes");
        let being_cancelled = state == CompositeState::Cancelled && composite.state == CompositeState::Cancelled && !ended;
        if composite.state != CompositeState::Running && !being_cancelled {
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
                settled_by: self.composites.settling.clone(),
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
                // A task that finished before its continuation did, as
                // when a successor settles what its predecessor left, is
                // settled in its own composite now.
                if !self.finish_continuation(&task) && self.retention.holds(&task) {
                    self.member_finished(&task);
                }
            }
            None => {}
        }
        self.update_pressure();
    }

    /// Cancels a composite: its current stage, or every unfinished member,
    /// and with them every child composite. `AlreadyFinished` once it is over;
    /// `UnknownTask` for an id it does not hold.
    pub fn cancel_composite(&mut self, id: &CompositeId) -> Result<Cancellation, CancelRejection> {
        let outcome = self.cancel_composite_now(id);
        self.end_call();
        outcome
    }

    pub(in crate::scheduler) fn cancel_composite_now(&mut self, id: &CompositeId) -> Result<Cancellation, CancelRejection> {
        if !self.check_leader() {
            return Err(CancelRejection::NotLeader);
        }
        if self.reconciliation.is_uncertain_composite(id) {
            return Err(CancelRejection::NotReady);
        }
        let Some(composite) = self.composites.held.get_mut(id) else {
            return Ok(Cancellation::UnknownTask);
        };
        if composite.state != CompositeState::Running {
            return Ok(Cancellation::AlreadyFinished);
        }
        // Cancelled first, so the members cancelled below start and settle nothing.
        composite.state = CompositeState::Cancelled;
        let unsettled: Vec<u64> = (0..composite.step.stages.len() as u64)
            .filter(|index| !composite.outcomes.contains_key(index))
            .collect();
        let mut was_running = false;
        for index in unsettled {
            was_running |= self.cancel_stage(id, index);
        }
        self.finish_composite(id, CompositeState::Cancelled, None);
        Ok(Cancellation::Cancelled { was_running })
    }

    /// Cancels stage or member `index` of `id` if it was started; says
    /// whether a worker held anything of it.
    fn cancel_stage(&mut self, id: &CompositeId, index: u64) -> bool {
        let task = id.member_task(index);
        if self.tasks.contains_key(&task) {
            return matches!(self.cancel_task(&task), Ok(Cancellation::Cancelled { was_running: true }));
        }
        matches!(
            self.cancel_composite_now(&id.child(index)),
            Ok(Cancellation::Cancelled { was_running: true })
        )
    }

    /// Where the composite `id` stands, or `None` for one this scheduler
    /// does not hold.
    pub fn composite_status(&self, id: &CompositeId) -> Option<CompositeStatus> {
        let composite = self.composites.held.get(id)?;
        Some(CompositeStatus {
            state: composite.state,
            next_stage: composite.next_stage,
            counts: composite.counts(self.running_members(id)),
        })
    }

    /// How many members of the held composite `id` are Tasks that have not
    /// settled and whose current run is claimed or started.
    pub(in crate::scheduler) fn running_members(&self, id: &CompositeId) -> u64 {
        let composite = &self.composites.held[id];
        (0..composite.step.stages.len() as u64)
            .filter(|index| !composite.outcomes.contains_key(index))
            .filter(|index| {
                self.current_run.get(&id.member_task(*index)).is_some_and(|run| {
                    matches!(self.runs[run].current_state(), TaskRunState::Claimed | TaskRunState::Running)
                })
            })
            .count() as u64
    }
}
