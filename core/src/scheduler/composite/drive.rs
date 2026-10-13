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

use super::held::{Composite, Outcomes};
use super::{
    BoundInput, BoundStep, CompositeFinished, CompositeKind, CompositeParent, CompositeState,
    CompositeStatus, EXPANSION_DEFINITION, InputShape, LOST_KIND, MemberBatch, MemberOutcome, MembersAccepted,
    PAGE_ENTRIES, Stage, Step, StreamedMember, SubmittedComposite, failure_of, pages_manifest,
};

/// The results of a composite a stage is to take were not found.
struct ResultsGone;

/// What a batch adds once it is admitted.
enum Admitted {
    /// Asked again: every member or item it names is held already.
    Repeat,
    /// The members or items past those held, by index.
    Fresh(Vec<(u64, StreamedMember)>),
}

impl BoundStep {
    /// The submission of this step as a Task. The input is the step's own
    /// bytes when it is bound to a value; when it is bound to blobs, none, since
    /// those belong to the composite and its member's worker reads them through
    /// the composite's record; otherwise the manifest of what it is given.
    pub(super) fn submission(&self, input: Option<Manifest>) -> Submission {
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

    /// Adds a batch of members to a streamed composite, or refuses it whole.
    /// A batch whose members are all held already is answered as accepted
    /// and changes nothing, so a submitter that lost an answer asks again.
    pub fn submit_members(&mut self, batch: MemberBatch) -> Result<MembersAccepted, SubmitRejection> {
        let outcome = self.record_members(batch);
        self.end_call();
        outcome
    }

    /// Whether SlowDown is raised: memory in use passed the soft limit and
    /// has not yet fallen back below 80% of it.
    pub fn slow_down(&self) -> bool {
        self.budget.slow_down()
    }

    fn record_members(&mut self, batch: MemberBatch) -> Result<MembersAccepted, SubmitRejection> {
        let (id, last) = (batch.composite_id.clone(), batch.last);
        let end = batch.start_index.saturating_add(batch.members.len() as u64);
        let fresh = match self.admit_batch(batch)? {
            Admitted::Repeat => return Ok(self.members_accepted(&id)),
            Admitted::Fresh(fresh) => fresh,
        };
        if self.composites.held[&id].step.kind == CompositeKind::Reduce {
            self.take_reduce_items(&id, fresh, last)?;
        } else {
            self.take_members(&id, fresh, end, last)?;
        }
        self.update_pressure();
        Ok(self.members_accepted(&id))
    }

    /// Checks a batch against the composite it is for, and says what it adds:
    /// nothing if every member or item it names is held already, else those
    /// past the ones held.
    fn admit_batch(&mut self, batch: MemberBatch) -> Result<Admitted, SubmitRejection> {
        if !self.check_leader() {
            return Err(SubmitRejection::NotLeader);
        }
        let id = &batch.composite_id;
        if self.reconciliation.is_uncertain_composite(id) {
            return Err(SubmitRejection::NotReady);
        }
        let composite = self.composites.held.get(id).ok_or(SubmitRejection::UnknownComposite)?;
        if !composite.takes_batches() {
            return Err(SubmitRejection::NotStreamed);
        }
        let (held, start) = (composite.batch_held(), batch.start_index);
        if start > held {
            return Err(SubmitRejection::MemberGap { held, start });
        }
        let end = start
            .checked_add(batch.members.len() as u64)
            .ok_or(SubmitRejection::MemberGap { held, start })?;
        if end <= held && (composite.sealed || !batch.last) {
            // Asked again: every member it names is held already.
            return Ok(Admitted::Repeat);
        }
        if composite.state != CompositeState::Running {
            return Err(SubmitRejection::CompositeFinished);
        }
        if composite.sealed {
            return Err(SubmitRejection::MemberGap { held, start: end });
        }
        Ok(Admitted::Fresh((start..).zip(batch.members).filter(|(index, _)| *index >= held).collect()))
    }

    /// Takes the new members of a map or group's batch, whole or not at all,
    /// and starts them; the last batch seals the composite.
    fn take_members(
        &mut self,
        id: &CompositeId,
        fresh: Vec<(u64, StreamedMember)>,
        end: u64,
        last: bool,
    ) -> Result<(), SubmitRejection> {
        let composite = &self.composites.held[id];
        // A member that is a composite is weighed by what starting it takes
        // (its record and the inputs of what it starts at once), so a batch
        // is not accepted only for the members' first stages to be refused.
        let needed: u64 = fresh
            .iter()
            .map(|(index, member)| match member {
                StreamedMember::Stage(Stage::Composite(step)) => super::records::start_bytes(&id.child(*index), step),
                _ => member.budget_bytes(),
            })
            .sum();
        let members: Vec<(u64, Stage)> = fresh
            .into_iter()
            .map(|(index, member)| composite.member_stage(member).map(|stage| (index, stage)))
            .collect::<Result<_, SubmitRejection>>()?;
        self.budget.check_room(needed, || 0)?;
        let composite = self.composites.held.get_mut(id).expect("held above");
        composite.members = composite.members.max(end);
        composite.outcomes.grow_to(composite.members);
        composite.sealed |= last;
        self.composites.unpublished.insert(id.clone());
        for (index, stage) in members {
            self.start_member_stage(id, index, stage);
        }
        self.finish_if_complete(id);
        Ok(())
    }

    fn members_accepted(&self, id: &CompositeId) -> MembersAccepted {
        MembersAccepted { members: self.composites.held[id].batch_held(), slow_down: self.budget.slow_down() }
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
        let bytes = super::records::budget_len(&id, &step);
        self.budget.take(bytes);
        let root = match &parent {
            Some(CompositeParent::Composite { id: parent, .. }) => self.composites.held[parent].root.clone(),
            _ => id.clone(),
        };
        let retained_by = self.retainer_of(parent.as_ref());
        let expands = step.expands_given(&input);
        let batches = step.streamed || expands;
        // A reduce has no member until it is sealed: its members are its
        // plan's steps, which need its item count.
        let reduce = step.kind == CompositeKind::Reduce;
        let starts: Vec<u64> = match step.kind {
            CompositeKind::Flow => vec![0],
            _ if batches || reduce => Vec::new(),
            _ => (0..step.stages.len() as u64).collect(),
        };
        // A map over an empty list has no member to wait for; a composite
        // fed in batches has none yet, and waits for its last batch.
        let empty = step.stages.is_empty() && !batches;
        let (members, sealed) = if batches || reduce { (0, false) } else { (step.stages.len() as u64, true) };
        // A reduce given the results of a composite folds them from that
        // composite's pages: they are its items.
        debug_assert!(
            !reduce || batches || input.is_some(),
            "a reduce that streams no items is given the results to fold: nothing runs before a submitted one"
        );
        let items = if reduce && !batches { self.items_in(input.as_ref()) } else { 0 };
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
                outcomes: Outcomes::of(BTreeMap::new(), members),
                failure: None,
                submitted_at,
                bytes,
                members,
                sealed,
                items_taken: items,
                partial_items: Vec::new(),
                filled_item_pages: BTreeMap::new(),
                published_item_pages: BTreeMap::new(),
            },
        );
        self.composites.unpublished.insert(id.clone());
        if empty {
            self.finish_composite(&id, CompositeState::Succeeded, None);
        }
        if expands {
            self.submit_expansion(&id);
        } else if reduce && !batches {
            self.seal_reduce(&id);
        } else {
            for index in starts {
                self.start_stage(&id, index);
            }
        }
        self.update_pressure();
    }

    /// How many results the composite `input` names pages of holds: those of
    /// a held composite's members, none for a list of no pages.
    fn items_in(&self, input: Option<&(Manifest, InputShape)>) -> u64 {
        let first = input.and_then(|(manifest, _)| manifest.entries().first());
        let Some(ManifestEntry::Page { composite, .. }) = first else {
            return 0;
        };
        self.composites.held.get(composite).expect("the composite whose results a reduce folds is held").members
    }

    /// Records the expansion run that splits the list an expanding map or
    /// reduce is given. The run is not a member: it sends the composite its
    /// items in batches, and its certification seals it.
    pub(super) fn submit_expansion(&mut self, id: &CompositeId) {
        let composite = &self.composites.held[id];
        let Stage::Task(template) = &composite.step.stages[0] else {
            unreachable!("a composite that expands has one task stage");
        };
        let then = match &composite.step.reduce {
            Some(shape) => generated::expansion::Then::Reduce((&shape.reducer).into()),
            None => generated::expansion::Then::Map(template.into()),
        };
        let (source, shape) = composite
            .input
            .clone()
            .expect("only a stage after another expands, and it is given that stage's result");
        let expansion = generated::Expansion {
            source: Some(source.into()),
            then: Some(then),
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
        let submitted = Submitted::received(id.expansion_task(), WallTime::now(&self.clock), submission, &self.clock);
        if let Err(refused) = self.record_submission(submitted) {
            let failure = Failure::new(refused.kind(), refused.to_string());
            self.finish_composite(id, CompositeState::Failed, Some(failure));
        }
    }

    /// Starts the composite `CompositeId::continuation_of(run)` from the step
    /// stored on `run`, a certified run of `task_id`, with the task as its
    /// parent. The step was committed by the revision that certified the run;
    /// the composite is derived from it, so the same run always makes the
    /// same composite and one already held is kept as it is.
    pub(in crate::scheduler) fn commit_continuation(&mut self, task_id: &TaskId, run: &TaskRunId) {
        let id = CompositeId::continuation_of(run);
        // A record that may exist is not made over: recovery derives the
        // composite if it turns out there is none.
        if self.composites.held.contains_key(&id) || self.reconciliation.is_uncertain_composite(&id) {
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
        if composite.step.kind == CompositeKind::Reduce {
            self.submit_reduce_step(id, index);
            return;
        }
        let stage = composite.step.stages[index as usize].clone();
        if self.stage_started(id, index, &stage) {
            return;
        }
        let Ok(input) = self.stage_input(id, index) else {
            let failure = Failure::new(LOST_KIND, "the results of the stage before were not found");
            self.finish_composite(id, CompositeState::Failed, Some(failure));
            return;
        };
        self.launch_stage(id, index, stage, input);
    }

    /// Starts member `index` of the streamed composite `id`, which runs
    /// `stage` and takes no input. Starting what already exists changes
    /// nothing, and nothing starts in a composite that is over.
    fn start_member_stage(&mut self, id: &CompositeId, index: u64, stage: Stage) {
        if self.composites.held[id].state != CompositeState::Running || self.stage_started(id, index, &stage) {
            return;
        }
        match stage {
            Stage::Task(bound) => {
                // A streamed member's blobs belong to the composite it
                // streams into, and no record holds its step, so its Task
                // names them.
                let mut submission = bound.submission(None);
                if let BoundInput::Chunked(manifest) = &bound.input {
                    submission = submission.with_input_manifest(manifest.clone());
                }
                self.submit_member(id, index, submission);
            }
            Stage::Composite(step) => self.launch_stage(id, index, Stage::Composite(step), None),
        }
    }

    /// Whether the Task or child composite stage `index` of `id` makes exists.
    fn stage_started(&self, id: &CompositeId, index: u64, stage: &Stage) -> bool {
        match stage {
            Stage::Task(_) => self.tasks.contains_key(&id.member_task(index)),
            Stage::Composite(_) => self.composites.held.contains_key(&id.child(index)),
        }
    }

    fn launch_stage(&mut self, id: &CompositeId, index: u64, stage: Stage, input: Option<(Manifest, InputShape)>) {
        match stage {
            Stage::Task(bound) => {
                self.submit_member(id, index, bound.submission(input.map(|(manifest, _)| manifest)));
            }
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
            MemberOutcome::Child(child) => {
                let manifest = self.results_manifest(child).ok_or(ResultsGone)?;
                // A reduce's results are one value, encoded by its reducer's
                // serializer; any other composite's are a list.
                let shape = match &self.composites.held[child].step.reduce {
                    Some(shape) => InputShape::Value(shape.reducer.definition.clone()),
                    None => InputShape::List,
                };
                Ok((manifest, shape))
            }
            MemberOutcome::Failed(_) => unreachable!("no stage starts after a failed one"),
        }
    }

    /// The manifest of a held composite's results, if it is held: its pages,
    /// or, for a reduce, its first page whatever its page count, since a
    /// reduce's results are one value.
    fn results_manifest(&self, id: &CompositeId) -> Option<Manifest> {
        let composite = self.composites.held.get(id)?;
        if composite.step.kind == CompositeKind::Reduce {
            return Some(Manifest::new(vec![ManifestEntry::Page { composite: id.clone(), page: 0 }], 0));
        }
        Some(pages_manifest(id, composite.pages()))
    }

    /// Records member `index`'s Task. A Task the scheduler refuses settles
    /// the member as failed with the refusal's kind.
    fn submit_member(&mut self, id: &CompositeId, index: u64, submission: Submission) {
        let submitted = Submitted::received(
            id.member_task(index),
            WallTime::now(&self.clock),
            submission.as_member_of(id.clone(), index),
            &self.clock,
        );
        if let Err(refused) = self.record_submission(submitted) {
            let failure = Failure::new(refused.kind(), refused.to_string());
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
        let expansion = self.tasks[task_id].task_definition_id().as_str() == EXPANSION_DEFINITION;
        if let Some(outcome) = self.member_outcome_of(task_id) {
            let outer = self.composites.settling.len();
            self.composites.settling.push(task_id.clone());
            if expansion {
                self.expansion_finished(&composite, outcome);
            } else {
                self.settle_member(&composite, index, outcome);
            }
            self.composites.settling.truncate(outer);
        }
    }

    /// The expansion run of the map or reduce `id` ended as `outcome`. Success
    /// means every item has been sent, so the map is sealed at the members it
    /// holds, and a reduce, unless its last batch sealed it, at the items it
    /// holds, which plans and starts it; failure fails it whatever its
    /// policy, since without the items it has no results. Members already
    /// accepted run on and are ignored.
    pub(super) fn expansion_finished(&mut self, id: &CompositeId, outcome: MemberOutcome) {
        let Some(composite) = self.composites.held.get_mut(id) else {
            return;
        };
        if composite.state != CompositeState::Running {
            return;
        }
        match outcome {
            MemberOutcome::Failed(failure) => self.finish_composite(id, CompositeState::Failed, Some(failure)),
            MemberOutcome::Value { .. } | MemberOutcome::Child(_) if composite.step.kind == CompositeKind::Reduce => {
                if !composite.sealed {
                    self.seal_reduce(id);
                }
            }
            MemberOutcome::Value { .. } | MemberOutcome::Child(_) => {
                composite.sealed = true;
                self.composites.unpublished.insert(id.clone());
                self.finish_if_complete(id);
            }
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
                    None => MemberOutcome::Failed(Failure::new(LOST_KIND, "the record of the step the task returned was not found")),
                }
            }
            TaskRunState::Succeeded => match run.result_digest.as_ref().and_then(|digest| Digest::try_from(digest).ok()) {
                Some(digest) => MemberOutcome::Value { digest, producer: self.tasks[task_id].task_definition_id() },
                None => MemberOutcome::Failed(Failure::new(LOST_KIND, "the run succeeded without a readable result digest")),
            },
            state => MemberOutcome::Failed(failure_of(task_id, state, &run.failure_kind, &run.failure_message)),
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
        self.composites.page_changed(id, (index / PAGE_ENTRIES) as u32);
        // The record now names every page up to the newest, so the pages
        // below the one that grew it are due with it once they are whole.
        for page in pages_before..pages_after {
            self.composites.page_changed(id, page);
        }
        true
    }

    fn advance(&mut self, id: &CompositeId, index: u64, outcome: MemberOutcome) {
        let composite = &self.composites.held[id];
        if composite.step.kind == CompositeKind::Reduce {
            self.advance_reduce(id, index, outcome);
            return;
        }
        let stages = composite.step.stages.len() as u64;
        match (composite.step.kind, outcome) {
            (_, MemberOutcome::Failed(failure)) if composite.fails_on_first_failure() => {
                self.finish_composite(id, CompositeState::Failed, Some(failure));
            }
            (CompositeKind::Flow, _) if index + 1 < stages => {
                self.composites.held.get_mut(id).expect("held above").next_stage = index + 1;
                self.start_stage(id, index + 1);
            }
            (CompositeKind::Flow, _) => self.finish_composite(id, CompositeState::Succeeded, None),
            _ => self.finish_if_complete(id),
        }
    }

    /// Ends a group or map as succeeded once its last batch is in and every
    /// member has settled.
    fn finish_if_complete(&mut self, id: &CompositeId) {
        let composite = &self.composites.held[id];
        if composite.state == CompositeState::Running
            && composite.sealed
            && composite.outcomes.len() as u64 == composite.members
        {
            self.finish_composite(id, CompositeState::Succeeded, None);
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
        let expands = composite.expands();
        // A map or reduce that ends without all of its items, which a failing
        // member or a cancel does, has no use for the rest of them: its
        // expansion run is stopped, since its next batch would find the
        // composite over.
        let expansion = id.expansion_task();
        if expands && state != CompositeState::Succeeded && self.tasks.contains_key(&expansion) {
            let _ = self.cancel_task(&expansion);
        }
        self.budget.give_back(bytes);
        self.composites.record_finished(id, now);
        self.composites.release_pages(id);
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
        let unsettled: Vec<u64> = composite.outcomes.open().collect();
        let mut was_running = false;
        for index in unsettled {
            was_running |= self.cancel_stage(id, index);
        }
        let expansion = id.expansion_task();
        if self.tasks.contains_key(&expansion) {
            was_running |= matches!(self.cancel_task(&expansion), Ok(Cancellation::Cancelled { was_running: true }));
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

    /// Notes, for the composite holding the Task `task_id` as a member, that
    /// it was claimed, so counting the running members visits only the
    /// members that were.
    pub(in crate::scheduler) fn note_member_claimed(&mut self, task_id: &TaskId) {
        let task = &self.tasks[task_id];
        let Some(id) = task.composite_id() else {
            return;
        };
        if task.task_definition_id().as_str() == EXPANSION_DEFINITION {
            return;
        }
        let index = task.composite_index;
        if let Some(composite) = self.composites.held.get_mut(&id) {
            composite.outcomes.note_claimed(index);
        }
    }

    /// How many members of the held composite `id` are Tasks that have not
    /// settled and whose current run is claimed or started.
    pub(in crate::scheduler) fn running_members(&self, id: &CompositeId) -> u64 {
        let composite = &self.composites.held[id];
        composite
            .outcomes
            .claimed()
            .filter(|index| {
                self.current_run.get(&id.member_task(*index)).is_some_and(|run| {
                    matches!(self.runs[run].current_state(), TaskRunState::Claimed | TaskRunState::Running)
                })
            })
            .count() as u64
    }
}
