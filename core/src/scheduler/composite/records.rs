//! A held composite as the records the leader publishes: its record, and its
//! pages of member outcomes.

use std::collections::BTreeMap;

use prost::Message as _;

use crate::manifest::{Failure, Manifest};
use crate::protocol::digest::Digest;
use crate::protocol::generated::{
    self, CompositePage, CompositeRecord, composite_entry, composite_record, input_shape,
};
use crate::protocol::ids::{CompositeId, IdGenerator, WorkerId};
use crate::task_record::RecordVersion;
use crate::time::{Clock, Instant, WallTime};

use crate::scheduler::{Observer, Scheduler, UNVERSIONED};

use super::held::Composite;
use super::{
    BoundInput, CompositeKind, CompositeParent, CompositeState, InputShape, MemberCounts, MemberOutcome, PAGE_ENTRIES,
    Stage, Step,
};

impl From<CompositeState> for generated::CompositeState {
    fn from(state: CompositeState) -> Self {
        match state {
            CompositeState::Running => generated::CompositeState::Running,
            CompositeState::Succeeded => generated::CompositeState::Succeeded,
            CompositeState::Failed => generated::CompositeState::Failed,
            CompositeState::Cancelled => generated::CompositeState::Cancelled,
        }
    }
}

impl From<MemberCounts> for generated::MemberCounts {
    fn from(counts: MemberCounts) -> Self {
        generated::MemberCounts {
            pending: counts.pending,
            running: counts.running,
            succeeded: counts.succeeded,
            failed: counts.failed,
            cancelled: counts.cancelled,
        }
    }
}

impl From<&InputShape> for generated::InputShape {
    fn from(shape: &InputShape) -> Self {
        let shape = match shape {
            InputShape::Value(producer) => input_shape::Shape::ValueOf(producer.clone().into()),
            InputShape::List => input_shape::Shape::List(true),
        };
        generated::InputShape { shape: Some(shape) }
    }
}

/// How large the first revision of the record of `id`, running `step`, is,
/// unversioned: what a composite takes of the memory budget and of a record.
pub(super) fn first_record_len(id: &CompositeId, step: &Step) -> u64 {
    CompositeRecord {
        composite_id: Some(id.clone().into()),
        description: Some(generated::StepDescription::from(step)),
        state: generated::CompositeState::Running as i32,
        root: Some(id.clone().into()),
        ..Default::default()
    }
    .encoded_len() as u64
}

/// What a held composite takes of the memory budget until it finishes: its
/// first record, or for a streamed composite only its description, since the
/// record would otherwise raise SlowDown before any member exists.
pub(super) fn budget_len(id: &CompositeId, step: &Step) -> u64 {
    if step.streamed { step.encoded_len() } else { first_record_len(id, step) }
}

/// What starting `step` as the composite `id` takes of the memory budget: its
/// record, and the input each Task it starts at once holds beside it, down
/// through the composites it starts at once. A flow starts only its first
/// stage, and a later one takes its input when it starts. A streamed
/// composite starts no member, so it takes only what it holds.
pub(super) fn start_bytes(id: &CompositeId, step: &Step) -> u64 {
    if step.streamed {
        return budget_len(id, step);
    }
    let started = match step.kind {
        CompositeKind::Flow => &step.stages[..step.stages.len().min(1)],
        _ => &step.stages[..],
    };
    let members: u64 = started
        .iter()
        .enumerate()
        .map(|(index, stage)| match stage {
            Stage::Task(bound) => match &bound.input {
                BoundInput::Value(bytes) => bytes.len() as u64,
                BoundInput::Chunked(_) | BoundInput::Previous => 0,
            },
            Stage::Composite(child) => start_bytes(&id.child(index as u64), child),
        })
        .sum();
    first_record_len(id, step) + members
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// The whole record of the composite `id` as the scheduler holds it now.
    pub(in crate::scheduler) fn composite_record_of(
        &self,
        id: &CompositeId,
        version: RecordVersion,
        published_at: WallTime,
    ) -> CompositeRecord {
        let composite = &self.composites.held[id];
        let (parent, parent_index) = match &composite.parent {
            None => (None, 0),
            Some(CompositeParent::Composite { id, index }) => {
                (Some(composite_record::Parent::ParentComposite(id.clone().into())), *index)
            }
            Some(CompositeParent::Task(task)) => {
                (Some(composite_record::Parent::ParentTask(task.clone().into())), 0)
            }
        };
        CompositeRecord {
            version: Some(version.into()),
            composite_id: Some(id.clone().into()),
            description: Some(generated::StepDescription::from(&composite.step)),
            client: composite.client.clone().map(Into::into),
            parent,
            parent_index,
            input: composite.input.as_ref().map(|(manifest, _)| manifest.clone().into()),
            input_shape: composite.input.as_ref().map(|(_, shape)| shape.into()),
            state: generated::CompositeState::from(composite.state) as i32,
            next_stage: composite.next_stage,
            counts: Some(composite.counts(self.running_members(id)).into()),
            pages: composite.pages(),
            failure: composite.failure.clone().map(Into::into),
            submitted_at: Some(composite.submitted_at.into()),
            placement: Vec::new(),
            prior_placements: Vec::new(),
            published_at: Some(published_at.into()),
            finished: self.composites.is_finished(id),
            root: Some(composite.root.clone().into()),
            members: composite.members,
            sealed: composite.sealed,
        }
    }

    /// Page `page` of the composite `id`: the outcomes of the members it
    /// holds, in index order.
    pub(in crate::scheduler) fn page_record_of(
        &self,
        id: &CompositeId,
        page: u32,
        version: RecordVersion,
        published_at: WallTime,
    ) -> CompositePage {
        let composite = &self.composites.held[id];
        let first = u64::from(page) * PAGE_ENTRIES;
        let entries = composite
            .outcomes
            .range(first..first + PAGE_ENTRIES)
            .map(|(index, outcome)| entry_of(*index, outcome))
            .collect();
        CompositePage {
            version: Some(version.into()),
            composite_id: Some(id.clone().into()),
            index: page,
            entries,
            placement: Vec::new(),
            prior_placements: Vec::new(),
            published_at: Some(published_at.into()),
            finished: self.composites.is_finished(id),
        }
    }

    /// Holds the composite `record` describes, with the outcomes its `pages`
    /// carry, as the newest revisions say. A record that cannot be read is
    /// skipped: a leader wrote it from a step it had validated, so this is
    /// a bug, never a peer's input. Says whether it was held.
    pub(in crate::scheduler) fn install_composite(
        &mut self,
        record: CompositeRecord,
        pages: &[CompositePage],
        now: Instant,
    ) -> bool {
        let Some(id) = record.composite_id.clone().map(CompositeId::from) else {
            return false;
        };
        let Some(composite) = self.composite_from(&id, record, pages) else {
            tracing::error!(composite = id.as_str(), "a composite's stored record cannot be read");
            return false;
        };
        if composite.state == CompositeState::Running {
            self.budget.take(composite.bytes);
        } else {
            self.composites.record_finished(&id, now);
        }
        self.composites.held.insert(id, composite);
        true
    }

    /// Takes the outcomes `pages` carry for the held composite `id` that it
    /// has no outcome for yet: what it decided itself stands.
    pub(in crate::scheduler) fn learn_pages(&mut self, id: &CompositeId, pages: &[CompositePage]) {
        let outcomes: Vec<(u64, MemberOutcome)> =
            pages.iter().flat_map(|page| &page.entries).filter_map(outcome_of).collect();
        for (index, outcome) in outcomes {
            self.record_member(id, index, outcome);
        }
    }

    fn composite_from(&self, id: &CompositeId, record: CompositeRecord, pages: &[CompositePage]) -> Option<Composite> {
        let step = Step::try_from(&record.description?).ok()?;
        let state = match generated::CompositeState::try_from(record.state).ok()? {
            generated::CompositeState::Running => CompositeState::Running,
            generated::CompositeState::Succeeded => CompositeState::Succeeded,
            generated::CompositeState::Failed => CompositeState::Failed,
            generated::CompositeState::Cancelled => CompositeState::Cancelled,
            _ => return None,
        };
        let parent = match record.parent {
            None => None,
            Some(composite_record::Parent::ParentComposite(parent)) => {
                Some(CompositeParent::Composite { id: parent.into(), index: record.parent_index })
            }
            Some(composite_record::Parent::ParentTask(task)) => Some(CompositeParent::Task(task.into())),
        };
        let input = match (record.input.as_ref(), record.input_shape.and_then(|shape| shape.shape)) {
            (None, _) => None,
            (Some(manifest), Some(shape)) => Some((Manifest::try_from(manifest).ok()?, input_shape_of(shape))),
            (Some(_), None) => return None,
        };
        if step.expands() && input.is_none() {
            return None;
        }
        let outcomes: BTreeMap<u64, MemberOutcome> = pages
            .iter()
            .flat_map(|page| &page.entries)
            .filter_map(outcome_of)
            .collect();
        let retained_by = self.retainer_of(parent.as_ref());
        let (members, sealed) = if step.takes_batches() {
            (record.members, record.sealed)
        } else {
            (step.stages.len() as u64, true)
        };
        Some(Composite {
            bytes: budget_len(id, &step),
            members,
            sealed,
            step,
            root: record.root.map_or_else(|| id.clone(), CompositeId::from),
            retained_by,
            parent,
            client: record.client.map(WorkerId::from),
            input,
            state,
            next_stage: record.next_stage,
            outcomes,
            failure: record.failure.as_ref().map(Failure::from),
            submitted_at: record.submitted_at.map_or_else(|| WallTime::now(&self.clock), WallTime::from),
        })
    }

    /// The composite's record as the scheduler holds it, unversioned: what a
    /// one-node runtime reads a member's step from.
    pub fn composite_record(&self, id: &CompositeId) -> Option<CompositeRecord> {
        self.composites
            .held
            .contains_key(id)
            .then(|| self.composite_record_of(id, UNVERSIONED, WallTime::now(&self.clock)))
    }

    /// Page `page` of the composite as the scheduler holds it, unversioned:
    /// what a one-node runtime reads its results from.
    pub fn composite_page(&self, id: &CompositeId, page: u32) -> Option<CompositePage> {
        let composite = self.composites.held.get(id)?;
        (page < composite.pages())
            .then(|| self.page_record_of(id, page, UNVERSIONED, WallTime::now(&self.clock)))
    }
}

fn entry_of(index: u64, outcome: &MemberOutcome) -> generated::CompositeEntry {
    let (outcome, producer) = match outcome {
        MemberOutcome::Value { digest, producer } => (
            composite_entry::Outcome::Value(digest.clone().into()),
            Some(producer.clone().into()),
        ),
        MemberOutcome::Failed(failure) => (composite_entry::Outcome::Failure(failure.clone().into()), None),
        MemberOutcome::Child(child) => (composite_entry::Outcome::Child(child.clone().into()), None),
    };
    generated::CompositeEntry { index, outcome: Some(outcome), producer }
}

fn input_shape_of(shape: input_shape::Shape) -> InputShape {
    match shape {
        input_shape::Shape::ValueOf(producer) => InputShape::Value(producer.into()),
        input_shape::Shape::List(_) => InputShape::List,
    }
}

/// The outcome a page entry holds, with its member's index. An entry that
/// names no outcome, or a value with no readable digest or producer, holds
/// none: its member is settled again from its own record.
fn outcome_of(entry: &generated::CompositeEntry) -> Option<(u64, MemberOutcome)> {
    let outcome = match entry.outcome.as_ref()? {
        composite_entry::Outcome::Value(digest) => MemberOutcome::Value {
            digest: Digest::try_from(digest).ok()?,
            producer: entry.producer.clone()?.into(),
        },
        composite_entry::Outcome::Failure(failure) => MemberOutcome::Failed(Failure::from(failure)),
        composite_entry::Outcome::Child(child) => MemberOutcome::Child(child.clone().into()),
    };
    Some((entry.index, outcome))
}
