//! A held composite as the records the leader publishes: its record, and its
//! pages of member outcomes.

use prost::Message as _;

use crate::protocol::generated::{
    self, CompositePage, CompositeRecord, composite_entry, composite_record, input_shape,
};
use crate::protocol::ids::{CompositeId, IdGenerator};
use crate::task_record::RecordVersion;
use crate::time::{Clock, WallTime};

use crate::scheduler::{Observer, Scheduler, UNVERSIONED};

use super::{CompositeParent, CompositeState, InputShape, MemberCounts, MemberOutcome, PAGE_ENTRIES, Step};

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
            counts: Some(composite.counts().into()),
            pages: composite.pages(),
            failure: composite.failure.clone().map(failure_message),
            submitted_at: Some(composite.submitted_at.into()),
            placement: Vec::new(),
            prior_placements: Vec::new(),
            published_at: Some(published_at.into()),
            finished: self.composites.is_finished(id),
            root: Some(composite.root.clone().into()),
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

fn failure_message(failure: crate::manifest::Failure) -> generated::Failure {
    generated::Failure { kind: failure.kind, message: failure.message }
}

fn entry_of(index: u64, outcome: &MemberOutcome) -> generated::CompositeEntry {
    let (outcome, producer) = match outcome {
        MemberOutcome::Value { digest, producer } => (
            composite_entry::Outcome::Value(digest.clone().into()),
            Some(producer.clone().into()),
        ),
        MemberOutcome::Failed(failure) => (composite_entry::Outcome::Failure(failure_message(failure.clone())), None),
        MemberOutcome::Child(child) => (composite_entry::Outcome::Child(child.clone().into()), None),
    };
    generated::CompositeEntry { index, outcome: Some(outcome), producer }
}
