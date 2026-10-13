//! Composites: a flow, group or map the shard's leader drives, as its
//! submitter described it.
//!
//! [`Step`] is the validated, domain form of the generated
//! `StepDescription`. It is built once at the edge, with
//! `Step::try_from(&description)`, so the rest of the leader never meets a
//! description that cannot be driven. Building it never decodes user data: a
//! bound input stays the bytes its serializer made.

use std::time::Duration;

use prost::Message as _;

use crate::manifest::{Failure, Manifest, ManifestEntry};
use crate::protocol::digest::Digest;
use crate::protocol::generated;
use crate::protocol::ids::{CompositeId, TaskDefinitionId, TaskId, WorkerId};
use crate::protocol::task::TaskRunState;
use crate::time::WallTime;

mod drive;
mod held;
mod records;
mod recover;

pub(in crate::scheduler) use held::Composites;

/// How many member outcomes one page of a composite holds.
pub const PAGE_ENTRIES: u64 = 1024;

/// The definition of the internal task that splits a list into one blob per
/// item. A user task may not take this name.
pub const EXPANSION_DEFINITION: &str = "kabudachi.expand";

/// What a composite does with its members.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompositeKind {
    /// Stages run one after another, each on the result of the one before.
    Flow,
    /// Members run side by side.
    Group,
    /// One task per item of a list, run as a group.
    Map,
    /// Folds a list of results into one.
    Reduce,
}

/// What a composite does when a member fails for good.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnError {
    /// The first failure ends the composite.
    FailFast,
    /// Every member runs to its end.
    CollectAll,
}

/// Where a composite stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompositeState {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

/// The failure kind of a member the system cancelled, and so of a composite
/// that was cancelled: the Python error class a client raises.
pub const CANCELLED_KIND: &str = "TaskCancelledError";

/// The failure kind of a member that expired before it could start.
pub const EXPIRED_KIND: &str = "TaskExpiredError";

/// The failure kind of a member whose worker was lost and which was not
/// replayed.
pub const LOST_KIND: &str = "TaskLostError";

/// The failure kind of a member that was running when its worker was lost
/// and is not safe to run twice.
pub const ORPHANED_KIND: &str = "TaskOrphanedError";

/// The failure kind of a member a newer generation of its coalescing key
/// replaced.
pub const SUPERSEDED_KIND: &str = "TaskSupersededError";

/// How a composite was submitted: the id and time are fixed by whoever
/// submits, before any leader sees it, so asking again names the same
/// composite.
#[derive(Debug, Clone, PartialEq)]
pub struct SubmittedComposite {
    pub composite_id: CompositeId,
    pub submitted_at: WallTime,
    pub step: Step,
    /// The worker told when the composite finishes; `None` when nobody
    /// waits for it.
    pub client: Option<WorkerId>,
}

/// What a composite is a part of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeParent {
    /// Stage or member `index` of another composite.
    Composite { id: CompositeId, index: u64 },
    /// The continuation of this task's certified run.
    Task(TaskId),
}

/// How one stage or member of a composite ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberOutcome {
    /// The member's result: the digest of the blob that holds it, and the
    /// task whose serializer encoded it.
    Value { digest: Digest, producer: TaskDefinitionId },
    Failed(Failure),
    /// The member was itself a composite; its results are that composite's.
    Child(CompositeId),
}

/// How many of a composite's members are in each state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemberCounts {
    pub pending: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub cancelled: u64,
}

/// Where a composite stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompositeStatus {
    pub state: CompositeState,
    /// A flow's stage that runs, or runs next.
    pub next_stage: u64,
    pub counts: MemberCounts,
}

/// A composite is over: its client, or the handle of the task it continues,
/// is to be told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeFinished {
    pub composite_id: CompositeId,
    pub state: CompositeState,
    pub failure: Option<Failure>,
    /// How many pages its member outcomes take.
    pub pages: u32,
    pub client: Option<WorkerId>,
    /// Set when the composite is a task's continuation.
    pub parent_task: Option<TaskId>,
}

impl CompositeFinished {
    /// The composite's results: a manifest of its pages, in order.
    pub fn results(&self) -> Manifest {
        pages_manifest(&self.composite_id, self.pages)
    }
}

/// A manifest naming the first `pages` pages of `composite`.
fn pages_manifest(composite: &CompositeId, pages: u32) -> Manifest {
    let entries = (0..pages)
        .map(|page| ManifestEntry::Page { composite: composite.clone(), page })
        .collect();
    Manifest::new(entries, 0)
}

/// How a nested composite's input decodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputShape {
    /// One value, encoded by this task's serializer.
    Value(TaskDefinitionId),
    /// A list whose entries name their own producers.
    List,
}

/// The failure a member that ended in `state` is recorded with: the kind its
/// run failed with when it has one, else the kind the state itself names.
pub(crate) fn failure_of(state: TaskRunState, kind: &str) -> Failure {
    let by_state = match state {
        TaskRunState::Failed => "",
        TaskRunState::Expired => EXPIRED_KIND,
        TaskRunState::Lost => LOST_KIND,
        TaskRunState::Orphaned => ORPHANED_KIND,
        TaskRunState::Cancelled => CANCELLED_KIND,
        TaskRunState::Superseded => SUPERSEDED_KIND,
        TaskRunState::Scheduled
        | TaskRunState::Queued
        | TaskRunState::Claimed
        | TaskRunState::Running
        | TaskRunState::Succeeded => unreachable!("a member whose task ended did not fail: {state:?}"),
    };
    let kind = if kind.is_empty() { by_state } else { kind };
    Failure { kind: kind.to_owned(), message: String::new() }
}

/// A composite as its submitter described it.
#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    pub kind: CompositeKind,
    pub on_error: OnError,
    /// A flow's stages, or a group's or map's members, in order.
    pub stages: Vec<Stage>,
}

/// One stage of a flow, or one member of a group or map.
#[derive(Debug, Clone, PartialEq)]
pub enum Stage {
    Task(BoundStep),
    Composite(Step),
}

/// One task to run, with as much of its input as the submitter fixed.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundStep {
    pub definition: TaskDefinitionId,
    pub version: u32,
    pub options: TaskOptions,
    pub input: BoundInput,
    /// Fields set on a copy of the input the step is given, sorted by name.
    pub fields: Vec<BoundField>,
}

/// What a member Task is submitted with, besides its input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskOptions {
    pub queue: String,
    pub retries: u32,
    pub ephemeral: bool,
    pub non_retriable: bool,
    /// `None`: the shard's default.
    pub reconnect_timeout: Option<Duration>,
}

impl TaskOptions {
    /// Options for a task on `queue` with every other setting at its default:
    /// no retries, not ephemeral, retriable, the shard's reconnect timeout.
    pub fn on_queue(queue: impl Into<String>) -> TaskOptions {
        TaskOptions {
            queue: queue.into(),
            retries: 0,
            ephemeral: false,
            non_retriable: false,
            reconnect_timeout: None,
        }
    }
}

/// The input a bound step is submitted with.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundInput {
    /// The whole input, encoded by the task's own serializer.
    Value(Vec<u8>),
    /// The whole input, too large for one frame, as chunks.
    Chunked(Manifest),
    /// The input the step is given: the composite's, or the previous stage's
    /// result.
    Previous,
}

/// One scalar field of a protobuf message input, as its submitter bound it.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundField {
    pub name: String,
    pub value: FieldValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Int(i64),
    Uint(u64),
    Double(f64),
    Bool(bool),
    Text(String),
    Bytes(Vec<u8>),
}

/// Why a step description cannot be driven.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MalformedStep {
    #[error("a flow or group needs at least one stage or member")]
    NoStages,
    #[error("a stage names no task and no composite")]
    EmptyStage,
    #[error("a task step names no definition")]
    NoDefinition,
    #[error("a task step names no queue")]
    NoQueue,
    #[error("a task step names a definition reserved for the system")]
    ReservedDefinition,
    #[error("the composite's kind or error policy is unknown")]
    UnknownKind,
    #[error("this kind of composite cannot be run here")]
    UnsupportedKind,
    #[error("a map has either bound members or one member that takes its input")]
    MapShape,
    #[error("bound fields are not sorted by name, or one is repeated")]
    FieldsUnsorted,
    #[error("a bound field has no value, or fields are bound with a whole input")]
    BadField,
    #[error("a bound input's manifest cannot be read")]
    BadManifest,
    #[error("the bytes are not an encoded step description")]
    Undecodable,
}

impl Step {
    /// The step an encoded `StepDescription` describes.
    pub fn decode(bytes: &[u8]) -> Result<Step, MalformedStep> {
        let description = generated::StepDescription::decode(bytes).map_err(|_| MalformedStep::Undecodable)?;
        Step::try_from(&description)
    }

    /// The BLAKE3 digest of the step's encoding: what a returning run's
    /// result digest is when the step is its continuation.
    pub fn digest(&self) -> Digest {
        Digest::blake3(&generated::StepDescription::from(self).encode_to_vec())
    }

    /// Whether the step needs an input from whatever runs before it: its
    /// first stage does (a flow), or any member does (a group or map).
    pub fn takes_input(&self) -> bool {
        match self.kind {
            CompositeKind::Flow => self.stages.first().is_some_and(Stage::takes_input),
            _ => self.stages.iter().any(Stage::takes_input),
        }
    }

    /// Whether the step is a map over a list it is given, which an
    /// expansion run must first split into items.
    pub fn expands(&self) -> bool {
        self.kind == CompositeKind::Map && self.stages.len() == 1 && self.stages[0].takes_input()
    }

    /// A map's members are all bound to their items, or it has one member
    /// that takes the list it is given.
    fn is_map_shaped(&self) -> bool {
        self.expands() || self.stages.iter().all(|stage| !stage.takes_input())
    }
}

impl Stage {
    /// Whether the stage needs an input from whatever runs before it.
    pub fn takes_input(&self) -> bool {
        match self {
            Stage::Task(step) => matches!(step.input, BoundInput::Previous),
            Stage::Composite(step) => step.takes_input(),
        }
    }

    pub fn as_composite(&self) -> Option<&Step> {
        match self {
            Stage::Composite(step) => Some(step),
            Stage::Task(_) => None,
        }
    }
}

impl TryFrom<&generated::StepDescription> for Step {
    type Error = MalformedStep;

    fn try_from(wire: &generated::StepDescription) -> Result<Self, MalformedStep> {
        let kind = match generated::CompositeKind::try_from(wire.kind) {
            Ok(generated::CompositeKind::Flow) => CompositeKind::Flow,
            Ok(generated::CompositeKind::Group) => CompositeKind::Group,
            Ok(generated::CompositeKind::Map) => CompositeKind::Map,
            // The reduce driver is not built yet: refusing it here keeps a
            // reduce from being stored and never driven.
            Ok(generated::CompositeKind::Reduce) => return Err(MalformedStep::UnsupportedKind),
            _ => return Err(MalformedStep::UnknownKind),
        };
        let on_error = match generated::OnError::try_from(wire.on_error) {
            Ok(generated::OnError::FailFast) => OnError::FailFast,
            Ok(generated::OnError::CollectAll) => OnError::CollectAll,
            _ => return Err(MalformedStep::UnknownKind),
        };
        // A map over an empty list is a map of no members, which finishes as
        // it starts; a flow or group of nothing is a mistake.
        if wire.stages.is_empty() && kind != CompositeKind::Map {
            return Err(MalformedStep::NoStages);
        }
        let stages = wire.stages.iter().map(Stage::try_from).collect::<Result<Vec<_>, _>>()?;
        let step = Step { kind, on_error, stages };
        if kind == CompositeKind::Map && !step.is_map_shaped() {
            return Err(MalformedStep::MapShape);
        }
        Ok(step)
    }
}

impl TryFrom<&generated::StageDescription> for Stage {
    type Error = MalformedStep;

    fn try_from(wire: &generated::StageDescription) -> Result<Self, MalformedStep> {
        match wire.step.as_ref() {
            None => Err(MalformedStep::EmptyStage),
            Some(generated::stage_description::Step::Task(task)) => BoundStep::try_from(task).map(Stage::Task),
            Some(generated::stage_description::Step::Composite(step)) => {
                Step::try_from(step).map(Stage::Composite)
            }
        }
    }
}

impl TryFrom<&generated::BoundTaskStep> for BoundStep {
    type Error = MalformedStep;

    fn try_from(wire: &generated::BoundTaskStep) -> Result<Self, MalformedStep> {
        let definition: TaskDefinitionId = wire.definition.clone().ok_or(MalformedStep::NoDefinition)?.into();
        if definition.as_str() == EXPANSION_DEFINITION {
            return Err(MalformedStep::ReservedDefinition);
        }
        let options = wire.options.clone().unwrap_or_default();
        if options.queue.is_empty() {
            return Err(MalformedStep::NoQueue);
        }
        let input = match wire.input.as_ref() {
            None => BoundInput::Previous,
            Some(generated::bound_task_step::Input::Value(bytes)) => BoundInput::Value(bytes.clone()),
            Some(generated::bound_task_step::Input::ValueManifest(manifest)) => {
                BoundInput::Chunked(Manifest::try_from(manifest).map_err(|_| MalformedStep::BadManifest)?)
            }
        };
        let fields = wire.fields.iter().map(BoundField::try_from).collect::<Result<Vec<_>, _>>()?;
        if !fields.is_empty() && !matches!(input, BoundInput::Previous) {
            return Err(MalformedStep::BadField);
        }
        if !fields.windows(2).all(|pair| pair[0].name < pair[1].name) {
            return Err(MalformedStep::FieldsUnsorted);
        }
        Ok(BoundStep {
            definition,
            version: wire.version,
            options: TaskOptions {
                queue: options.queue,
                retries: options.max_retries,
                ephemeral: options.ephemeral,
                non_retriable: options.non_retriable,
                reconnect_timeout: (options.reconnect_timeout_ms != 0)
                    .then(|| Duration::from_millis(options.reconnect_timeout_ms)),
            },
            input,
            fields,
        })
    }
}

impl TryFrom<&generated::BoundField> for BoundField {
    type Error = MalformedStep;

    fn try_from(wire: &generated::BoundField) -> Result<Self, MalformedStep> {
        use generated::bound_field::Value;
        let value = match wire.value.as_ref().ok_or(MalformedStep::BadField)? {
            Value::IntValue(value) => FieldValue::Int(*value),
            Value::UintValue(value) => FieldValue::Uint(*value),
            Value::DoubleValue(value) => FieldValue::Double(*value),
            Value::BoolValue(value) => FieldValue::Bool(*value),
            Value::StringValue(value) => FieldValue::Text(value.clone()),
            Value::BytesValue(value) => FieldValue::Bytes(value.clone()),
        };
        Ok(BoundField { name: wire.name.clone(), value })
    }
}

impl From<&Step> for generated::StepDescription {
    fn from(step: &Step) -> Self {
        let kind = match step.kind {
            CompositeKind::Flow => generated::CompositeKind::Flow,
            CompositeKind::Group => generated::CompositeKind::Group,
            CompositeKind::Map => generated::CompositeKind::Map,
            CompositeKind::Reduce => generated::CompositeKind::Reduce,
        };
        generated::StepDescription {
            kind: kind as i32,
            on_error: generated::OnError::from(step.on_error) as i32,
            stages: step.stages.iter().map(generated::StageDescription::from).collect(),
        }
    }
}

impl From<OnError> for generated::OnError {
    fn from(on_error: OnError) -> Self {
        match on_error {
            OnError::FailFast => generated::OnError::FailFast,
            OnError::CollectAll => generated::OnError::CollectAll,
        }
    }
}

impl From<&Stage> for generated::StageDescription {
    fn from(stage: &Stage) -> Self {
        let step = match stage {
            Stage::Task(task) => generated::stage_description::Step::Task(task.into()),
            Stage::Composite(step) => generated::stage_description::Step::Composite(step.into()),
        };
        generated::StageDescription { step: Some(step) }
    }
}

impl From<&BoundStep> for generated::BoundTaskStep {
    fn from(step: &BoundStep) -> Self {
        let input = match &step.input {
            BoundInput::Previous => None,
            BoundInput::Value(bytes) => Some(generated::bound_task_step::Input::Value(bytes.clone())),
            BoundInput::Chunked(manifest) => Some(generated::bound_task_step::Input::ValueManifest(
                manifest.clone().into(),
            )),
        };
        generated::BoundTaskStep {
            definition: Some(step.definition.clone().into()),
            version: step.version,
            options: Some(generated::TaskOptions {
                queue: step.options.queue.clone(),
                max_retries: step.options.retries,
                ephemeral: step.options.ephemeral,
                non_retriable: step.options.non_retriable,
                reconnect_timeout_ms: step
                    .options
                    .reconnect_timeout
                    .map_or(0, |timeout| timeout.as_millis().try_into().unwrap_or(u64::MAX)),
            }),
            input,
            fields: step.fields.iter().map(generated::BoundField::from).collect(),
        }
    }
}

impl From<&BoundField> for generated::BoundField {
    fn from(field: &BoundField) -> Self {
        use generated::bound_field::Value;
        let value = match &field.value {
            FieldValue::Int(value) => Value::IntValue(*value),
            FieldValue::Uint(value) => Value::UintValue(*value),
            FieldValue::Double(value) => Value::DoubleValue(*value),
            FieldValue::Bool(value) => Value::BoolValue(*value),
            FieldValue::Text(value) => Value::StringValue(value.clone()),
            FieldValue::Bytes(value) => Value::BytesValue(value.clone()),
        };
        generated::BoundField { name: field.name.clone(), value: Some(value) }
    }
}
