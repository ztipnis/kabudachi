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
mod resolve;

pub(in crate::scheduler) use held::Composites;
pub use resolve::{
    CompositeSource, MemberInput, Resolved, Unresolved, expansion_source, member_input, resolve_manifest, results,
};

/// The most members one batch of a streamed composite carries.
pub const MEMBER_BATCH_ITEMS: u64 = 1024;

/// The most inline input one batch of a streamed composite carries.
pub const MEMBER_BATCH_BYTES: u64 = 512 * 1024;

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

impl SubmittedComposite {
    /// The bytes its record takes of a record.
    pub fn record_bytes(&self) -> u64 {
        records::first_record_len(&self.composite_id, &self.step)
    }

    /// The bytes starting it takes of the memory budget: its record, and the
    /// input of every Task that starts at once, which the record holds a copy
    /// of as well.
    pub fn memory_bytes(&self) -> u64 {
        records::start_bytes(&self.composite_id, &self.step)
    }
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
    /// Members not settled and not running: waiting, or not yet started.
    pub pending: u64,
    /// Members whose current run is claimed or started.
    pub running: u64,
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
    /// The Tasks whose ends led to this one, innermost first: the member
    /// whose outcome ended the composite, and, for a composite that ended a
    /// member of another, the Task that member was. Their records were made
    /// in the same call, and what is told of the end must not outrun them.
    /// Known only to the leader that raised the event; empty otherwise.
    pub settled_by: Vec<TaskId>,
}

impl CompositeFinished {
    /// The composite's results: a manifest of its pages, in order.
    pub fn results(&self) -> Manifest {
        pages_manifest(&self.composite_id, self.pages)
    }

    /// What the composite `record` stores of its end, as its client was told
    /// it; `None` while it runs, or for a record that names no composite.
    /// A client that missed the message settles from the record instead.
    pub fn of_record(record: &generated::CompositeRecord) -> Option<CompositeFinished> {
        let state = match generated::CompositeState::try_from(record.state) {
            Ok(generated::CompositeState::Succeeded) => CompositeState::Succeeded,
            Ok(generated::CompositeState::Failed) => CompositeState::Failed,
            Ok(generated::CompositeState::Cancelled) => CompositeState::Cancelled,
            _ => return None,
        };
        let parent_task = match &record.parent {
            Some(generated::composite_record::Parent::ParentTask(task)) => Some(TaskId::from(task.clone())),
            _ => None,
        };
        Some(CompositeFinished {
            composite_id: CompositeId::from(record.composite_id.clone()?),
            state,
            failure: record.failure.as_ref().map(Failure::from),
            pages: record.pages,
            client: record.client.clone().map(WorkerId::from),
            parent_task,
            settled_by: Vec::new(),
        })
    }
}

/// What a leader tells a composite's client: the composite, how it ended,
/// its failure if it has one, and its pages as the results. The client's own
/// id is not told to the client.
impl From<&CompositeFinished> for generated::CompositeFinished {
    fn from(finished: &CompositeFinished) -> Self {
        generated::CompositeFinished {
            composite_id: Some(finished.composite_id.clone().into()),
            state: generated::CompositeState::from(finished.state) as i32,
            results: Some(finished.results().into()),
            failure: finished.failure.clone().map(Into::into),
            parent_task: finished.parent_task.clone().map(Into::into),
        }
    }
}

/// A finish message that does not name a composite, one of its states, or its
/// pages in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a composite's finish message cannot be read")]
pub struct MalformedFinished;

impl TryFrom<&generated::CompositeFinished> for CompositeFinished {
    type Error = MalformedFinished;

    /// The composite as a client reads it: with no client of its own to tell.
    fn try_from(wire: &generated::CompositeFinished) -> Result<Self, MalformedFinished> {
        let composite_id = CompositeId::from(wire.composite_id.clone().ok_or(MalformedFinished)?);
        let state = match generated::CompositeState::try_from(wire.state) {
            Ok(generated::CompositeState::Running) => CompositeState::Running,
            Ok(generated::CompositeState::Succeeded) => CompositeState::Succeeded,
            Ok(generated::CompositeState::Failed) => CompositeState::Failed,
            Ok(generated::CompositeState::Cancelled) => CompositeState::Cancelled,
            _ => return Err(MalformedFinished),
        };
        let results = Manifest::try_from(wire.results.as_ref().ok_or(MalformedFinished)?)
            .map_err(|_| MalformedFinished)?;
        let pages = u32::try_from(results.entries().len()).map_err(|_| MalformedFinished)?;
        if results.entries() != pages_manifest(&composite_id, pages).entries() {
            return Err(MalformedFinished);
        }
        Ok(CompositeFinished {
            composite_id,
            state,
            failure: wire.failure.as_ref().map(Failure::from),
            pages,
            client: None,
            parent_task: wire.parent_task.clone().map(TaskId::from),
            settled_by: Vec::new(),
        })
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
/// run failed with when it has one, else the kind the state itself names,
/// with the run's own message when it failed.
pub(crate) fn failure_of(task: &TaskId, state: TaskRunState, kind: &str, message: &str) -> Failure {
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
    let message = if state == TaskRunState::Failed { message } else { "" };
    Failure::new(kind, message).of_task(task.clone())
}

/// A composite as its submitter described it.
#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    pub kind: CompositeKind,
    pub on_error: OnError,
    /// A flow's stages, or a group's or map's members, in order. A streamed
    /// group has none, and a streamed map holds its one template.
    pub stages: Vec<Stage>,
    /// The members are not in `stages`: they arrive in batches, and the
    /// template, if any, takes each item as its input.
    pub streamed: bool,
}

/// One member of a streamed composite, as its submitter sends it.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamedMember {
    /// A map's item: its template's whole input (`Value` or `Chunked`).
    Item(BoundInput),
    /// A group's member.
    Stage(Stage),
}

impl StreamedMember {
    /// The input the member's Task will carry inline, which is what a batch
    /// of members is bounded by on the wire: the length of a `Value`, and
    /// nothing for a chunked item or stage, whose bytes are blobs, or for a
    /// composite stage.
    pub fn input_bytes(&self) -> u64 {
        match self.task_input() {
            Some(BoundInput::Value(bytes)) => bytes.len() as u64,
            _ => 0,
        }
    }

    /// What the member weighs against the memory budget once its Task is
    /// submitted: its whole input, inline or as the blobs of a manifest.
    pub fn budget_bytes(&self) -> u64 {
        match self.task_input() {
            Some(BoundInput::Value(bytes)) => bytes.len() as u64,
            Some(BoundInput::Chunked(manifest)) => manifest.total_bytes(),
            Some(BoundInput::Previous) | None => 0,
        }
    }

    /// The input of the Task the member is, if it is one.
    fn task_input(&self) -> Option<&BoundInput> {
        match self {
            StreamedMember::Item(input) => Some(input),
            StreamedMember::Stage(Stage::Task(step)) => Some(&step.input),
            StreamedMember::Stage(Stage::Composite(_)) => None,
        }
    }
}

/// The next members of a streamed composite, starting at `start_index`.
#[derive(Debug, Clone, PartialEq)]
pub struct MemberBatch {
    pub composite_id: CompositeId,
    pub start_index: u64,
    pub members: Vec<StreamedMember>,
    /// No member follows these.
    pub last: bool,
}

/// What a leader answers a batch it took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembersAccepted {
    /// How many members the composite holds now.
    pub members: u64,
    /// The leader's memory is past its soft limit.
    pub slow_down: bool,
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
    #[error("a flow cannot be streamed")]
    StreamedFlow,
    #[error("a streamed group has no stages and a streamed map has exactly one template that takes its input")]
    StreamedWithStages,
    #[error("a streamed member names neither an item nor a stage")]
    EmptyMember,
    #[error("a member batch names no composite")]
    NoComposite,
    #[error("the bytes are not an encoded step description")]
    Undecodable,
}

impl Step {
    /// The step an encoded `StepDescription` describes.
    pub fn decode(bytes: &[u8]) -> Result<Step, MalformedStep> {
        let description = generated::StepDescription::decode(bytes).map_err(|_| MalformedStep::Undecodable)?;
        Step::try_from(&description)
    }

    /// The step as the bytes of its description. The same step always
    /// encodes the same bytes.
    pub fn encode(&self) -> Vec<u8> {
        generated::StepDescription::from(self).encode_to_vec()
    }

    /// The length of the step's encoding.
    pub fn encoded_len(&self) -> u64 {
        generated::StepDescription::from(self).encoded_len() as u64
    }

    /// The BLAKE3 digest of the step's encoding: what a returning run's
    /// result digest is when the step is its continuation.
    pub fn digest(&self) -> Digest {
        Digest::blake3(&self.encode())
    }

    /// Whether the step needs an input from whatever runs before it: its
    /// first stage does (a flow), or any member does (a group or map).
    pub fn takes_input(&self) -> bool {
        if self.streamed {
            return false;
        }
        match self.kind {
            CompositeKind::Flow => self.stages.first().is_some_and(Stage::takes_input),
            _ => self.stages.iter().any(Stage::takes_input),
        }
    }

    /// Whether the step is a map over a list it is given, which an
    /// expansion run must first split into items.
    pub fn expands(&self) -> bool {
        self.kind == CompositeKind::Map
            && !self.streamed
            && matches!(self.stages.as_slice(), [Stage::Task(template)] if matches!(template.input, BoundInput::Previous))
    }

    /// Whether the step's members arrive in batches: it is streamed, or it is
    /// a map whose expansion run sends the items it splits a list into.
    pub(in crate::scheduler) fn takes_batches(&self) -> bool {
        self.streamed || self.expands()
    }

    /// Whether the step expands a list and its template has fields set: each
    /// item is the template's whole input, so none could be given them.
    fn template_has_fields(&self) -> bool {
        self.expands() && matches!(self.stages.as_slice(), [Stage::Task(template)] if !template.fields.is_empty())
    }

    /// A map's members are all bound to their items, or it has one member
    /// that takes the list it is given.
    fn is_map_shaped(&self) -> bool {
        self.expands() || self.stages.iter().all(|stage| !stage.takes_input())
    }
}

/// What an expansion run is asked: the task to run on every item of the list
/// it splits, and how the map over the items treats a failure.
#[derive(Debug, Clone, PartialEq)]
pub struct ExpansionRequest {
    pub task: BoundStep,
    pub on_error: OnError,
}

impl ExpansionRequest {
    /// The request an expansion run carries as its input.
    pub fn decode(bytes: &[u8]) -> Result<ExpansionRequest, MalformedStep> {
        let request = generated::Expansion::decode(bytes).map_err(|_| MalformedStep::Undecodable)?;
        let Some(generated::expansion::Then::Map(template)) = request.then else {
            return Err(MalformedStep::UnsupportedKind);
        };
        let on_error = match generated::OnError::try_from(request.on_error) {
            Ok(generated::OnError::FailFast) => OnError::FailFast,
            Ok(generated::OnError::CollectAll) => OnError::CollectAll,
            _ => return Err(MalformedStep::UnknownKind),
        };
        Ok(ExpansionRequest { task: BoundStep::try_from(&template)?, on_error })
    }
}

impl Stage {
    /// Whether this stage is a composite that waits for members to be sent,
    /// at any depth.
    pub fn is_or_holds_streamed(&self) -> bool {
        match self {
            Stage::Task(_) => false,
            Stage::Composite(step) => step.streamed || step.stages.iter().any(Stage::is_or_holds_streamed),
        }
    }

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
        if wire.streamed {
            if kind == CompositeKind::Flow {
                return Err(MalformedStep::StreamedFlow);
            }
            if wire.stages.len() > 1 {
                return Err(MalformedStep::StreamedWithStages);
            }
        } else if wire.stages.is_empty() && kind != CompositeKind::Map {
            return Err(MalformedStep::NoStages);
        }
        let stages = wire.stages.iter().map(Stage::try_from).collect::<Result<Vec<_>, _>>()?;
        let step = Step { kind, on_error, stages, streamed: wire.streamed };
        if step.streamed {
            let fed = match kind {
                CompositeKind::Map => matches!(
                    step.stages.as_slice(),
                    [Stage::Task(template)] if matches!(template.input, BoundInput::Previous) && template.fields.is_empty()
                ),
                _ => step.stages.is_empty(),
            };
            return if fed { Ok(step) } else { Err(MalformedStep::StreamedWithStages) };
        }
        if kind == CompositeKind::Map && (!step.is_map_shaped() || step.template_has_fields()) {
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
            streamed: step.streamed,
        }
    }
}

impl TryFrom<&generated::StreamedMember> for StreamedMember {
    type Error = MalformedStep;

    fn try_from(wire: &generated::StreamedMember) -> Result<Self, MalformedStep> {
        use generated::streamed_member::Member;
        match wire.member.as_ref().ok_or(MalformedStep::EmptyMember)? {
            Member::Value(bytes) => Ok(StreamedMember::Item(BoundInput::Value(bytes.clone()))),
            Member::ValueManifest(manifest) => Ok(StreamedMember::Item(BoundInput::Chunked(
                Manifest::try_from(manifest).map_err(|_| MalformedStep::BadManifest)?,
            ))),
            Member::Stage(stage) => Stage::try_from(stage).map(StreamedMember::Stage),
        }
    }
}

impl From<&StreamedMember> for generated::StreamedMember {
    fn from(member: &StreamedMember) -> Self {
        use generated::streamed_member::Member;
        let member = match member {
            StreamedMember::Item(BoundInput::Value(bytes)) => Member::Value(bytes.clone()),
            StreamedMember::Item(BoundInput::Chunked(manifest)) => Member::ValueManifest(manifest.clone().into()),
            // An item always carries its input; an open one has no wire form.
            StreamedMember::Item(BoundInput::Previous) => Member::Value(Vec::new()),
            StreamedMember::Stage(stage) => Member::Stage(stage.into()),
        };
        generated::StreamedMember { member: Some(member) }
    }
}

impl TryFrom<&generated::SubmitMembers> for MemberBatch {
    type Error = MalformedStep;

    fn try_from(wire: &generated::SubmitMembers) -> Result<Self, MalformedStep> {
        Ok(MemberBatch {
            composite_id: CompositeId::from(wire.composite_id.clone().ok_or(MalformedStep::NoComposite)?),
            start_index: wire.start_index,
            members: wire.items.iter().map(StreamedMember::try_from).collect::<Result<Vec<_>, _>>()?,
            last: wire.last,
        })
    }
}

impl From<&MemberBatch> for generated::SubmitMembers {
    fn from(batch: &MemberBatch) -> Self {
        generated::SubmitMembers {
            composite_id: Some(batch.composite_id.clone().into()),
            start_index: batch.start_index,
            items: batch.members.iter().map(generated::StreamedMember::from).collect(),
            last: batch.last,
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
