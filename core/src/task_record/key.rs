//! Which record of a shard a revision is: a Task's, a composite's, or one
//! page of a composite's member outcomes. Every holder keeps all three in
//! one store, and a leader writes all three by the same path.

use prost::Message as _;

use crate::protocol::generated::{self, CompositePage, CompositeRecord, TaskRecord};
use crate::protocol::ids::{CompositeId, TaskId, WorkerId};
use crate::task_record::store::{PutRefusal, identify};
use crate::task_record::version::RecordVersion;

/// The prefix of every composite record's and page's kad key. A task id
/// never contains `/` (see `IdGenerator`), so no Task record's key starts
/// with it.
const COMPOSITE_PREFIX: &str = "composite/";

/// Which record of a shard a revision is.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecordKey {
    Task(TaskId),
    Composite(CompositeId),
    /// Page `.1` of the composite `.0`.
    Page(CompositeId, u32),
}

impl RecordKey {
    /// The bytes kad stores the record under: a task id's own bytes (as
    /// before composites existed), `composite/<id>`, or
    /// `composite/<id>/<page>`.
    pub fn kad_bytes(&self) -> Vec<u8> {
        match self {
            RecordKey::Task(task) => task.as_str().as_bytes().to_vec(),
            RecordKey::Composite(id) => format!("{COMPOSITE_PREFIX}{}", id.as_str()).into_bytes(),
            RecordKey::Page(id, page) => format!("{COMPOSITE_PREFIX}{}/{page}", id.as_str()).into_bytes(),
        }
    }

    /// The key whose `kad_bytes` are `bytes`; `None` for bytes no record
    /// is stored under (not UTF-8, or a malformed composite key).
    pub fn from_kad_bytes(bytes: &[u8]) -> Option<RecordKey> {
        let text = std::str::from_utf8(bytes).ok()?;
        let Some(rest) = text.strip_prefix(COMPOSITE_PREFIX) else {
            return Some(RecordKey::Task(TaskId::new(text)));
        };
        match rest.split_once('/') {
            None => Some(RecordKey::Composite(CompositeId::new(rest))),
            Some((id, page)) => Some(RecordKey::Page(CompositeId::new(id), page.parse().ok()?)),
        }
    }
}

/// One revision of a record a shard's leader writes: a Task's, a
/// composite's, or one page of a composite's member outcomes.
#[derive(Debug, Clone, PartialEq)]
pub enum ShardRecord {
    Task(TaskRecord),
    Composite(CompositeRecord),
    Page(CompositePage),
}

impl From<TaskRecord> for ShardRecord {
    fn from(record: TaskRecord) -> Self {
        ShardRecord::Task(record)
    }
}

impl From<CompositeRecord> for ShardRecord {
    fn from(record: CompositeRecord) -> Self {
        ShardRecord::Composite(record)
    }
}

impl From<CompositePage> for ShardRecord {
    fn from(record: CompositePage) -> Self {
        ShardRecord::Page(record)
    }
}

impl ShardRecord {
    /// The key and version a well-formed record names; `Malformed` when it
    /// lacks its version or the id its kind is keyed by.
    pub fn identify(&self) -> Result<(RecordKey, RecordVersion), PutRefusal> {
        match self {
            ShardRecord::Task(record) => {
                let (task, version) = identify(record)?;
                Ok((RecordKey::Task(task), version))
            }
            ShardRecord::Composite(record) => {
                let version = record.version.as_ref().ok_or(PutRefusal::Malformed)?;
                let id = composite_id_of(record.composite_id.as_ref())?;
                Ok((RecordKey::Composite(id), RecordVersion::from(version)))
            }
            ShardRecord::Page(record) => {
                let version = record.version.as_ref().ok_or(PutRefusal::Malformed)?;
                let id = composite_id_of(record.composite_id.as_ref())?;
                Ok((RecordKey::Page(id, record.index), RecordVersion::from(version)))
            }
        }
    }

    /// The voters this revision was written to.
    pub fn placement(&self) -> &[generated::WorkerId] {
        match self {
            ShardRecord::Task(record) => &record.placement,
            ShardRecord::Composite(record) => &record.placement,
            ShardRecord::Page(record) => &record.placement,
        }
    }

    /// The placements earlier revisions were written to that this one had to
    /// reach as well.
    pub fn prior_placements(&self) -> &[generated::PlacementHolders] {
        match self {
            ShardRecord::Task(record) => &record.prior_placements,
            ShardRecord::Composite(record) => &record.prior_placements,
            ShardRecord::Page(record) => &record.prior_placements,
        }
    }

    /// Places the record on `holders`, carrying `prior` placements.
    pub fn set_placement(&mut self, holders: Vec<generated::WorkerId>, prior: Vec<generated::PlacementHolders>) {
        let (placement, prior_placements) = match self {
            ShardRecord::Task(record) => (&mut record.placement, &mut record.prior_placements),
            ShardRecord::Composite(record) => (&mut record.placement, &mut record.prior_placements),
            ShardRecord::Page(record) => (&mut record.placement, &mut record.prior_placements),
        };
        *placement = holders;
        *prior_placements = prior;
    }

    /// Places the record on `holders`, keeping the prior placements it
    /// carries.
    pub fn place_on(&mut self, holders: Vec<WorkerId>) {
        let prior = self.prior_placements().to_vec();
        self.set_placement(holders.into_iter().map(Into::into).collect(), prior);
    }

    /// Whether nothing in the record changes again.
    pub fn is_finished(&self) -> bool {
        match self {
            ShardRecord::Task(record) => record.finished,
            ShardRecord::Composite(record) => record.finished,
            ShardRecord::Page(record) => record.finished,
        }
    }

    /// The record's wire encoding.
    pub fn encode_to_vec(&self) -> Vec<u8> {
        match self {
            ShardRecord::Task(record) => record.encode_to_vec(),
            ShardRecord::Composite(record) => record.encode_to_vec(),
            ShardRecord::Page(record) => record.encode_to_vec(),
        }
    }

    /// The record of the kind `key` names, decoded from `bytes`.
    pub fn decode(key: &RecordKey, bytes: &[u8]) -> Result<ShardRecord, prost::DecodeError> {
        Ok(match key {
            RecordKey::Task(_) => ShardRecord::Task(TaskRecord::decode(bytes)?),
            RecordKey::Composite(_) => ShardRecord::Composite(CompositeRecord::decode(bytes)?),
            RecordKey::Page(..) => ShardRecord::Page(CompositePage::decode(bytes)?),
        })
    }

    /// The record's key fields, version, placement and whether it is
    /// finished, and nothing else: all a holder keeps of a revision the
    /// leader placed elsewhere. A Task's stub also keeps its definition and
    /// coalescing key.
    pub(crate) fn stub(&self) -> ShardRecord {
        match self {
            ShardRecord::Task(record) => ShardRecord::Task(TaskRecord {
                task: record.task.as_ref().map(|task| generated::Task {
                    task_id: task.task_id.clone(),
                    task_definition_id: task.task_definition_id.clone(),
                    coalescing_key: task.coalescing_key.clone(),
                    ..Default::default()
                }),
                version: record.version,
                placement: record.placement.clone(),
                finished: record.finished,
                ..Default::default()
            }),
            ShardRecord::Composite(record) => ShardRecord::Composite(CompositeRecord {
                version: record.version,
                composite_id: record.composite_id.clone(),
                placement: record.placement.clone(),
                finished: record.finished,
                ..Default::default()
            }),
            ShardRecord::Page(record) => ShardRecord::Page(CompositePage {
                version: record.version,
                composite_id: record.composite_id.clone(),
                index: record.index,
                placement: record.placement.clone(),
                finished: record.finished,
                ..Default::default()
            }),
        }
    }
}

fn composite_id_of(id: Option<&generated::CompositeId>) -> Result<CompositeId, PutRefusal> {
    id.map(|id| CompositeId::new(id.value.clone())).ok_or(PutRefusal::Malformed)
}
