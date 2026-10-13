//! Result backends: where certified results are mirrored, and how a reader
//! checks what it loads.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use prost::Message;

use crate::manifest::Failure;
use crate::protocol::digest::{Digest, DigestAlgorithm};
use crate::protocol::generated;
use crate::protocol::ids::{CompositeId, TaskDefinitionId, TaskId, TaskRunId};
use crate::time::{Clock, WallTime};

/// Whether a certified run's result was mirrored to a result backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceState {
    /// Nothing is mirrored: no backend, a task that keeps no result, or a
    /// result that is not a task's own value.
    Disabled,
    /// Its worker is mirroring it.
    Pending,
    Stored,
    /// The backend refused it or did not answer; the run's outcome stands.
    Failed,
}

impl From<PersistenceState> for generated::PersistenceState {
    fn from(state: PersistenceState) -> Self {
        match state {
            PersistenceState::Disabled => generated::PersistenceState::Disabled,
            PersistenceState::Pending => generated::PersistenceState::Pending,
            PersistenceState::Stored => generated::PersistenceState::Stored,
            PersistenceState::Failed => generated::PersistenceState::Failed,
        }
    }
}

/// The wire's `PERSISTENCE_STATE_UNSPECIFIED`, which no worker sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("no persistence state was given")]
pub struct UnspecifiedPersistence;

impl TryFrom<generated::PersistenceState> for PersistenceState {
    type Error = UnspecifiedPersistence;

    fn try_from(state: generated::PersistenceState) -> Result<Self, Self::Error> {
        match state {
            generated::PersistenceState::Disabled => Ok(PersistenceState::Disabled),
            generated::PersistenceState::Pending => Ok(PersistenceState::Pending),
            generated::PersistenceState::Stored => Ok(PersistenceState::Stored),
            generated::PersistenceState::Failed => Ok(PersistenceState::Failed),
            generated::PersistenceState::Unspecified => Err(UnspecifiedPersistence),
        }
    }
}

/// How large the parts of a stored result may be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultBounds {
    /// A value up to this is stored inline; a larger one as chunks.
    pub inline_bytes: usize,
    /// The largest chunk.
    pub chunk_bytes: usize,
    /// A page closes before the member that would take its encoding past this.
    pub page_bytes: usize,
}

impl ResultBounds {
    /// 64 KiB inline, 256 KiB chunks, 256 KiB pages.
    pub const DEFAULT: ResultBounds =
        ResultBounds { inline_bytes: 64 * 1024, chunk_bytes: 256 * 1024, page_bytes: 256 * 1024 };
}

/// Whose value a chunk is a part of.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResultOwner {
    Task(TaskId),
    Composite(CompositeId),
}

/// Where one entry of a stored result is filed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResultKey {
    Task(TaskId),
    Composite(CompositeId),
    Page(CompositeId, u32),
    Chunk(ResultOwner, u32),
}

impl ResultKey {
    /// `task/<id>`, `task/<id>/chunk/<n>`, `composite/<id>`,
    /// `composite/<id>/page/<n>` or `composite/<id>/chunk/<n>`: the text a
    /// backend files the entry under.
    pub fn as_text(&self) -> String {
        match self {
            ResultKey::Task(id) => format!("task/{}", id.as_str()),
            ResultKey::Composite(id) => format!("composite/{}", id.as_str()),
            ResultKey::Page(id, page) => format!("composite/{}/page/{page}", id.as_str()),
            ResultKey::Chunk(ResultOwner::Task(id), chunk) => {
                format!("task/{}/chunk/{chunk}", id.as_str())
            }
            ResultKey::Chunk(ResultOwner::Composite(id), chunk) => {
                format!("composite/{}/chunk/{chunk}", id.as_str())
            }
        }
    }
}

/// A composite's results as they are mirrored and resumed: values with the
/// definition that produced them and the serializer that encoded them,
/// failures, and lists, nested as the composites that made them were.
///
/// A failure keeps the Task whose run raised it, so a resumed error names
/// the same task as the original.
#[derive(Debug, Clone, PartialEq)]
pub enum ResultTree {
    Value { producer: TaskDefinitionId, serializer: String, bytes: Vec<u8> },
    Failure(Failure),
    List(Vec<ResultTree>),
}

/// One member of a composite's flattened results, in preorder.
#[derive(Debug, Clone, PartialEq)]
pub enum StoredMember {
    Value { producer: TaskDefinitionId, serializer: String, bytes: StoredBytes },
    Failure(Failure),
    /// The next `n` members, each with its own subtree, form a list.
    List(u64),
}

/// A member's bytes: in its page, or in chunks of the composite.
#[derive(Debug, Clone, PartialEq)]
pub enum StoredBytes {
    Inline(Vec<u8>),
    Chunks { first: u32, count: u32, total_bytes: u64 },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResultValue {
    /// A task's value, whole.
    Payload(Vec<u8>),
    /// A task's value in `chunks` chunks.
    Chunked { chunks: u32, total_bytes: u64 },
    /// A composite's head: how many pages and members its results have.
    Composite { pages: u32, members: u64 },
    Page(Vec<StoredMember>),
    Chunk(Vec<u8>),
}

/// One entry of a stored result, as a backend keeps it.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultEntry {
    pub key: ResultKey,
    /// A task head's certified run; `None` otherwise.
    pub run: Option<TaskRunId>,
    /// A head's digest: the task run's certified digest, or the composite's
    /// digest over its pages and chunks. `None` on a page or a chunk.
    pub digest: Option<Digest>,
    pub value: ResultValue,
    pub expires_at: WallTime,
}

impl ResultEntry {
    pub fn encode(&self) -> Vec<u8> {
        let owner = match &self.key {
            ResultKey::Task(id) | ResultKey::Chunk(ResultOwner::Task(id), _) => {
                generated::stored_result::Owner::Task(id.clone().into())
            }
            ResultKey::Composite(id)
            | ResultKey::Page(id, _)
            | ResultKey::Chunk(ResultOwner::Composite(id), _) => {
                generated::stored_result::Owner::Composite(id.clone().into())
            }
        };
        let index = match &self.key {
            ResultKey::Page(_, index) | ResultKey::Chunk(_, index) => *index,
            ResultKey::Task(_) | ResultKey::Composite(_) => 0,
        };
        let value = match &self.value {
            ResultValue::Payload(bytes) => generated::stored_result::Value::Payload(bytes.clone()),
            ResultValue::Chunked { chunks, total_bytes } => {
                generated::stored_result::Value::Chunked(generated::ChunkedValue {
                    chunks: *chunks,
                    total_bytes: *total_bytes,
                })
            }
            ResultValue::Composite { pages, members } => {
                generated::stored_result::Value::CompositeHead(generated::CompositeHead {
                    pages: *pages,
                    members: *members,
                })
            }
            ResultValue::Page(members) => generated::stored_result::Value::Page(page_to_wire(members)),
            ResultValue::Chunk(bytes) => generated::stored_result::Value::Chunk(bytes.clone()),
        };
        generated::StoredResult {
            owner: Some(owner),
            task_run_id: self.run.clone().map(Into::into),
            result_digest: self.digest.clone().map(Into::into),
            value: Some(value),
            expires_at: Some(generated::WallTime { unix_millis: self.expires_at.as_unix_millis() }),
            index,
        }
        .encode_to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BackendError> {
        let wire = generated::StoredResult::decode(bytes).map_err(|_| BackendError::Corrupt)?;
        let owner = match wire.owner.ok_or(BackendError::Corrupt)? {
            generated::stored_result::Owner::Task(id) => ResultOwner::Task(id.into()),
            generated::stored_result::Owner::Composite(id) => ResultOwner::Composite(id.into()),
        };
        let expires_at = wire.expires_at.ok_or(BackendError::Corrupt)?;
        let expires_at = WallTime::from_unix_millis(expires_at.unix_millis);
        let digest = wire
            .result_digest
            .as_ref()
            .map(Digest::try_from)
            .transpose()
            .map_err(|_| BackendError::Corrupt)?;
        let run = wire.task_run_id.map(TaskRunId::from);
        let (key, run, digest, value) = match (owner, wire.value.ok_or(BackendError::Corrupt)?) {
            (ResultOwner::Task(task), generated::stored_result::Value::Payload(bytes)) => (
                ResultKey::Task(task),
                Some(run.ok_or(BackendError::Corrupt)?),
                Some(digest.ok_or(BackendError::Corrupt)?),
                ResultValue::Payload(bytes),
            ),
            (ResultOwner::Task(task), generated::stored_result::Value::Chunked(chunked)) => (
                ResultKey::Task(task),
                Some(run.ok_or(BackendError::Corrupt)?),
                Some(digest.ok_or(BackendError::Corrupt)?),
                ResultValue::Chunked { chunks: chunked.chunks, total_bytes: chunked.total_bytes },
            ),
            (
                ResultOwner::Composite(composite),
                generated::stored_result::Value::CompositeHead(head),
            ) => (
                ResultKey::Composite(composite),
                None,
                Some(digest.ok_or(BackendError::Corrupt)?),
                ResultValue::Composite { pages: head.pages, members: head.members },
            ),
            (ResultOwner::Composite(composite), generated::stored_result::Value::Page(page)) => (
                ResultKey::Page(composite, wire.index),
                None,
                None,
                ResultValue::Page(page_from_wire(page)?),
            ),
            (owner, generated::stored_result::Value::Chunk(bytes)) => {
                (ResultKey::Chunk(owner, wire.index), None, None, ResultValue::Chunk(bytes))
            }
            _ => return Err(BackendError::Corrupt),
        };
        Ok(ResultEntry { key, run, digest, value, expires_at })
    }

    /// Whether `now` is at or past this entry's expiry.
    pub fn has_expired(&self, now: WallTime) -> bool {
        self.expires_at.as_unix_millis() <= now.as_unix_millis()
    }
}

fn member_to_wire(member: &StoredMember) -> generated::StoredMember {
    let member = match member {
        StoredMember::Value { producer, serializer, bytes } => {
            let bytes_of = match bytes {
                StoredBytes::Inline(bytes) => generated::stored_value::BytesOf::Inline(bytes.clone()),
                StoredBytes::Chunks { first, count, total_bytes } => {
                    generated::stored_value::BytesOf::Chunks(generated::ChunkRange {
                        first: *first,
                        count: *count,
                        total_bytes: *total_bytes,
                    })
                }
            };
            generated::stored_member::Member::Value(generated::StoredValue {
                producer: Some(producer.clone().into()),
                serializer: serializer.clone(),
                bytes_of: Some(bytes_of),
            })
        }
        StoredMember::Failure(failure) => {
            generated::stored_member::Member::Failure(failure.clone().into())
        }
        StoredMember::List(len) => generated::stored_member::Member::List(*len),
    };
    generated::StoredMember { member: Some(member) }
}

fn page_to_wire(members: &[StoredMember]) -> generated::ResultPage {
    generated::ResultPage { members: members.iter().map(member_to_wire).collect() }
}

fn member_from_wire(wire: generated::StoredMember) -> Result<StoredMember, BackendError> {
    Ok(match wire.member.ok_or(BackendError::Corrupt)? {
        generated::stored_member::Member::Value(value) => StoredMember::Value {
            producer: value.producer.ok_or(BackendError::Corrupt)?.into(),
            serializer: value.serializer,
            bytes: match value.bytes_of.ok_or(BackendError::Corrupt)? {
                generated::stored_value::BytesOf::Inline(bytes) => StoredBytes::Inline(bytes),
                generated::stored_value::BytesOf::Chunks(range) => StoredBytes::Chunks {
                    first: range.first,
                    count: range.count,
                    total_bytes: range.total_bytes,
                },
            },
        },
        generated::stored_member::Member::Failure(failure) => StoredMember::Failure((&failure).into()),
        generated::stored_member::Member::List(len) => StoredMember::List(len),
    })
}

fn page_from_wire(page: generated::ResultPage) -> Result<Vec<StoredMember>, BackendError> {
    page.members.into_iter().map(member_from_wire).collect()
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BackendError {
    /// The backend did not answer in time, or could not be reached.
    #[error("the result backend did not answer: {0}")]
    Unavailable(String),
    /// The backend answered with an error.
    #[error("the result backend refused: {0}")]
    Refused(String),
    /// What the backend returned is not a stored result.
    #[error("the result backend returned something that is not a stored result")]
    Corrupt,
}

/// Where certified results are mirrored, so they outlive the process that
/// awaited them, and the shard's records of them. Implement it to keep
/// results anywhere; kabudachi ships an in-memory one and a Redis/Valkey
/// one, and wraps any Python object with async `store` and `load`.
///
/// An adapter is a key-value store with expiry: one result is several
/// entries (see [`task_entries`], [`composite_entries`]), none larger than
/// about the bounds' `page_bytes`. Calls block. Never call one on an async
/// runtime's own threads: use a blocking pool. A failed call changes no
/// task's outcome; its caller logs it and records the run's persistence as
/// failed.
pub trait ResultBackend: Send + Sync {
    /// Files `entry` under its key, replacing any entry there, until
    /// `entry.expires_at`. An entry that has already expired may be dropped.
    fn store(&self, entry: &ResultEntry) -> Result<(), BackendError>;

    /// The entry filed under `key`, unless there is none or it has expired.
    fn load(&self, key: &ResultKey) -> Result<Option<ResultEntry>, BackendError>;
}

/// A result backend in this process's memory, for tests and one-node runs:
/// entries expire by `clock`'s wall time and go with the process.
#[derive(Debug)]
pub struct InMemoryResultBackend<C: Clock> {
    clock: C,
    entries: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl<C: Clock> InMemoryResultBackend<C> {
    pub fn new(clock: C) -> Self {
        InMemoryResultBackend { clock, entries: Mutex::new(BTreeMap::new()) }
    }

    fn entries(&self) -> MutexGuard<'_, BTreeMap<String, Vec<u8>>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<C: Clock + Send + Sync> ResultBackend for InMemoryResultBackend<C> {
    fn store(&self, entry: &ResultEntry) -> Result<(), BackendError> {
        self.entries().insert(entry.key.as_text(), entry.encode());
        Ok(())
    }

    fn load(&self, key: &ResultKey) -> Result<Option<ResultEntry>, BackendError> {
        let mut entries = self.entries();
        let Some(bytes) = entries.get(&key.as_text()) else {
            return Ok(None);
        };
        let entry = ResultEntry::decode(bytes)?;
        if entry.has_expired(WallTime::now(&self.clock)) {
            entries.remove(&key.as_text());
            return Ok(None);
        }
        Ok(Some(entry))
    }
}

/// The entries that mirror a task's certified value, parts first, head last.
pub fn task_entries(
    bounds: &ResultBounds,
    task: &TaskId,
    run: &TaskRunId,
    digest: &Digest,
    payload: &[u8],
    expires_at: WallTime,
) -> Vec<ResultEntry> {
    let owner = ResultOwner::Task(task.clone());
    let mut entries = Vec::new();
    let value = if payload.len() <= bounds.inline_bytes {
        ResultValue::Payload(payload.to_vec())
    } else {
        for (index, piece) in payload.chunks(bounds.chunk_bytes.max(1)).enumerate() {
            entries.push(chunk_entry(&owner, index, piece, expires_at));
        }
        ResultValue::Chunked { chunks: count_u32(entries.len()), total_bytes: payload.len() as u64 }
    };
    entries.push(ResultEntry {
        key: ResultKey::Task(task.clone()),
        run: Some(run.clone()),
        digest: Some(digest.clone()),
        value,
        expires_at,
    });
    entries
}

fn count_u32(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

fn chunk_entry(owner: &ResultOwner, index: usize, bytes: &[u8], expires_at: WallTime) -> ResultEntry {
    ResultEntry {
        key: ResultKey::Chunk(owner.clone(), count_u32(index)),
        run: None,
        digest: None,
        value: ResultValue::Chunk(bytes.to_vec()),
        expires_at,
    }
}

/// The bytes a page costs in a page's encoding: the member's, its length and
/// the field tag.
fn framed_len(member: &StoredMember) -> usize {
    let len = member_to_wire(member).encoded_len();
    1 + prost::encoding::encoded_len_varint(len as u64) + len
}

/// The entries that mirror composite `id`'s results, parts first, head last.
pub fn composite_entries(
    bounds: &ResultBounds,
    id: &CompositeId,
    results: &ResultTree,
    expires_at: WallTime,
) -> Vec<ResultEntry> {
    let owner = ResultOwner::Composite(id.clone());
    let mut chunks: Vec<&[u8]> = Vec::new();
    let mut members: Vec<StoredMember> = Vec::new();
    let mut open = vec![std::slice::from_ref(results).iter()];
    while let Some(items) = open.last_mut() {
        let Some(item) = items.next() else {
            open.pop();
            continue;
        };
        members.push(match item {
            ResultTree::Value { producer, serializer, bytes } => {
                let bytes = if bytes.len() <= bounds.inline_bytes {
                    StoredBytes::Inline(bytes.clone())
                } else {
                    let first = chunks.len();
                    chunks.extend(bytes.chunks(bounds.chunk_bytes.max(1)));
                    StoredBytes::Chunks {
                        first: count_u32(first),
                        count: count_u32(chunks.len() - first),
                        total_bytes: bytes.len() as u64,
                    }
                };
                StoredMember::Value { producer: producer.clone(), serializer: serializer.clone(), bytes }
            }
            ResultTree::Failure(failure) => StoredMember::Failure(failure.clone()),
            ResultTree::List(children) => {
                open.push(children.iter());
                StoredMember::List(children.len() as u64)
            }
        });
    }
    let member_count = members.len() as u64;

    let mut pages: Vec<Vec<StoredMember>> = Vec::new();
    let mut page_len = 0;
    for member in members {
        let len = framed_len(&member);
        match pages.last_mut() {
            Some(page) if page_len + len <= bounds.page_bytes => {
                page.push(member);
                page_len += len;
            }
            _ => {
                pages.push(vec![member]);
                page_len = len;
            }
        }
    }

    let mut hasher = CompositeHasher::new();
    for page in &pages {
        hasher.part(&page_to_wire(page).encode_to_vec());
    }
    for chunk in &chunks {
        hasher.part(chunk);
    }

    let mut entries = Vec::with_capacity(chunks.len() + pages.len() + 1);
    for (index, chunk) in chunks.iter().enumerate() {
        entries.push(chunk_entry(&owner, index, chunk, expires_at));
    }
    let page_count = count_u32(pages.len());
    for (index, page) in pages.into_iter().enumerate() {
        entries.push(ResultEntry {
            key: ResultKey::Page(id.clone(), count_u32(index)),
            run: None,
            digest: None,
            value: ResultValue::Page(page),
            expires_at,
        });
    }
    entries.push(ResultEntry {
        key: ResultKey::Composite(id.clone()),
        run: None,
        digest: Some(hasher.finish()),
        value: ResultValue::Composite { pages: page_count, members: member_count },
        expires_at,
    });
    entries
}

/// The digest of a composite's stored form: BLAKE3 over every page's
/// encoding, then every chunk's bytes, in order, each prefixed by its length
/// as 8 big-endian bytes.
struct CompositeHasher(blake3::Hasher);

impl CompositeHasher {
    fn new() -> Self {
        CompositeHasher(blake3::Hasher::new())
    }

    fn part(&mut self, bytes: &[u8]) {
        self.0.update(&(bytes.len() as u64).to_be_bytes());
        self.0.update(bytes);
    }

    fn finish(&self) -> Digest {
        Digest::new(DigestAlgorithm::Blake3, self.0.finalize().as_bytes().to_vec())
            .expect("a BLAKE3 hash has a BLAKE3 digest's length")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stored {
    Written,
    AlreadyComplete,
}

/// Stores `entries` (from [`task_entries`] or [`composite_entries`]) unless
/// the backend already holds a complete result under their head's key: a
/// complete head is never rewritten, so two mirrors of one result (its
/// worker's or client's, and a resumer's) never race. An incomplete or
/// mismatched result is written over: only a complete one is final.
pub fn store_unless_complete(
    backend: &dyn ResultBackend,
    entries: &[ResultEntry],
) -> Result<Stored, BackendError> {
    if let Some(head) = entries.last() {
        let held = match (&head.key, &head.run, &head.digest) {
            (ResultKey::Task(task), Some(run), Some(digest)) => {
                read_task_value(backend, task, run, digest).map(|value| value.is_some())
            }
            (ResultKey::Composite(id), ..) => read_composite(backend, id).map(|tree| tree.is_some()),
            _ => Ok(false),
        };
        match held {
            Ok(true) => return Ok(Stored::AlreadyComplete),
            Ok(false) | Err(ReadError::Incomplete | ReadError::Mismatch(_)) => {}
            Err(ReadError::Backend(error)) => return Err(error),
        }
    }
    for entry in entries {
        backend.store(entry)?;
    }
    Ok(Stored::Written)
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadError {
    #[error(transparent)]
    Backend(#[from] BackendError),
    /// A part the head names is gone (expired, evicted, or never stored).
    #[error("a part of the stored result is missing")]
    Incomplete,
    #[error("the stored result does not match ({0})")]
    Mismatch(Mismatch),
}

/// What a stored result held that it should not have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Mismatch {
    #[error("another run")]
    Run,
    #[error("another digest")]
    Digest,
    #[error("a malformed shape")]
    Shape,
}

fn part(backend: &dyn ResultBackend, key: &ResultKey) -> Result<ResultEntry, ReadError> {
    let entry = backend.load(key)?.ok_or(ReadError::Incomplete)?;
    if &entry.key == key { Ok(entry) } else { Err(ReadError::Mismatch(Mismatch::Shape)) }
}

fn chunk(backend: &dyn ResultBackend, owner: &ResultOwner, index: u32) -> Result<Vec<u8>, ReadError> {
    match part(backend, &ResultKey::Chunk(owner.clone(), index))?.value {
        ResultValue::Chunk(bytes) => Ok(bytes),
        _ => Err(ReadError::Mismatch(Mismatch::Shape)),
    }
}

/// `task`'s value as certified for `run` under `digest`: `Ok(None)` when the
/// backend has no head for it.
pub fn read_task_value(
    backend: &dyn ResultBackend,
    task: &TaskId,
    run: &TaskRunId,
    digest: &Digest,
) -> Result<Option<Vec<u8>>, ReadError> {
    let Some(head) = backend.load(&ResultKey::Task(task.clone()))? else {
        return Ok(None);
    };
    if head.run.as_ref() != Some(run) {
        return Err(ReadError::Mismatch(Mismatch::Run));
    }
    if head.digest.as_ref() != Some(digest) {
        return Err(ReadError::Mismatch(Mismatch::Digest));
    }
    let payload = match head.value {
        ResultValue::Payload(payload) => payload,
        ResultValue::Chunked { chunks, total_bytes } => {
            let owner = ResultOwner::Task(task.clone());
            let mut payload = Vec::new();
            for index in 0..chunks {
                payload.extend(chunk(backend, &owner, index)?);
            }
            if payload.len() as u64 != total_bytes {
                return Err(ReadError::Mismatch(Mismatch::Shape));
            }
            payload
        }
        _ => return Err(ReadError::Mismatch(Mismatch::Shape)),
    };
    if &Digest::blake3(&payload) != digest {
        return Err(ReadError::Mismatch(Mismatch::Digest));
    }
    Ok(Some(payload))
}

/// A composite member with its bytes read back.
enum Member {
    Value { producer: TaskDefinitionId, serializer: String, bytes: Vec<u8> },
    Failure(Failure),
    List(u64),
}

/// Composite `id`'s results, reassembled and checked: `Ok(None)` when the
/// backend has no head for it.
pub fn read_composite(
    backend: &dyn ResultBackend,
    id: &CompositeId,
) -> Result<Option<ResultTree>, ReadError> {
    let shape = ReadError::Mismatch(Mismatch::Shape);
    let Some(head) = backend.load(&ResultKey::Composite(id.clone()))? else {
        return Ok(None);
    };
    let (ResultValue::Composite { pages, members }, Some(digest)) = (head.value, head.digest) else {
        return Err(shape);
    };

    let mut hasher = CompositeHasher::new();
    let mut stored = Vec::new();
    for index in 0..pages {
        let ResultValue::Page(page) = part(backend, &ResultKey::Page(id.clone(), index))?.value
        else {
            return Err(shape);
        };
        hasher.part(&page_to_wire(&page).encode_to_vec());
        stored.extend(page);
    }
    if stored.len() as u64 != members {
        return Err(shape);
    }

    let owner = ResultOwner::Composite(id.clone());
    let mut next_chunk = 0u32;
    let mut read = Vec::with_capacity(stored.len());
    for member in stored {
        read.push(match member {
            StoredMember::Value { producer, serializer, bytes: StoredBytes::Inline(bytes) } => {
                Member::Value { producer, serializer, bytes }
            }
            StoredMember::Value {
                producer,
                serializer,
                bytes: StoredBytes::Chunks { first, count, total_bytes },
            } => {
                if first != next_chunk {
                    return Err(shape);
                }
                next_chunk = first.checked_add(count).ok_or(shape.clone())?;
                let mut bytes = Vec::new();
                for index in first..next_chunk {
                    let piece = chunk(backend, &owner, index)?;
                    hasher.part(&piece);
                    bytes.extend(piece);
                }
                if bytes.len() as u64 != total_bytes {
                    return Err(shape);
                }
                Member::Value { producer, serializer, bytes }
            }
            StoredMember::Failure(failure) => Member::Failure(failure),
            StoredMember::List(len) => Member::List(len),
        });
    }
    if hasher.finish() != digest {
        return Err(ReadError::Mismatch(Mismatch::Digest));
    }
    rebuild(read).map(Some).ok_or(shape)
}

/// The tree whose preorder members these are, if they are exactly one tree.
fn rebuild(members: Vec<Member>) -> Option<ResultTree> {
    // Lists still being filled: the items so far and how many more are due.
    let mut open: Vec<(Vec<ResultTree>, u64)> = Vec::new();
    let mut root = None;
    for member in members {
        if root.is_some() {
            return None;
        }
        let mut done = match member {
            Member::Value { producer, serializer, bytes } => {
                ResultTree::Value { producer, serializer, bytes }
            }
            Member::Failure(failure) => ResultTree::Failure(failure),
            Member::List(0) => ResultTree::List(Vec::new()),
            Member::List(len) => {
                open.push((Vec::new(), len));
                continue;
            }
        };
        loop {
            let Some((items, due)) = open.last_mut() else {
                root = Some(done);
                break;
            };
            items.push(done);
            *due -= 1;
            if *due > 0 {
                break;
            }
            done = ResultTree::List(open.pop()?.0);
        }
    }
    if open.is_empty() { root } else { None }
}

/// Where a task stands, as its record says, for a reader deciding whether to
/// look for its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStanding {
    /// No record, or none with a run.
    Unknown,
    Unfinished,
    /// The newest run ended without a value.
    Ended { state: generated::TaskRunState, failure_kind: String },
    /// The newest run returned a step; its results are the composite's.
    Continued(CompositeId),
    Succeeded { run: TaskRunId, digest: Digest, persistence: Option<PersistenceState> },
}

/// Where the task of `record` stands, by its newest run.
pub fn task_standing(record: Option<&generated::TaskRecord>) -> TaskStanding {
    use generated::TaskRunState as State;

    let Some(run) = record.and_then(|record| record.runs.last()) else {
        return TaskStanding::Unknown;
    };
    let Some(run_id) = run.identity.as_ref().and_then(|identity| identity.task_run_id.clone()) else {
        return TaskStanding::Unknown;
    };
    let run_id = TaskRunId::from(run_id);
    match run.state() {
        State::Scheduled | State::Queued | State::Claimed | State::Running => TaskStanding::Unfinished,
        State::Succeeded if run.continuation.is_some() => {
            TaskStanding::Continued(CompositeId::continuation_of(&run_id))
        }
        State::Succeeded => match run.result_digest.as_ref().map(Digest::try_from) {
            Some(Ok(digest)) => TaskStanding::Succeeded {
                run: run_id,
                digest,
                persistence: PersistenceState::try_from(run.persistence()).ok(),
            },
            _ => TaskStanding::Unknown,
        },
        state => TaskStanding::Ended { state, failure_kind: run.failure_kind.clone() },
    }
}
