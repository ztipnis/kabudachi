//! How an input or a result is read: the one place that says how a member's
//! worker finds what its stage is given, and how a client finds what a
//! composite produced.
//!
//! Nothing here runs on the leader. A reader asks a [`CompositeSource`] for
//! the records and blobs, which are the scheduler's own copy in a one-node
//! runtime and the shard's over the network, and decodes them itself.

use std::future::Future;
use std::pin::Pin;

use crate::blob::{BlobKey, BlobOwner};
use crate::manifest::{Failure, Manifest, ManifestEntry};
use crate::protocol::digest::Digest;
use crate::protocol::generated::{
    CompositeEntry, CompositePage, CompositeRecord, composite_entry, composite_record, input_shape,
};
use crate::protocol::ids::{CompositeId, TaskDefinitionId};
use crate::protocol::messages::Task;
use crate::protocol::messages::prelude::*;

use super::{BoundInput, BoundStep, CompositeKind, Stage, Step};

/// Where a reader finds a composite's records and the blobs they name: the
/// scheduler's own copy in a one-node runtime, the shard over the network.
pub trait CompositeSource: Sync {
    fn composite(&self, id: &CompositeId) -> impl Future<Output = Option<CompositeRecord>> + Send;

    fn page(&self, id: &CompositeId, page: u32) -> impl Future<Output = Option<CompositePage>> + Send;

    /// The blob `owner` stored under `key`.
    fn blob(&self, owner: &BlobOwner, key: &BlobKey) -> impl Future<Output = Option<Vec<u8>>> + Send;

    /// A chunked payload `owner` owns, joined and checked against `manifest`.
    fn payload(
        &self,
        owner: &BlobOwner,
        manifest: &Manifest,
    ) -> impl Future<Output = Result<Vec<u8>, Unresolved>> + Send;
}

/// A result or input as a reader decodes it: one value with the definition
/// whose serializer encoded it, a failure, or a list of either, nested as the
/// composites that made them were.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    Value { producer: TaskDefinitionId, bytes: Vec<u8> },
    Failure(Failure),
    List(Vec<Resolved>),
}

/// What a member's worker needs besides its claim: its bound step, and the
/// input it takes when it takes one.
#[derive(Debug, Clone, PartialEq)]
pub struct MemberInput {
    pub step: BoundStep,
    /// What it takes from before it: the previous stage's result or the
    /// composite's input.
    pub input: Option<Resolved>,
    /// Its own whole input, bound into the step as blobs its tree's root
    /// (`CompositeRecord.root`) owns, joined; encoded by the member's own
    /// serializer.
    pub own_input: Option<Vec<u8>>,
}

/// Why a record or a blob a reader needs could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unresolved {
    #[error("composite {0:?} could not be found")]
    MissingComposite(CompositeId),
    #[error("page {1} of composite {0:?} could not be found")]
    MissingPage(CompositeId, u32),
    #[error("blob {0} could not be found")]
    MissingBlob(BlobKey),
    #[error("a record or blob a reader needs cannot be read")]
    Malformed,
}

type Resolving<'a> = Pin<Box<dyn Future<Output = Result<Resolved, Unresolved>> + Send + 'a>>;

/// Member `task.composite_index` of `task.composite_id`'s step, and the input
/// it takes. A member bound to a whole value takes none: the claim carries it.
/// A member bound to blobs reads them. Otherwise its `input_manifest` names the
/// input: pages of a composite (a list), or the blobs of one value, whose
/// producer is the flow's previous stage or, for a nested composite's member,
/// the composite's input shape.
pub async fn member_input<S: CompositeSource>(source: &S, task: &Task) -> Result<MemberInput, Unresolved> {
    let id = task.composite_id().ok_or(Unresolved::Malformed)?;
    let record = source
        .composite(&id)
        .await
        .ok_or_else(|| Unresolved::MissingComposite(id.clone()))?;
    let step = step_of(&record)?;
    let index = task.composite_index;
    let Some(Stage::Task(bound)) = usize::try_from(index).ok().and_then(|index| step.stages.get(index)).cloned()
    else {
        return Err(Unresolved::Malformed);
    };
    match &bound.input {
        BoundInput::Value(_) => return Ok(MemberInput { step: bound, input: None, own_input: None }),
        BoundInput::Chunked(chunks) => {
            // The tree's root owns every large bound value.
            let root: CompositeId = record.root.clone().ok_or(Unresolved::Malformed)?.into();
            let own = source.payload(&BlobOwner::Composite(root), chunks).await?;
            return Ok(MemberInput { step: bound, input: None, own_input: Some(own) });
        }
        BoundInput::Previous => {}
    }
    let manifest = task
        .input_manifest
        .as_ref()
        .ok_or(Unresolved::Malformed)
        .and_then(|manifest| Manifest::try_from(manifest).map_err(|_| Unresolved::Malformed))?;
    let (producer, owner) = match (step.kind, index) {
        (CompositeKind::Flow, previous @ 1..) => {
            let owner = BlobOwner::Task(id.member_task(previous - 1));
            match &step.stages[previous as usize - 1] {
                Stage::Task(before) => (Some(before.definition.clone()), Some(owner)),
                Stage::Composite(_) => (None, None),
            }
        }
        _ => (input_producer(&record), input_owner(source, &record).await?),
    };
    let input = resolve_manifest(source, &manifest, producer.as_ref(), owner.as_ref()).await?;
    Ok(MemberInput { step: bound, input: Some(input), own_input: None })
}

/// `manifest` as one value or a list. A manifest of pages is a list: the
/// entries of those pages in order, each resolved in turn (a child composite's
/// entry is that composite's results, nested). A manifest of one blob is one
/// value: the blob is the manifest of a result `owner` stored, which `producer`
/// encoded.
pub async fn resolve_manifest<S: CompositeSource>(
    source: &S,
    manifest: &Manifest,
    producer: Option<&TaskDefinitionId>,
    owner: Option<&BlobOwner>,
) -> Result<Resolved, Unresolved> {
    resolving_manifest(source, manifest, producer, owner).await
}

/// A finished composite's results, as its client reads them: its pages'
/// entries in index order. An expanding map's results are those of the map its
/// expansion returned.
pub async fn results<S: CompositeSource>(source: &S, id: &CompositeId) -> Result<Resolved, Unresolved> {
    resolving_results(source, id).await
}

fn resolving_manifest<'a, S: CompositeSource>(
    source: &'a S,
    manifest: &'a Manifest,
    producer: Option<&'a TaskDefinitionId>,
    owner: Option<&'a BlobOwner>,
) -> Resolving<'a> {
    Box::pin(async move {
        if let [ManifestEntry::Blob(key)] = manifest.entries() {
            let (producer, owner) = producer.zip(owner).ok_or(Unresolved::Malformed)?;
            let bytes = read_result(source, owner, key).await?;
            return Ok(Resolved::Value { producer: producer.clone(), bytes });
        }
        let mut list = Vec::new();
        for entry in manifest.entries() {
            match entry {
                ManifestEntry::Page { composite, page } => {
                    let held = source
                        .page(composite, *page)
                        .await
                        .ok_or_else(|| Unresolved::MissingPage(composite.clone(), *page))?;
                    for entry in &held.entries {
                        list.push(resolving_entry(source, composite, entry).await?);
                    }
                }
                ManifestEntry::Failure(failure) => list.push(Resolved::Failure(failure.clone())),
                ManifestEntry::Blob(_) | ManifestEntry::Inline(_) => return Err(Unresolved::Malformed),
            }
        }
        Ok(Resolved::List(list))
    })
}

fn resolving_results<'a, S: CompositeSource>(source: &'a S, id: &'a CompositeId) -> Resolving<'a> {
    Box::pin(async move {
        let record = source
            .composite(id)
            .await
            .ok_or_else(|| Unresolved::MissingComposite(id.clone()))?;
        if let Some(members) = expanded_into(source, id, &record).await? {
            return resolving_results(source, &members).await;
        }
        let mut list = Vec::new();
        for page in 0..record.pages {
            let held = source.page(id, page).await.ok_or_else(|| Unresolved::MissingPage(id.clone(), page))?;
            for entry in &held.entries {
                list.push(resolving_entry(source, id, entry).await?);
            }
        }
        Ok(Resolved::List(list))
    })
}

/// One entry of a page of `composite`: a value its member Task stored, a
/// failure, or the results of the composite the member was.
fn resolving_entry<'a, S: CompositeSource>(
    source: &'a S,
    composite: &'a CompositeId,
    entry: &'a CompositeEntry,
) -> Resolving<'a> {
    Box::pin(async move {
        match entry.outcome.as_ref().ok_or(Unresolved::Malformed)? {
            composite_entry::Outcome::Value(digest) => {
                let key = BlobKey::from(&Digest::try_from(digest).map_err(|_| Unresolved::Malformed)?);
                let producer: TaskDefinitionId = entry.producer.clone().ok_or(Unresolved::Malformed)?.into();
                let owner = BlobOwner::Task(composite.member_task(entry.index));
                let bytes = read_result(source, &owner, &key).await?;
                Ok(Resolved::Value { producer, bytes })
            }
            composite_entry::Outcome::Failure(failure) => Ok(Resolved::Failure(Failure {
                kind: failure.kind.clone(),
                message: failure.message.clone(),
            })),
            composite_entry::Outcome::Child(child) => {
                let child: CompositeId = child.clone().into();
                resolving_results(source, &child).await
            }
        }
    })
}

/// A result `owner` stored: the blob under `key` is the encoded manifest of
/// its bytes.
async fn read_result<S: CompositeSource>(source: &S, owner: &BlobOwner, key: &BlobKey) -> Result<Vec<u8>, Unresolved> {
    let bytes = source.blob(owner, key).await.ok_or_else(|| Unresolved::MissingBlob(key.clone()))?;
    if !key.holds(&bytes) {
        return Err(Unresolved::Malformed);
    }
    let manifest = Manifest::decode(&bytes).map_err(|_| Unresolved::Malformed)?;
    source.payload(owner, &manifest).await
}

/// The map an expanding map's expansion returned, once it has.
async fn expanded_into<S: CompositeSource>(
    source: &S,
    id: &CompositeId,
    record: &CompositeRecord,
) -> Result<Option<CompositeId>, Unresolved> {
    if !step_of(record)?.expands() || record.pages == 0 {
        return Ok(None);
    }
    let page = source.page(id, 0).await.ok_or_else(|| Unresolved::MissingPage(id.clone(), 0))?;
    Ok(page.entries.iter().find_map(|entry| match (entry.index, entry.outcome.as_ref()) {
        (0, Some(composite_entry::Outcome::Child(members))) => Some(members.clone().into()),
        _ => None,
    }))
}

fn step_of(record: &CompositeRecord) -> Result<Step, Unresolved> {
    Step::try_from(record.description.as_ref().ok_or(Unresolved::Malformed)?).map_err(|_| Unresolved::Malformed)
}

/// The task whose serializer encoded the input a nested composite is given,
/// when it is given one value.
fn input_producer(record: &CompositeRecord) -> Option<TaskDefinitionId> {
    match record.input_shape.as_ref()?.shape.as_ref()? {
        input_shape::Shape::ValueOf(producer) => Some(producer.clone().into()),
        input_shape::Shape::List(_) => None,
    }
}

/// Who owns the blob a nested composite's input names: the task of the flow
/// stage before the composite, or, when the composite takes its own parent's
/// input, whoever owns that one.
async fn input_owner<S: CompositeSource>(
    source: &S,
    record: &CompositeRecord,
) -> Result<Option<BlobOwner>, Unresolved> {
    let mut current = record.clone();
    loop {
        let Some(composite_record::Parent::ParentComposite(parent)) = current.parent.clone() else {
            return Ok(None);
        };
        let parent: CompositeId = parent.into();
        let held = source
            .composite(&parent)
            .await
            .ok_or_else(|| Unresolved::MissingComposite(parent.clone()))?;
        let index = current.parent_index;
        if step_of(&held)?.kind == CompositeKind::Flow && index > 0 {
            return Ok(Some(BlobOwner::Task(parent.member_task(index - 1))));
        }
        current = held;
    }
}
