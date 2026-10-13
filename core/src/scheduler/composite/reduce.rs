//! A reduce: a composite that folds a list into one value with a reducer
//! task. These are its description and the shape of its item pages; the
//! leader reads them and never runs the reducer.

use crate::manifest::Manifest;
use crate::protocol::generated::{self, composite_entry, reduce_options};

use super::{BoundInput, BoundStep, MalformedStep};

/// The first page of a reduce's items; item `k` is on page
/// `ITEM_PAGES + k / PAGE_ENTRIES`.
pub const ITEM_PAGES: u32 = 1 << 31;

/// What a reduce composite folds with, and how.
#[derive(Debug, Clone, PartialEq)]
pub struct ReduceShape {
    /// The reducer every step runs; its input is `BoundInput::Previous` and it binds no fields.
    pub reducer: BoundStep,
    /// Chunks folded side by side and combined pairwise, instead of one step per item.
    pub tree: bool,
    /// Items per tree chunk, at least 1 for a tree; unused by a chain.
    pub chunk_size: u32,
    /// The result before any item is folded: `Value` or `Chunked` (blobs the reduce owns). `None`: the
    /// first item is.
    pub initial: Option<BoundInput>,
}

/// Where one reduce step sits: level 0 folds items, a higher level combines two results below it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReduceNode {
    pub level: u32,
    pub position: u64,
}

/// One item of a reduce, as its item page holds it.
#[derive(Debug, Clone, PartialEq)]
pub enum ReduceItem {
    /// Encoded by the reducer's serializer as its item type.
    Value(Vec<u8>),
    /// The same, stored as blobs the reduce owns.
    Manifest(Manifest),
}

impl ReduceItem {
    /// The item an entry of an item page holds; `None` for an entry that
    /// holds none, or whose manifest cannot be read.
    pub fn of_entry(entry: &generated::CompositeEntry) -> Option<ReduceItem> {
        match entry.outcome.as_ref()? {
            composite_entry::Outcome::Item(bytes) => Some(ReduceItem::Value(bytes.clone())),
            composite_entry::Outcome::ItemManifest(manifest) => Manifest::try_from(manifest).ok().map(ReduceItem::Manifest),
            _ => None,
        }
    }
}

impl From<&ReduceItem> for composite_entry::Outcome {
    fn from(item: &ReduceItem) -> Self {
        match item {
            ReduceItem::Value(bytes) => composite_entry::Outcome::Item(bytes.clone()),
            ReduceItem::Manifest(manifest) => composite_entry::Outcome::ItemManifest(manifest.clone().into()),
        }
    }
}

impl From<ReduceNode> for generated::ReduceNode {
    fn from(node: ReduceNode) -> Self {
        generated::ReduceNode { level: node.level, position: node.position }
    }
}

impl From<&generated::ReduceNode> for ReduceNode {
    fn from(node: &generated::ReduceNode) -> Self {
        ReduceNode { level: node.level, position: node.position }
    }
}

impl TryFrom<&generated::ReduceOptions> for ReduceShape {
    type Error = MalformedStep;

    fn try_from(wire: &generated::ReduceOptions) -> Result<Self, MalformedStep> {
        let reducer = wire.reducer.as_ref().ok_or(MalformedStep::ReduceShape)?;
        let initial = match wire.initial.as_ref() {
            None => None,
            Some(reduce_options::Initial::InitialValue(bytes)) => Some(BoundInput::Value(bytes.clone())),
            Some(reduce_options::Initial::InitialManifest(manifest)) => Some(BoundInput::Chunked(
                Manifest::try_from(manifest).map_err(|_| MalformedStep::BadManifest)?,
            )),
        };
        Ok(ReduceShape {
            reducer: BoundStep::try_from(reducer)?,
            tree: wire.tree,
            chunk_size: wire.chunk_size,
            initial,
        })
    }
}

impl From<&ReduceShape> for generated::ReduceOptions {
    fn from(shape: &ReduceShape) -> Self {
        let initial = match &shape.initial {
            None | Some(BoundInput::Previous) => None,
            Some(BoundInput::Value(bytes)) => Some(reduce_options::Initial::InitialValue(bytes.clone())),
            Some(BoundInput::Chunked(manifest)) => {
                Some(reduce_options::Initial::InitialManifest(manifest.clone().into()))
            }
        };
        generated::ReduceOptions {
            reducer: Some(generated::BoundTaskStep::from(&shape.reducer)),
            tree: shape.tree,
            chunk_size: shape.chunk_size,
            initial,
        }
    }
}
