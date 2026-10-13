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
        // The wire cannot carry a `Previous` initial value; a reduce never has one.
        debug_assert!(!matches!(shape.initial, Some(BoundInput::Previous)));
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

/// The failure kind of a reduce with no items and no initial value; the
/// Python runtime raises it as `ReduceEmptyError`.
pub const REDUCE_EMPTY_KIND: &str = "ReduceEmptyError";

/// One operand of a reduce step, in the order the step takes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operand {
    /// Item `k` of the list, by its place in the list.
    Item(u64),
    /// The reduce's initial value.
    Initial,
    /// The result of an earlier step, by its number.
    Result(u64),
}

/// One step of a reduce and what it works on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedStep {
    pub index: u64,
    pub node: ReduceNode,
    pub operands: Vec<Operand>,
}

impl PlannedStep {
    /// Whether the step combines two results (left, right) instead of
    /// folding items into its first operand.
    pub fn combines(&self) -> bool {
        self.node.level > 0
    }
}

/// What a reduce gives when it is over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReduceRoot {
    /// Nothing: no items and no initial value, so the reduce fails.
    Empty,
    /// The initial value: no items to fold.
    Initial,
    /// The one item, as it is: seedless, with nothing to fold it into.
    Item(u64),
    /// The last step's result.
    Step(u64),
}

/// How a reduce of `items` is cut into steps. A pure function of the item
/// count and the shape, so the leader that starts the steps and the worker
/// that runs one agree on every step without storing the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducePlan {
    items: u64,
    seeded: bool,
    /// A tree's chunk size; `None` for a chain.
    chunk: Option<u64>,
    /// A tree's slots per level, from its chunks up to the one root slot.
    slots: Vec<u64>,
}

impl ReducePlan {
    pub fn new(items: u64, shape: &ReduceShape) -> Self {
        let seeded = shape.initial.is_some();
        let chunk = shape.tree.then(|| u64::from(shape.chunk_size.max(1)));
        let mut slots = Vec::new();
        if let Some(size) = chunk
            && items > u64::from(!seeded)
        {
            let mut width = items.div_ceil(size);
            slots.push(width);
            while width > 1 {
                width = width.div_ceil(2);
                slots.push(width);
            }
        }
        ReducePlan { items, seeded, chunk, slots }
    }

    /// How many steps the reduce runs; none when there is nothing to fold.
    pub fn steps(&self) -> u64 {
        if self.items <= u64::from(!self.seeded) {
            return 0;
        }
        match self.chunk {
            None => self.items - u64::from(!self.seeded),
            // Every combine joins two slots into one, so c chunks take c - 1.
            Some(_) => 2 * self.slots[0] - 1,
        }
    }

    pub fn root(&self) -> ReduceRoot {
        match (self.items, self.seeded) {
            (0, false) => ReduceRoot::Empty,
            (0, true) => ReduceRoot::Initial,
            (1, false) => ReduceRoot::Item(0),
            _ => ReduceRoot::Step(self.steps() - 1),
        }
    }

    /// Step `index`, or `None` past the last step.
    pub fn step(&self, index: u64) -> Option<PlannedStep> {
        if index >= self.steps() {
            return None;
        }
        Some(match self.chunk {
            None => self.chain_step(index),
            Some(size) if index < self.slots[0] => self.chunk_step(index, size),
            Some(_) => self.combine_step(index),
        })
    }

    /// The step that takes `index`'s result, or `None` for the last one.
    pub fn consumer(&self, index: u64) -> Option<u64> {
        if self.chunk.is_none() {
            return (index + 1 < self.steps()).then_some(index + 1);
        }
        let (mut level, mut position) = self.node_of(index);
        while (level as usize) + 1 < self.slots.len() {
            let parent = position / 2;
            if parent < self.steps_at(level + 1) {
                return Some(self.offset(level + 1) + parent);
            }
            // Unpaired: carried up to the next level as it is.
            level += 1;
            position = parent;
        }
        None
    }

    /// Every step not yet settled whose earlier results all are: what a
    /// leader starts, or makes sure exists, from scratch.
    pub fn startable(&self, settled: impl Fn(u64) -> bool) -> Vec<u64> {
        (0..self.steps())
            .filter(|&index| !settled(index))
            .filter(|&index| {
                self.step(index).is_some_and(|step| {
                    step.operands.iter().all(|operand| match operand {
                        Operand::Result(before) => settled(*before),
                        Operand::Item(_) | Operand::Initial => true,
                    })
                })
            })
            .collect()
    }

    fn chain_step(&self, index: u64) -> PlannedStep {
        let (item, first) = if self.seeded { (index, Operand::Initial) } else { (index + 1, Operand::Item(0)) };
        let start = if index == 0 { first } else { Operand::Result(index - 1) };
        PlannedStep {
            index,
            node: ReduceNode { level: 0, position: index },
            operands: vec![start, Operand::Item(item)],
        }
    }

    fn chunk_step(&self, index: u64, size: u64) -> PlannedStep {
        let first = index * size;
        let end = (first + size).min(self.items);
        let mut operands = Vec::with_capacity((end - first) as usize + 1);
        if self.seeded {
            operands.push(Operand::Initial);
        }
        operands.extend((first..end).map(Operand::Item));
        PlannedStep { index, node: ReduceNode { level: 0, position: index }, operands }
    }

    fn combine_step(&self, index: u64) -> PlannedStep {
        let (level, position) = self.node_of(index);
        let left = self.slot_step(level - 1, 2 * position);
        let right = self.slot_step(level - 1, 2 * position + 1);
        PlannedStep {
            index,
            node: ReduceNode { level, position },
            operands: vec![Operand::Result(left), Operand::Result(right)],
        }
    }

    /// The steps on `level`: every chunk on level 0, then one combine per
    /// pair of the slots below.
    fn steps_at(&self, level: u32) -> u64 {
        if level == 0 { self.slots[0] } else { self.slots[level as usize - 1] / 2 }
    }

    /// The number of the first step on `level`.
    fn offset(&self, level: u32) -> u64 {
        (0..level).map(|below| self.steps_at(below)).sum()
    }

    /// The level and position of tree step `index`.
    fn node_of(&self, index: u64) -> (u32, u64) {
        let (mut level, mut first) = (0, 0);
        while index >= first + self.steps_at(level) {
            first += self.steps_at(level);
            level += 1;
        }
        (level, index - first)
    }

    /// The step whose result fills slot `position` of `level`: its own
    /// combine, or the slot below it, carried up for want of a partner.
    fn slot_step(&self, level: u32, position: u64) -> u64 {
        if level == 0 {
            return position;
        }
        if position < self.steps_at(level) {
            self.offset(level) + position
        } else {
            self.slot_step(level - 1, 2 * position)
        }
    }
}
