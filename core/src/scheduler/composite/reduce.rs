//! A reduce: a composite that folds a list into one value with a reducer
//! task. These are its description and the shape of its item pages; the
//! leader reads them and never runs the reducer.

use crate::manifest::Manifest;
use crate::protocol::generated::{self, composite_entry, reduce_options};
use crate::protocol::ids::{CompositeId, IdGenerator};
use crate::scheduler::{Observer, Scheduler, SubmitRejection};
use crate::time::Clock;

use super::held::Composite;
use super::{BoundInput, BoundStep, INLINE_VALUE_BYTES, ItemInput, MalformedStep, PAGE_ENTRIES, StreamedMember};

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
    /// The plan for a reduce of `items` items; a tree's chunk size of 0 is read as 1.
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

    /// What the reduce gives when its steps are done.
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
        if index >= self.steps() {
            return None;
        }
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

/// The page of a reduce that holds its item `index`.
pub(super) fn item_page(index: u64) -> u32 {
    ITEM_PAGES + u32::try_from(index / PAGE_ENTRIES).expect("item pages stay below 2^31")
}

impl ReduceItem {
    /// What holding it costs the leader: its inline bytes, or its manifest's.
    fn held_bytes(&self) -> u64 {
        match self {
            ReduceItem::Value(bytes) => bytes.len() as u64,
            ReduceItem::Manifest(manifest) => manifest.encode().len() as u64,
        }
    }
}

impl Composite {
    /// The item pages the leader still has: those that filled during this
    /// call and are not yet published, and the partly filled last one.
    pub(super) fn held_item_pages(&self) -> Vec<u32> {
        let partial = (!self.partial_items.is_empty()).then(|| item_page(self.items_taken - 1));
        self.filled_item_pages.keys().copied().chain(partial).collect()
    }

    /// The entries of item page `page`, if the leader still has that page.
    pub(in crate::scheduler) fn item_page_entries(&self, page: u32) -> Option<Vec<generated::CompositeEntry>> {
        if page < ITEM_PAGES {
            return None;
        }
        let items = match self.filled_item_pages.get(&page) {
            Some(items) => items,
            None if self.held_item_pages().contains(&page) => &self.partial_items,
            None => return None,
        };
        let first = u64::from(page - ITEM_PAGES) * PAGE_ENTRIES;
        Some(
            (first..)
                .zip(items)
                .map(|(index, item)| generated::CompositeEntry {
                    index,
                    outcome: Some(composite_entry::Outcome::from(item)),
                    producer: None,
                })
                .collect(),
        )
    }
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Takes a batch's new items into a reduce, whole or not at all, and
    /// marks every item page they touch for publication; the last batch seals
    /// the reduce. An item is at most `INLINE_VALUE_BYTES` inline or blobs the
    /// reduce owns, so a page of `PAGE_ENTRIES` entries fits one record.
    pub(super) fn take_reduce_items(
        &mut self,
        id: &CompositeId,
        fresh: Vec<(u64, StreamedMember)>,
        last: bool,
    ) -> Result<(), SubmitRejection> {
        let items: Vec<ReduceItem> = fresh
            .into_iter()
            .map(|(_, member)| match member {
                StreamedMember::Item(ItemInput::Value(bytes)) if bytes.len() <= INLINE_VALUE_BYTES => {
                    Ok(ReduceItem::Value(bytes))
                }
                StreamedMember::Item(ItemInput::Chunked(manifest)) => Ok(ReduceItem::Manifest(manifest)),
                // A longer inline item would not fit an item page: its sender spills it.
                StreamedMember::Item(_) | StreamedMember::Stage(_) => Err(SubmitRejection::MemberKind),
            })
            .collect::<Result<_, _>>()?;
        let needed = items.iter().map(ReduceItem::held_bytes).sum();
        self.budget.check_room(needed, || 0)?;
        self.budget.take(needed);
        let composite = self.composites.held.get_mut(id).expect("admitted above");
        composite.bytes += needed;
        for item in items {
            let page = item_page(composite.items_taken);
            composite.partial_items.push(item);
            composite.items_taken += 1;
            self.composites.unpublished_pages.insert((id.clone(), page));
            if composite.partial_items.len() as u64 == PAGE_ENTRIES {
                let full = std::mem::take(&mut composite.partial_items);
                composite.filled_item_pages.insert(page, full);
            }
        }
        self.composites.unpublished.insert(id.clone());
        if last {
            self.seal_reduce(id);
        }
        Ok(())
    }

    /// Lets the full item page `page` of `id` go once it is published,
    /// giving its bytes back to the budget unless the composite's whole
    /// weight already went back when it finished.
    pub(in crate::scheduler) fn release_item_page(&mut self, id: &CompositeId, page: u32) {
        let finished = self.composites.is_finished(id);
        let Some(composite) = self.composites.held.get_mut(id) else {
            return;
        };
        if let Some(items) = composite.filled_item_pages.remove(&page) {
            let released: u64 = items.iter().map(ReduceItem::held_bytes).sum();
            composite.bytes -= released;
            if !finished {
                self.budget.give_back(released);
            }
        }
    }

    /// A reduce's item count is final: it now has a plan, and its members are
    /// that plan's steps.
    pub(super) fn seal_reduce(&mut self, id: &CompositeId) {
        let composite = self.composites.held.get_mut(id).expect("a held reduce");
        composite.sealed = true;
        composite.members = composite.reduce_plan().expect("a sealed reduce has a plan").steps();
        composite.outcomes.grow_to(composite.members);
        self.composites.unpublished.insert(id.clone());
    }
}
