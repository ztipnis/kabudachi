//! A reduce: a composite that folds a list into one value with a reducer
//! task. These are its description and the shape of its item pages; the
//! leader reads them and never runs the reducer.

use crate::blob::BlobKey;
use crate::manifest::{Failure, Manifest, ManifestEntry};
use crate::protocol::generated::{self, composite_entry, reduce_options};
use crate::protocol::ids::{CompositeId, IdGenerator};
use crate::scheduler::{Observer, Scheduler, SubmitRejection};
use crate::scheduler::Submitted;
use crate::task_record::{RecordKey, RecordVersion, VersionOrder};
use crate::time::{Clock, WallTime};

use super::held::Composite;
use super::{
    BoundInput, BoundStep, CompositeKind, CompositeState, INLINE_VALUE_BYTES, ItemInput, MalformedStep, MemberOutcome, PAGE_ENTRIES,
    SpilledValue, StreamedMember, spill_value,
};

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

impl ReduceShape {
    /// The shape with a long inline initial value moved to `values`, after
    /// the stages of the step that holds it.
    pub(super) fn spill(self, values: &mut Vec<SpilledValue>) -> ReduceShape {
        match self.initial {
            Some(BoundInput::Value(bytes)) if bytes.len() > INLINE_VALUE_BYTES => {
                ReduceShape { initial: Some(spill_value(bytes, values)), ..self }
            }
            _ => self,
        }
    }
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

/// The failure kind of a reduce one of whose steps returned a continuation
/// instead of a value: a reducer is a function of its operands, and a step
/// still running has no value to fold.
pub const REDUCER_CONTINUED_KIND: &str = "ReducerContinuedError";

/// The failure kind of a reduce whose stored items fell short of the count
/// its record names when its items ended: folding the fewer would give a
/// wrong result.
pub const REDUCE_ITEMS_LOST_KIND: &str = "ReduceItemsLostError";

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
    pub(super) fn held_bytes(&self) -> u64 {
        match self {
            ReduceItem::Value(bytes) => bytes.len() as u64,
            ReduceItem::Manifest(manifest) => manifest.encoded_len() as u64,
        }
    }
}

impl Composite {
    /// The item pages the leader still has: the full ones whose write is not
    /// stored yet, and the partly filled last one.
    pub(super) fn held_item_pages(&self) -> Vec<u32> {
        let partial = (!self.partial_items.is_empty()).then(|| item_page(self.items_taken - 1));
        self.filled_item_pages.keys().copied().chain(partial).collect()
    }

    /// How many item pages the reduce's items fill, none for a reduce that
    /// folds another composite's results.
    pub(super) fn item_page_count(&self) -> u32 {
        if self.step.kind == CompositeKind::Reduce && self.takes_batches() {
            u32::try_from(self.items_taken.div_ceil(PAGE_ENTRIES)).expect("item pages stay below 2^31")
        } else {
            0
        }
    }

    /// Whether `page` is one of the item pages the reduce's items fill.
    pub(in crate::scheduler) fn is_item_page_of_items(&self, page: u32) -> bool {
        page >= ITEM_PAGES && page - ITEM_PAGES < self.item_page_count()
    }

    /// Whether the leader still has item page `page`.
    pub(in crate::scheduler) fn holds_item_page(&self, page: u32) -> bool {
        page >= ITEM_PAGES && (self.filled_item_pages.contains_key(&page) || self.is_partial_page(page))
    }

    fn is_partial_page(&self, page: u32) -> bool {
        !self.partial_items.is_empty() && item_page(self.items_taken - 1) == page
    }

    /// The entries of item page `page`, if the leader still has that page.
    pub(in crate::scheduler) fn item_page_entries(&self, page: u32) -> Option<Vec<generated::CompositeEntry>> {
        if page < ITEM_PAGES {
            return None;
        }
        let items = match self.filled_item_pages.get(&page) {
            Some(items) => items,
            None if self.is_partial_page(page) => &self.partial_items,
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
        debug_assert!(!self.composites.held[id].sealed, "admission refuses every new item of a sealed reduce");
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

    /// Lets the full item page `page` of `id` go, giving its bytes back to
    /// the budget unless the composite's whole weight already went back when
    /// it finished. Says whether it held the page.
    pub(in crate::scheduler) fn release_item_page(&mut self, id: &CompositeId, page: u32) -> bool {
        let finished = self.composites.is_finished(id);
        let Some(composite) = self.composites.held.get_mut(id) else {
            return false;
        };
        composite.published_item_pages.remove(&page);
        let Some(items) = composite.filled_item_pages.remove(&page) else {
            return false;
        };
        let released: u64 = items.iter().map(ReduceItem::held_bytes).sum();
        composite.bytes -= released;
        if !finished {
            self.budget.give_back(released);
        }
        true
    }

    /// Notes `version` as the newest revision published of item page `page`
    /// of `id`, if the page is full: only a stored write of it or a newer one
    /// lets the leader go of the page.
    pub(in crate::scheduler) fn item_page_published(&mut self, id: &CompositeId, page: u32, version: RecordVersion) {
        if let Some(composite) = self.composites.held.get_mut(id)
            && composite.filled_item_pages.contains_key(&page)
        {
            composite.published_item_pages.insert(page, version);
        }
    }

    /// The write of `version` of `key` was stored where it belongs: a full
    /// item page published at that version or an older one is let go, since
    /// no write is left to retry or to move.
    pub fn record_stored(&mut self, key: &RecordKey, version: RecordVersion) {
        let RecordKey::Page(id, page) = key else {
            return;
        };
        let published = self
            .composites
            .held
            .get(id)
            .and_then(|composite| composite.published_item_pages.get(page));
        if published.is_some_and(|published| published.order(&version) != VersionOrder::Older)
            && self.release_item_page(id, *page)
        {
            self.update_pressure();
        }
    }

    /// A reduce's item count is final: it now has a plan, and its members are
    /// that plan's steps.
    pub(super) fn seal_reduce(&mut self, id: &CompositeId) {
        let composite = self.composites.held.get_mut(id).expect("a held reduce");
        if composite.items_taken < composite.items_floor {
            let message = format!(
                "the reduce held {} of the {} items its record counted",
                composite.items_taken, composite.items_floor
            );
            self.finish_composite(id, CompositeState::Failed, Some(Failure::new(REDUCE_ITEMS_LOST_KIND, message)));
            return;
        }
        composite.sealed = true;
        composite.members = composite.reduce_plan().expect("a sealed reduce has a plan").steps();
        composite.outcomes.grow_to(composite.members);
        self.composites.unpublished.insert(id.clone());
        // As a successor drives it: settle the steps that already ran (a
        // lost record may leave some), then start the rest.
        self.reevaluate(id);
    }

    /// Starts a sealed reduce: its first steps, or, with nothing to fold, its
    /// end at once (its initial value or its one item is its result).
    pub(super) fn start_reduce(&mut self, id: &CompositeId) {
        let plan = self.composites.held[id].reduce_plan().expect("only a sealed reduce starts");
        match plan.root() {
            ReduceRoot::Empty => {
                let failure = Failure::new(REDUCE_EMPTY_KIND, "a reduce of no items needs an initial value");
                self.finish_composite(id, CompositeState::Failed, Some(failure));
            }
            ReduceRoot::Initial | ReduceRoot::Item(_) => {
                self.finish_composite(id, CompositeState::Succeeded, None);
            }
            ReduceRoot::Step(_) => {
                for index in plan.startable(|_| false) {
                    self.submit_reduce_step(id, index);
                }
            }
        }
    }

    /// Records step `index`'s Task, unless it exists already (a takeover
    /// driving the reduce again) or the reduce is over. Its input manifest
    /// names the results it takes, in order; its items and initial value stay
    /// in the reduce's pages and description.
    pub(super) fn submit_reduce_step(&mut self, id: &CompositeId, index: u64) {
        let task_id = id.member_task(index);
        let composite = &self.composites.held[id];
        if composite.state != CompositeState::Running || self.tasks.contains_key(&task_id) {
            return;
        }
        let plan = composite.reduce_plan().expect("a reduce step belongs to a planned reduce");
        let step = plan.step(index).expect("only a planned step is started");
        let mut results: Vec<ManifestEntry> = Vec::new();
        for operand in &step.operands {
            let Operand::Result(before) = operand else {
                continue;
            };
            match composite.outcomes.get(before) {
                Some(MemberOutcome::Value { digest, .. }) => results.push(ManifestEntry::Blob(BlobKey::from(digest))),
                // A step starts only once the results it takes have settled, and one
                // that settled with a failure ended the reduce; what is left is a
                // step that returned a continuation, which has no value to fold.
                _ => {
                    self.fail_continued_reducer(id, *before);
                    return;
                }
            }
        }
        let reducer = &composite.step.reduce.as_ref().expect("a planned reduce has a shape").reducer;
        let input = (!results.is_empty()).then(|| Manifest::new(results, 0));
        let submission = reducer.submission(input).as_member_of(id.clone(), index).with_reduce_node(step.node);
        let submitted = Submitted::received(task_id, WallTime::now(&self.clock), submission, &self.clock);
        if let Err(refused) = self.record_submission(submitted) {
            let failure = Failure::new(refused.kind(), refused.to_string());
            self.settle_member(id, index, MemberOutcome::Failed(failure));
        }
    }

    /// Fails the reduce `id` because step `index` returned a continuation: a
    /// reducer must return its value, and the step that would take that
    /// value as an operand has none to take.
    fn fail_continued_reducer(&mut self, id: &CompositeId, index: u64) {
        let message = format!("reduce step {index} returned a continuation instead of a value");
        self.finish_composite(id, CompositeState::Failed, Some(Failure::new(REDUCER_CONTINUED_KIND, message)));
    }

    /// Step `index` settled: a failure fails the reduce; the root's value
    /// finishes it; any other value starts the step that takes it, once that
    /// step's other operand has settled too.
    pub(super) fn advance_reduce(&mut self, id: &CompositeId, index: u64, outcome: MemberOutcome) {
        match outcome {
            MemberOutcome::Failed(failure) => {
                self.finish_composite(id, CompositeState::Failed, Some(failure));
                return;
            }
            MemberOutcome::Child(_) => {
                self.fail_continued_reducer(id, index);
                return;
            }
            MemberOutcome::Value { .. } => {}
        }
        let composite = &self.composites.held[id];
        let plan = composite.reduce_plan().expect("only a planned reduce advances here");
        if plan.root() == ReduceRoot::Step(index) {
            self.finish_composite(id, CompositeState::Succeeded, None);
            return;
        }
        let next = plan.consumer(index).filter(|&next| {
            let step = plan.step(next).expect("a consumer is a planned step");
            step.operands.iter().all(|operand| match operand {
                Operand::Result(before) => composite.outcomes.contains_key(before),
                Operand::Item(_) | Operand::Initial => true,
            })
        });
        if let Some(next) = next {
            self.submit_reduce_step(id, next);
        }
    }
}
