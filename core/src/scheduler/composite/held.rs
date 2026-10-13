//! The composites a scheduler holds: what each is made of, how far it has
//! come, and when a finished one is forgotten.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use crate::manifest::{Failure, Manifest};
use crate::protocol::ids::{CompositeId, TaskId, WorkerId};
use crate::scheduler::SubmitRejection;
use crate::time::{Duration, Instant, WallTime};

use super::{
    BoundStep, CANCELLED_KIND, CompositeKind, CompositeParent, CompositeState, InputShape,
    MemberCounts, MemberOutcome, OnError, PAGE_ENTRIES, ReduceItem, ReducePlan, Stage, Step, StreamedMember,
};

/// One composite this scheduler drives, or keeps until its retention ends.
#[derive(Debug, Clone)]
pub(in crate::scheduler) struct Composite {
    pub(super) step: Step,
    pub(super) parent: Option<CompositeParent>,
    /// The root of the step tree this composite is part of, which owns the
    /// blobs of every large value bound in it: its own id unless its parent
    /// is a composite, then its parent's root.
    pub(super) root: CompositeId,
    /// The composite whose results name this one, which is kept as long as
    /// that one is: its parent composite, or, for the continuation of a task
    /// that is a member of a composite, that composite. A composite that is
    /// not named by anything's results is forgotten on its own.
    pub(super) retained_by: Option<CompositeId>,
    pub(super) client: Option<WorkerId>,
    /// What a nested composite takes, and how it decodes.
    pub(super) input: Option<(Manifest, InputShape)>,
    pub(super) state: CompositeState,
    pub(super) next_stage: u64,
    /// Every stage or member that has settled, by index.
    pub(super) outcomes: Outcomes,
    pub(super) failure: Option<Failure>,
    pub(super) submitted_at: WallTime,
    /// The encoded description's size, counted against the memory budget
    /// until the composite finishes.
    pub(super) bytes: u64,
    /// How many members it holds: its stages' count unless it is streamed,
    /// whose members arrive in batches.
    pub(super) members: u64,
    /// No batch adds a member: always true unless members arrive in batches.
    pub(super) sealed: bool,
    /// A reduce's items taken from its batches so far.
    pub(super) items_taken: u64,
    /// The items of a reduce's last item page while that page is partly
    /// filled: all the leader keeps of the items it took.
    pub(super) partial_items: Vec<ReduceItem>,
    /// Item pages that filled during this call, kept only until they are
    /// published.
    pub(super) filled_item_pages: BTreeMap<u32, Vec<ReduceItem>>,
}

/// The outcomes of the members that settled, by index, with the tallies a
/// 100k-member composite needs in constant time: every status, record and
/// page check would otherwise walk them all.
#[derive(Debug, Clone, Default)]
pub(in crate::scheduler) struct Outcomes {
    settled: BTreeMap<u64, MemberOutcome>,
    /// The indices below `members` with no outcome yet.
    open: BTreeSet<u64>,
    /// The unsettled members whose Task has been claimed: a superset of the
    /// ones whose run is claimed or started now, which the reader checks.
    claimed: BTreeSet<u64>,
    members: u64,
    succeeded: u64,
    failed: u64,
    cancelled: u64,
    /// How many outcomes each page holds.
    per_page: BTreeMap<u32, u64>,
}

impl Deref for Outcomes {
    type Target = BTreeMap<u64, MemberOutcome>;

    fn deref(&self) -> &Self::Target {
        &self.settled
    }
}

impl Outcomes {
    /// The outcomes `settled` names, for a composite of `members` members.
    pub(super) fn of(settled: BTreeMap<u64, MemberOutcome>, members: u64) -> Self {
        let mut outcomes = Outcomes::default();
        for (index, outcome) in settled {
            outcomes.insert(index, outcome);
        }
        outcomes.grow_to(members);
        outcomes
    }

    /// Notes that the composite now holds `members` members.
    pub(super) fn grow_to(&mut self, members: u64) {
        for index in self.members..members {
            if !self.settled.contains_key(&index) {
                self.open.insert(index);
            }
        }
        self.members = self.members.max(members);
    }

    /// Records `outcome` for member `index`, which has none yet.
    pub(super) fn insert(&mut self, index: u64, outcome: MemberOutcome) {
        debug_assert!(!self.settled.contains_key(&index), "member {index} already has an outcome");
        match &outcome {
            MemberOutcome::Value { .. } | MemberOutcome::Child(_) => self.succeeded += 1,
            MemberOutcome::Failed(failure) if failure.kind == CANCELLED_KIND => self.cancelled += 1,
            MemberOutcome::Failed(_) => self.failed += 1,
        }
        *self.per_page.entry((index / PAGE_ENTRIES) as u32).or_default() += 1;
        self.open.remove(&index);
        self.claimed.remove(&index);
        self.settled.insert(index, outcome);
    }

    /// The members that have no outcome yet, in order.
    pub(super) fn open(&self) -> impl Iterator<Item = u64> + '_ {
        self.open.iter().copied()
    }

    /// Notes that member `index`'s Task was claimed.
    pub(super) fn note_claimed(&mut self, index: u64) {
        if !self.settled.contains_key(&index) {
            self.claimed.insert(index);
        }
    }

    /// The unsettled members whose Task was claimed, in order.
    pub(super) fn claimed(&self) -> impl Iterator<Item = u64> + '_ {
        self.claimed.iter().copied()
    }

    /// How many outcomes page `page` holds.
    fn in_page(&self, page: u32) -> u64 {
        self.per_page.get(&page).copied().unwrap_or(0)
    }
}

impl Composite {
    /// Whether its members arrive in batches.
    pub(super) fn takes_batches(&self) -> bool {
        self.step.takes_batches()
    }

    /// How far its batches have filled it: a reduce's items, else its members.
    pub(super) fn batch_held(&self) -> u64 {
        if self.step.kind == CompositeKind::Reduce { self.items_taken } else { self.members }
    }

    /// The plan of a reduce whose item count is final; `None` for any other
    /// composite, and for a reduce still taking items.
    pub(super) fn reduce_plan(&self) -> Option<ReducePlan> {
        let shape = self.step.reduce.as_ref()?;
        self.sealed.then(|| ReducePlan::new(self.items_taken, shape))
    }

    /// The stage a streamed member is to run as: a map's item is the input of
    /// its template, a group's stage is run as it is unless it needs an input,
    /// which a streamed composite has none to give, or itself waits for
    /// members. An item has no fields set on it: it is the whole input.
    pub(super) fn member_stage(&self, member: StreamedMember) -> Result<Stage, SubmitRejection> {
        match (self.step.kind, member) {
            (CompositeKind::Map, StreamedMember::Item(input)) => match (&self.step.stages[..], input) {
                ([Stage::Task(template)], input) if template.fields.is_empty() => {
                    Ok(Stage::Task(BoundStep { input: input.into(), ..template.clone() }))
                }
                _ => Err(SubmitRejection::MemberKind),
            },
            (CompositeKind::Group, StreamedMember::Stage(stage))
                if !stage.takes_input()
                    && !stage.is_or_holds_streamed()
                    && !matches!(&stage, Stage::Task(task) if !task.fields.is_empty()) =>
            {
                Ok(stage)
            }
            _ => Err(SubmitRejection::MemberKind),
        }
    }

    /// Whether the first stage or member to fail for good ends the
    /// composite: a flow's always does, a group's or map's by its policy.
    pub(super) fn fails_on_first_failure(&self) -> bool {
        self.step.kind == CompositeKind::Flow || self.step.on_error == OnError::FailFast
    }

    /// The failure that ended the composite, or the cancellation that did.
    pub(super) fn failure_or_cancelled(&self) -> Failure {
        self.failure
            .clone()
            .unwrap_or_else(|| Failure::new(CANCELLED_KIND, ""))
    }

    /// The lowest-indexed failure, if a failure ends this composite.
    pub(super) fn first_failure_it_fails_on(&self) -> Option<Failure> {
        if !self.fails_on_first_failure() {
            return None;
        }
        self.outcomes.values().find_map(|outcome| match outcome {
            MemberOutcome::Failed(failure) => Some(failure.clone()),
            MemberOutcome::Value { .. } | MemberOutcome::Child(_) => None,
        })
    }

    /// Whether page `page` should go out now: the composite is over, or every
    /// index the page covers has settled. A page of a composite that may still
    /// grow is whole only when it is full.
    fn page_due(&self, page: u32) -> bool {
        if self.state != CompositeState::Running {
            return true;
        }
        let first = u64::from(page) * PAGE_ENTRIES;
        let end = first + PAGE_ENTRIES;
        let end = if self.sealed { end.min(self.members) } else { end };
        let width = end.saturating_sub(first);
        self.outcomes.in_page(page) >= width
    }

    /// How many pages the settled outcomes take.
    pub(in crate::scheduler) fn pages(&self) -> u32 {
        self.outcomes
            .last_key_value()
            .map_or(0, |(index, _)| (index / PAGE_ENTRIES) as u32 + 1)
    }

    /// The members by state, `running` of those not settled being claimed or
    /// started.
    pub(super) fn counts(&self, running: u64) -> MemberCounts {
        let mut counts = MemberCounts {
            running,
            succeeded: self.outcomes.succeeded,
            failed: self.outcomes.failed,
            cancelled: self.outcomes.cancelled,
            ..MemberCounts::default()
        };
        counts.pending = self
            .members
            .saturating_sub(self.outcomes.len() as u64)
            .saturating_sub(running);
        counts
    }
}

/// The composites a scheduler holds, and which of their records changed.
#[derive(Debug, Default)]
pub(in crate::scheduler) struct Composites {
    pub(in crate::scheduler) held: BTreeMap<CompositeId, Composite>,
    pub(in crate::scheduler) unpublished: BTreeSet<CompositeId>,
    pub(in crate::scheduler) unpublished_pages: BTreeSet<(CompositeId, u32)>,
    /// Pages that changed but are not whole yet, so are not written until
    /// they are or their composite is over.
    unwritten_pages: BTreeSet<(CompositeId, u32)>,
    /// The Tasks whose ends are being settled into their composites right
    /// now, outermost first: a composite that ends meanwhile says so.
    pub(in crate::scheduler) settling: Vec<TaskId>,
    /// When each finished composite finished, for its retention.
    finished_at: BTreeMap<CompositeId, Instant>,
    /// The same, by time, so only the due ones are visited.
    finished: BTreeSet<(Instant, CompositeId)>,
}

impl Composites {
    /// Forgets every composite it holds.
    pub(in crate::scheduler) fn clear(&mut self) {
        *self = Composites::default();
    }

    /// Marks the composite `id` and its pages as changed, so the next
    /// publication writes them again.
    pub(in crate::scheduler) fn mark_unpublished(&mut self, id: &CompositeId) {
        let Some(composite) = self.held.get(id) else {
            return;
        };
        let pages = composite.pages();
        self.unpublished.insert(id.clone());
        for page in 0..pages {
            self.page_changed(id, page);
        }
        for page in self.held[id].held_item_pages() {
            self.unpublished_pages.insert((id.clone(), page));
        }
    }

    /// Page `page` of the held composite `id` changed: it is written at the
    /// next publication if it is due, and kept back otherwise.
    pub(super) fn page_changed(&mut self, id: &CompositeId, page: u32) {
        let key = (id.clone(), page);
        if self.held[id].page_due(page) {
            self.unwritten_pages.remove(&key);
            self.unpublished_pages.insert(key);
        } else {
            self.unwritten_pages.insert(key);
        }
    }

    /// The composite `id` is over: every page is written now, those kept
    /// back for the first time and those already written while it ran, which
    /// were stamped unfinished and are rewritten as finished so a store can
    /// begin retiring them.
    pub(super) fn release_pages(&mut self, id: &CompositeId) {
        let Some(composite) = self.held.get(id) else {
            return;
        };
        let pages = composite.pages();
        for page in 0..pages {
            let key = (id.clone(), page);
            self.unwritten_pages.remove(&key);
            self.unpublished_pages.insert(key);
        }
    }

    pub(super) fn record_finished(&mut self, id: &CompositeId, now: Instant) {
        self.finished_at.insert(id.clone(), now);
        self.finished.insert((now, id.clone()));
    }

    pub(super) fn is_finished(&self, id: &CompositeId) -> bool {
        self.finished_at.contains_key(id)
    }

    /// Whether the composite that names `id`'s results is still held: it
    /// reads them for its own results and for the next stage's input until
    /// it is forgotten itself, so `id` is forgotten only after it.
    fn retainer_is_held(&self, id: &CompositeId) -> bool {
        self.held
            .get(id)
            .and_then(|composite| composite.retained_by.as_ref())
            .is_some_and(|retainer| self.held.contains_key(retainer))
    }

    /// When the first finished composite that may be forgotten is due, with
    /// composites kept `ttl` after they finish.
    pub(in crate::scheduler) fn next_due(&self, ttl: Duration) -> Option<Instant> {
        self.finished
            .iter()
            .find(|(_, id)| !self.retainer_is_held(id))
            .map(|(finished_at, _)| *finished_at + ttl)
    }

    /// Forgets every finished composite, with its pages, that finished at
    /// least `ttl` before `now` and whose parent composite is gone. Returns
    /// the Tasks that were members of them, which are no longer kept for them.
    pub(in crate::scheduler) fn forget_due(&mut self, ttl: Duration, now: Instant) -> Vec<TaskId> {
        let mut members = Vec::new();
        loop {
            let due: Vec<CompositeId> = self
                .finished
                .iter()
                .take_while(|(finished_at, _)| now - *finished_at >= ttl)
                .filter(|(_, id)| !self.retainer_is_held(id))
                .map(|(_, id)| id.clone())
                .collect();
            if due.is_empty() {
                return members;
            }
            for id in due {
                members.extend(self.forget(&id));
            }
        }
    }

    fn forget(&mut self, id: &CompositeId) -> Vec<TaskId> {
        let members = self.held.remove(id).map_or_else(Vec::new, |composite| {
            // A composite fed in batches keeps no stage once it has started
            // it: a child's index names no Task, which retention never holds.
            let fed = composite.step.takes_batches();
            let expansion = composite.step.expands().then(|| id.expansion_task());
            (0..composite.members)
                .filter(|index| fed || matches!(composite.step.stages[*index as usize], Stage::Task(_)))
                .map(|index| id.member_task(index))
                .chain(expansion)
                .collect()
        });
        self.unpublished.remove(id);
        self.unpublished_pages.retain(|(composite, _)| composite != id);
        self.unwritten_pages.retain(|(composite, _)| composite != id);
        if let Some(finished_at) = self.finished_at.remove(id) {
            self.finished.remove(&(finished_at, id.clone()));
        }
        members
    }
}
