//! The composites a scheduler holds: what each is made of, how far it has
//! come, and when a finished one is forgotten.

use std::collections::{BTreeMap, BTreeSet};

use crate::manifest::{Failure, Manifest};
use crate::protocol::ids::{CompositeId, WorkerId};
use crate::time::{Duration, Instant, WallTime};

use super::{
    CANCELLED_KIND, CompositeKind, CompositeParent, CompositeState, InputShape, MemberCounts,
    MemberOutcome, OnError, PAGE_ENTRIES, Step,
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
    pub(super) outcomes: BTreeMap<u64, MemberOutcome>,
    pub(super) failure: Option<Failure>,
    pub(super) submitted_at: WallTime,
    /// The encoded description's size, counted against the memory budget
    /// until the composite finishes.
    pub(super) bytes: u64,
}

impl Composite {
    /// Whether the first stage or member to fail for good ends the
    /// composite: a flow's always does, a group's or map's by its policy, and
    /// an expanding map's always, since its one member is the run that
    /// splits its list and without the items it has no results.
    pub(super) fn fails_on_first_failure(&self) -> bool {
        self.step.kind == CompositeKind::Flow
            || self.step.on_error == OnError::FailFast
            || self.step.expands()
    }

    /// The failure that ended the composite, or the cancellation that did.
    pub(super) fn failure_or_cancelled(&self) -> Failure {
        self.failure
            .clone()
            .unwrap_or_else(|| Failure { kind: CANCELLED_KIND.to_owned(), message: String::new() })
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

    /// How many pages the settled outcomes take.
    pub(in crate::scheduler) fn pages(&self) -> u32 {
        self.outcomes
            .last_key_value()
            .map_or(0, |(index, _)| (index / PAGE_ENTRIES) as u32 + 1)
    }

    pub(super) fn counts(&self) -> MemberCounts {
        let mut counts = MemberCounts::default();
        for outcome in self.outcomes.values() {
            match outcome {
                MemberOutcome::Value { .. } | MemberOutcome::Child(_) => counts.succeeded += 1,
                MemberOutcome::Failed(failure) if failure.kind == CANCELLED_KIND => counts.cancelled += 1,
                MemberOutcome::Failed(_) => counts.failed += 1,
            }
        }
        counts.pending = (self.step.stages.len() as u64).saturating_sub(self.outcomes.len() as u64);
        counts
    }
}

/// The composites a scheduler holds, and which of their records changed.
#[derive(Debug, Default)]
pub(in crate::scheduler) struct Composites {
    pub(in crate::scheduler) held: BTreeMap<CompositeId, Composite>,
    pub(in crate::scheduler) unpublished: BTreeSet<CompositeId>,
    pub(in crate::scheduler) unpublished_pages: BTreeSet<(CompositeId, u32)>,
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

    /// Marks every composite and page held as changed, so the next
    /// publication writes them all again.
    pub(in crate::scheduler) fn mark_all_unpublished(&mut self) {
        for (id, composite) in &self.held {
            self.unpublished.insert(id.clone());
            for page in 0..composite.pages() {
                self.unpublished_pages.insert((id.clone(), page));
            }
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
    /// least `ttl` before `now` and whose parent composite is gone.
    pub(in crate::scheduler) fn forget_due(&mut self, ttl: Duration, now: Instant) {
        loop {
            let due: Vec<CompositeId> = self
                .finished
                .iter()
                .take_while(|(finished_at, _)| now - *finished_at >= ttl)
                .filter(|(_, id)| !self.retainer_is_held(id))
                .map(|(_, id)| id.clone())
                .collect();
            if due.is_empty() {
                return;
            }
            for id in due {
                self.forget(&id);
            }
        }
    }

    fn forget(&mut self, id: &CompositeId) {
        self.held.remove(id);
        self.unpublished.remove(id);
        self.unpublished_pages.retain(|(composite, _)| composite != id);
        if let Some(finished_at) = self.finished_at.remove(id) {
            self.finished.remove(&(finished_at, id.clone()));
        }
    }
}
