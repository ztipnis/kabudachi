//! How a new leader brings the composites it rebuilt up to what their
//! members' records say, and drives them on from there.
//!
//! A composite is driven only from what its own record, its pages and its
//! members' records say, re-evaluated from scratch: the writes of one call
//! reach their holders in any order, so no field of one record is trusted to
//! agree with another.

use std::collections::BTreeMap;

use crate::manifest::Failure;
use crate::protocol::ids::{CompositeId, IdGenerator, TaskDefinitionId, TaskId, TaskRunId};
use crate::protocol::messages::prelude::*;
use crate::protocol::records::TaskRunRecord as _;
use crate::protocol::task::TaskRunState;
use crate::time::Clock;

use crate::scheduler::{Observer, Scheduler};

use super::{CompositeKind, CompositeState, EXPANSION_DEFINITION, LOST_KIND, MemberOutcome, Stage};

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Brings every composite rebuilt from records up to what its members'
    /// records say, and drives on from there. Derives the composite of a
    /// continuing task that has none, from the step its certified run
    /// carries.
    pub(in crate::scheduler) fn recover_composites(&mut self) {
        self.resolve_retainers();
        self.derive_continuations();
        self.count_streamed_members();
        self.note_claimed_members();
        let ids: Vec<CompositeId> = self.composites.held.keys().cloned().collect();
        for id in ids {
            if !self.holds_composite_back(&id) {
                self.reevaluate(&id);
            }
        }
        // A continuation that ended without its task hearing of it.
        let ended: Vec<TaskId> = self
            .continuing
            .iter()
            .filter(|task| {
                self.current_run
                    .get(*task)
                    .and_then(|run| self.composites.held.get(&CompositeId::continuation_of(run)))
                    .is_some_and(|continuation| continuation.state != CompositeState::Running)
            })
            .cloned()
            .collect();
        for task in ended {
            self.finish_continuation(&task);
        }
    }

    /// Notes every member Task a worker holds, for the composite that counts
    /// the running ones.
    fn note_claimed_members(&mut self) {
        let held: Vec<TaskId> = self
            .tasks
            .keys()
            .filter(|task| {
                self.current_run.get(*task).is_some_and(|run| {
                    matches!(self.runs[run].current_state(), TaskRunState::Claimed | TaskRunState::Running)
                })
            })
            .cloned()
            .collect();
        for task in held {
            self.note_member_claimed(&task);
        }
    }

    /// Raises the member count of every streamed composite to one past the
    /// highest index any of its member Tasks or settled outcomes names. Its
    /// record may lag what its old leader stored, so the larger count stands;
    /// whether it is sealed only its record says, so a successor never seals
    /// a composite on its own. An expansion run carries the map's id but is
    /// not one of its members.
    fn count_streamed_members(&mut self) {
        let expansion = TaskDefinitionId::new(EXPANSION_DEFINITION);
        let mut counts: BTreeMap<CompositeId, u64> = BTreeMap::new();
        for task in self.tasks.values() {
            let Some(id) = task.composite_id() else {
                continue;
            };
            if task.task_definition_id() != expansion && self.composites.held.get(&id).is_some_and(|c| c.takes_batches()) {
                let held = counts.entry(id).or_default();
                *held = (*held).max(task.composite_index + 1);
            }
        }
        for (id, composite) in &mut self.composites.held {
            if !composite.takes_batches() {
                continue;
            }
            let named = counts.get(id).copied().unwrap_or(0);
            let settled = composite.outcomes.last_key_value().map_or(0, |(index, _)| index + 1);
            let counted = composite.members.max(named).max(settled);
            if counted > composite.members {
                composite.members = counted;
                composite.outcomes.grow_to(counted);
                self.composites.unpublished.insert(id.clone());
            }
        }
    }

    /// Names, for each continuation composite held before its task was, the
    /// composite that keeps it.
    fn resolve_retainers(&mut self) {
        let unresolved: Vec<CompositeId> = self
            .composites
            .held
            .iter()
            .filter(|(_, composite)| composite.retained_by.is_none())
            .map(|(id, _)| id.clone())
            .collect();
        for id in unresolved {
            let retainer = self.retainer_of(self.composites.held[&id].parent.as_ref());
            self.composites.held.get_mut(&id).expect("held above").retained_by = retainer;
        }
    }

    /// Makes the composite of every continuing task that has no record: the
    /// step was committed by the revision that certified its run, so a lost
    /// composite record, a lost worker, or both, change nothing. One whose
    /// record is merely not known yet is waited for.
    fn derive_continuations(&mut self) {
        let underived: Vec<(TaskId, TaskRunId)> = self
            .continuing
            .iter()
            .filter_map(|task| Some((task.clone(), self.current_run.get(task)?.clone())))
            .filter(|(_, run)| {
                let id = CompositeId::continuation_of(run);
                !self.composites.held.contains_key(&id) && !self.reconciliation.is_uncertain_composite(&id)
            })
            .collect();
        for (task, run) in underived {
            self.commit_continuation(&task, &run);
        }
    }

    /// Whether `id` must wait: the record of a member Task, child composite
    /// or continuation it has started is not known for certain yet.
    pub(super) fn holds_composite_back(&self, id: &CompositeId) -> bool {
        let composite = &self.composites.held[id];
        let uncertain = &self.reconciliation;
        uncertain.is_uncertain_composite(id)
            || (composite.step.expands() && uncertain.is_uncertain(&id.expansion_task()))
            || (0..composite.members).any(|index| {
                let task = id.member_task(index);
                uncertain.is_uncertain(&task)
                    || uncertain.is_uncertain_composite(&id.child(index))
                    || self
                        .current_run
                        .get(&task)
                        .is_some_and(|run| uncertain.is_uncertain_composite(&CompositeId::continuation_of(run)))
            })
            || composite
                .outcomes
                .values()
                .any(|outcome| matches!(outcome, MemberOutcome::Child(child) if uncertain.is_uncertain_composite(child)))
    }

    /// Settles every member whose own record says it ended, then moves the
    /// composite on as `advance` would have: a running composite with a
    /// failure its policy fails on fails, one with every stage settled
    /// succeeds, and otherwise each stage it should have started is made
    /// unless it exists.
    fn reevaluate(&mut self, id: &CompositeId) {
        if self.composites.held[id].state != CompositeState::Running {
            return;
        }
        let stages = self.composites.held[id].members;
        for index in 0..stages {
            self.settle_from_record(id, index);
        }
        self.settle_members_nothing_names(id);
        if self.composites.held[id].step.expands() && !self.composites.held[id].sealed {
            self.resume_expansion(id);
            if self.composites.held[id].state != CompositeState::Running {
                return;
            }
        }
        let composite = &self.composites.held[id];
        if let Some(failure) = composite.first_failure_it_fails_on() {
            self.finish_composite(id, CompositeState::Failed, Some(failure));
            return;
        }
        let open: Vec<u64> = (0..stages).filter(|index| !composite.outcomes.contains_key(index)).collect();
        match (composite.step.kind, open.first()) {
            (_, None) if composite.sealed => self.finish_composite(id, CompositeState::Succeeded, None),
            (CompositeKind::Flow, Some(&first)) => {
                self.composites.held.get_mut(id).expect("held above").next_stage = first;
                self.ensure_stage(id, first);
            }
            _ => {
                for index in open {
                    self.ensure_stage(id, index);
                }
            }
        }
    }

    /// Settles member `index` of `id` from what the scheduler holds of it:
    /// its Task's record if the task is over, or the child composite if that
    /// is. Records the outcome and moves nothing on: `reevaluate` decides
    /// after.
    fn settle_from_record(&mut self, id: &CompositeId, index: u64) {
        let composite = &self.composites.held[id];
        if composite.outcomes.contains_key(&index) {
            return;
        }
        // A streamed composite keeps no stage once it has started it: a member
        // is a child composite only if there is one.
        let is_task = match composite.step.stages.get(index as usize) {
            Some(Stage::Task(_)) => true,
            Some(Stage::Composite(_)) => false,
            None => !self.composites.held.contains_key(&id.child(index)),
        };
        let outcome = if is_task {
            let task = id.member_task(index);
            if !self.retention.holds(&task) {
                return;
            }
            self.member_outcome_of(&task)
        } else {
            let child = id.child(index);
            self.composites.held.get(&child).and_then(|child_composite| match child_composite.state {
                CompositeState::Running => None,
                CompositeState::Succeeded => Some(MemberOutcome::Child(child.clone())),
                CompositeState::Failed | CompositeState::Cancelled => {
                    Some(MemberOutcome::Failed(child_composite.failure_or_cancelled()))
                }
            })
        };
        if let Some(outcome) = outcome {
            self.record_member(id, index, outcome);
        }
    }

    /// Settles as lost every member of a sealed composite fed in batches that
    /// has no outcome and no Task or child composite: the batch that made it
    /// reached the holders in part, and with the composite sealed nothing
    /// sends it again, so the composite would otherwise wait for it forever.
    /// Members whose records are merely unknown were waited for before this.
    fn settle_members_nothing_names(&mut self, id: &CompositeId) {
        let composite = &self.composites.held[id];
        if !composite.takes_batches() || !composite.sealed {
            return;
        }
        let nameless: Vec<u64> = composite
            .outcomes
            .open()
            .filter(|index| {
                !self.tasks.contains_key(&id.member_task(*index)) && !self.composites.held.contains_key(&id.child(*index))
            })
            .collect();
        for index in nameless {
            let failure = Failure::new(LOST_KIND, "the record of the member was not found");
            self.record_member(id, index, MemberOutcome::Failed(failure));
        }
    }

    /// Starts stage or member `index` of `id` unless its Task or child
    /// composite exists. A composite fed in batches starts nothing: its
    /// submitter or its expansion run sends the members again if they are
    /// missing.
    fn ensure_stage(&mut self, id: &CompositeId, index: u64) {
        if !self.composites.held[id].step.takes_batches() {
            self.start_stage(id, index);
        }
    }

    /// Takes up the expansion run of the expanding map `id` from its Task's
    /// record: a run that ended seals the map, one that failed for good fails
    /// it, and a map with no run yet gets one.
    fn resume_expansion(&mut self, id: &CompositeId) {
        let task = id.expansion_task();
        if !self.tasks.contains_key(&task) {
            self.submit_expansion(id);
        } else if self.retention.holds(&task) {
            if let Some(outcome) = self.member_outcome_of(&task) {
                self.expansion_finished(id, outcome);
            }
        }
    }
}
