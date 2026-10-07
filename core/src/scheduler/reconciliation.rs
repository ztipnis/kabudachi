//! What a rebuild left the scheduler: the office it reconciles for until
//! that office's grant, the workers whose answers the rebuild used, and the
//! tasks whose newest record it cannot know yet, with what is held back
//! until it can.

use std::collections::{BTreeMap, BTreeSet};

use crate::coalescing::{self, Key};
use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::{TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::Task;
use crate::protocol::messages::prelude::*;
use crate::protocol::records::TaskRunRecord;
use crate::reconcile::{CoalescingKey, ReconcileTerm, ReportedRun, ReportedState};
use crate::task_record::{VersionOrder, identify};

/// What a rebuild left the scheduler, kept consistent in one place. A task
/// is uncertain until its record is installed; a record held back with its
/// key is uncertain too; a worker's report is held only for an uncertain
/// task, and handed back when that task's record is installed.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub(super) struct Reconciliation {
    /// While its node reconciles: the office, whether the rebuild ran, and
    /// the workers reported lost meanwhile.
    office: Option<Office>,
    /// The workers whose answer the last rebuild used, kept after the
    /// reconciliation ends: a late record that names one of them as holding
    /// a run does not make it a silent holder.
    answered: BTreeSet<WorkerId>,
    /// Tasks some worker holds whose newest record cannot be known yet, with
    /// the run ids known to be theirs. Not scheduled, not republished; claims,
    /// cancels and reports of them are answered `NotReady`.
    uncertain: BTreeMap<TaskId, BTreeSet<TaskRunId>>,
    /// The coalescing key of each task in `uncertain` that has one. While any
    /// generation of a key is in here, no generation of that key is installed.
    uncertain_keys: BTreeMap<TaskId, Key>,
    /// Records known for certain that are held back with their key: a
    /// generation of the key has no record this leader can rely on yet.
    deferred: BTreeMap<TaskId, TaskRecord>,
    /// What workers reported about runs of uncertain tasks, applied when the
    /// task's record is installed.
    held_reports: BTreeMap<TaskId, Vec<(WorkerId, ReportedRun)>>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct Office {
    term: ReconcileTerm,
    rebuilt: bool,
    lost: BTreeSet<WorkerId>,
}

/// The coalescing keys of the tasks of `keys`.
fn key_map(keys: BTreeMap<TaskId, CoalescingKey>) -> BTreeMap<TaskId, Key> {
    keys.into_iter()
        .map(|(task_id, key)| (task_id, coalescing::key(&key.definition, &key.key)))
        .collect()
}

impl Reconciliation {
    pub(super) fn is_uncertain(&self, task: &TaskId) -> bool {
        self.uncertain.contains_key(task)
    }

    /// Whether `run` is one of an uncertain task's known runs.
    pub(super) fn holds_uncertain_run(&self, run: &TaskRunId) -> bool {
        self.uncertain.values().any(|runs| runs.contains(run))
    }

    pub(super) fn uncertain_count(&self) -> usize {
        self.uncertain.len()
    }

    /// Whether a generation of `key` is uncertain or held back, so that no
    /// generation of it is installed and occupancy cannot tell whether one
    /// is still running.
    pub(super) fn holds_key_back(&self, key: &Key) -> bool {
        self.uncertain_keys.values().any(|held| held == key)
            || self.deferred.values().any(|record| {
                record.task.as_ref().is_some_and(|task| {
                    task.coalescing_key.as_deref().is_some_and(|name| {
                        coalescing::key(&task.task_definition_id(), name) == *key
                    })
                })
            })
    }

    /// Every key with a generation whose record is not known.
    pub(super) fn unknown_keys(&self) -> BTreeSet<Key> {
        self.uncertain_keys.values().cloned().collect()
    }

    /// `tasks` were installed: none of them is uncertain any more.
    pub(super) fn installed(&mut self, tasks: &[TaskId]) {
        for task_id in tasks {
            self.uncertain.remove(task_id);
        }
    }

    /// `record` is known but held back with its key: its task is uncertain,
    /// with the record's runs, until the key is known. A record with no task
    /// is ignored.
    pub(super) fn hold_back(&mut self, record: TaskRecord) {
        let Some(task_id) = record.task.as_ref().map(Task::task_id) else {
            return;
        };
        let runs = record.runs.iter().map(TaskRunRecord::task_run_id);
        self.uncertain.entry(task_id.clone()).or_default().extend(runs);
        self.deferred.insert(task_id, record);
    }

    /// Takes late knowledge: returns the newest known record of every task
    /// `held` says the scheduler does not hold (records held back earlier
    /// compete with the new ones), marks their keys known, and makes the
    /// tasks of `uncertain` it does not hold uncertain.
    pub(super) fn learn(
        &mut self,
        records: Vec<TaskRecord>,
        uncertain: BTreeMap<TaskId, BTreeSet<TaskRunId>>,
        uncertain_keys: BTreeMap<TaskId, CoalescingKey>,
        held: impl Fn(&TaskId) -> bool,
    ) -> Vec<TaskRecord> {
        let mut candidates = std::mem::take(&mut self.deferred);
        for record in records {
            let Ok((task_id, version)) = identify(&record) else {
                continue;
            };
            if held(&task_id) {
                continue;
            }
            // Its record is known now, so it no longer holds its key back.
            self.uncertain_keys.remove(&task_id);
            let newer = candidates.get(&task_id).is_none_or(|known| {
                identify(known).is_ok_and(|(_, known)| known.order(&version) == VersionOrder::Older)
            });
            if newer {
                candidates.insert(task_id, record);
            }
        }
        let mut uncertain_keys = key_map(uncertain_keys);
        for (task_id, runs) in uncertain {
            if !held(&task_id) {
                if let Some(key) = uncertain_keys.remove(&task_id) {
                    self.uncertain_keys.insert(task_id.clone(), key);
                }
                self.uncertain.entry(task_id).or_default().extend(runs);
            }
        }
        candidates.into_values().collect()
    }

    /// Holds `reported` when its task is uncertain, replacing an earlier
    /// report of the same run; hands it back otherwise.
    pub(super) fn hold_report(
        &mut self,
        worker: &WorkerId,
        reported: ReportedRun,
    ) -> Option<ReportedRun> {
        let task_id = reported.claim.task.task_id();
        let run_id = reported.claim.task_run_id.clone();
        if !self.uncertain.contains_key(&task_id) {
            return Some(reported);
        }
        let held = self.held_reports.entry(task_id).or_default();
        held.retain(|(_, known)| known.claim.task_run_id != run_id);
        held.push((worker.clone(), reported));
        None
    }

    /// `worker` answered again with only `reported`: what it reported
    /// earlier and left out now is no longer held.
    pub(super) fn drop_unreported(&mut self, worker: &WorkerId, reported: &BTreeSet<TaskRunId>) {
        for held in self.held_reports.values_mut() {
            held.retain(|(holder, run)| holder != worker || reported.contains(&run.claim.task_run_id));
        }
        self.held_reports.retain(|_, held| !held.is_empty());
    }

    /// The reports held for `task`, which is now installed.
    pub(super) fn take_reports_for(&mut self, task: &TaskId) -> Vec<(WorkerId, ReportedRun)> {
        self.held_reports.remove(task).unwrap_or_default()
    }

    /// The runs of uncertain tasks `worker` reported as claimed or running.
    pub(super) fn active_runs_reported_by<'a>(
        &'a self,
        worker: &'a WorkerId,
    ) -> impl Iterator<Item = TaskRunId> + 'a {
        self.held_reports
            .values()
            .flatten()
            .filter(move |(reporter, run)| {
                reporter == worker
                    && matches!(run.state, ReportedState::Claimed | ReportedState::Running)
            })
            .map(|(_, run)| run.claim.task_run_id.clone())
    }

    /// A rebuild ran: replaces what earlier ones left.
    pub(super) fn replace_uncertain(
        &mut self,
        uncertain: BTreeMap<TaskId, BTreeSet<TaskRunId>>,
        uncertain_keys: BTreeMap<TaskId, CoalescingKey>,
    ) {
        self.uncertain = uncertain;
        self.uncertain_keys = key_map(uncertain_keys);
        self.deferred.clear();
        self.held_reports.clear();
    }
}
