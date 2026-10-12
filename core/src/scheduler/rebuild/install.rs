//! Installing a record: the task, its runs, where it waits, its memory, and its
//! coalescing key's occupancy.

use std::collections::{BTreeMap, BTreeSet};

use crate::coalescing::{ChainItem, Folded, Key};
use crate::protocol::digest::Digest;
use crate::protocol::generated::{ChainEntry, CoalescingLink, TaskRecord, chain_entry};
use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{Task, TaskRun};
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::reconcile::RECONCILE_SKEW_MARGIN;
use crate::time::{Clock, Duration, Instant, WallTime};

use super::super::{Observer, Scheduler};

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Holds the task `record` describes, as its newest revision says.
    pub(super) fn install(&mut self, record: TaskRecord, now: Instant, wall_now: WallTime) {
        let TaskRecord {
            task,
            runs,
            input_digest,
            link,
            finished,
            retained_chain,
            ..
        } = record;
        let (Some(task), Some(current)) = (task, runs.last().map(TaskRunRecord::task_run_id))
        else {
            return;
        };
        let task_id = task.task_id();
        let state = runs[runs.len() - 1].current_state();
        if matches!(state, TaskRunState::Scheduled | TaskRunState::Queued) {
            let (not_before, expires_at) =
                installed_wait_bounds(&task, state, runs.len() == 1, now, wall_now);
            self.waiting.admit(&task_id, not_before, expires_at);
        }
        if finished {
            self.retention.record(&task_id, now);
        } else {
            self.count_unfinished(&task_id, &task, &retained_chain, state);
        }
        let lost = runs
            .iter()
            .filter(|run| run.current_state() == TaskRunState::Lost)
            .count();
        if lost > 0 {
            self.losses.insert(task_id.clone(), lost as u32);
        }
        let digest = input_digest
            .as_ref()
            .and_then(|digest| Digest::try_from(digest).ok())
            .unwrap_or_else(|| Digest::blake3(&task.serialized_input));
        self.hold_installed(task, runs, current, digest, link);
    }

    /// Counts an unfinished installed task's input, and the folded payloads its
    /// chain carries, against memory, and a certified one as continuing.
    fn count_unfinished(
        &mut self,
        task_id: &TaskId,
        task: &Task,
        retained_chain: &[ChainEntry],
        state: TaskRunState,
    ) {
        self.budget.take(task.serialized_input.len() as u64);
        self.budget.take(
            chain_items(retained_chain)
                .iter()
                .map(|item| match item {
                    ChainItem::Folded(folded) => folded.payload.len() as u64,
                    ChainItem::Absorbed(_) => 0,
                })
                .sum(),
        );
        if state == TaskRunState::Succeeded {
            self.continuing.insert(task_id.clone());
        }
    }

    /// Holds `task` with `runs`, `current` the authoritative one. Installing a
    /// record decides nothing: this leader did not make it.
    fn hold_installed(
        &mut self,
        task: Task,
        runs: Vec<TaskRun>,
        current: TaskRunId,
        digest: Digest,
        link: Option<CoalescingLink>,
    ) {
        let task_id = task.task_id();
        self.input_digests.insert(task_id.clone(), digest);
        if let Some(link) = link {
            self.links.insert(task_id.clone(), link);
        }
        self.runs_of_task.insert(
            task_id.clone(),
            runs.iter().map(TaskRunRecord::task_run_id).collect(),
        );
        for run in runs {
            self.runs.insert(run.task_run_id(), run);
        }
        self.current_run.insert(task_id.clone(), current);
        self.tasks.insert(task_id.clone(), task);
        self.mark_current_decided(&task_id);
        for run_id in self.runs_of_task[&task_id].clone() {
            self.run_decided_at.remove(&run_id);
        }
    }

    /// Sets what each coalescing key holds from the tasks installed:
    /// `kept_chains` is the chain each one's record still carries. A
    /// generation held before and not among them keeps the chain it had.
    ///
    /// An absorbed generation whose own record is not installed is left out
    /// of the chain, even when the winner's record still holds its payload:
    /// the chain's size, release and fold all look the generation up as an
    /// installed task. This loses nothing for a certain key, because the
    /// classification of the rebuilt records makes a key uncertain
    /// when an absorbed generation's record is unknown, so such a key is
    /// held back and never reaches this function.
    pub(super) fn restore_keys(
        &mut self,
        kept_chains: &BTreeMap<TaskId, Vec<ChainItem>>,
        only: Option<&BTreeSet<Key>>,
    ) {
        for (key, tasks) in self.generations_by_key() {
            if only.is_some_and(|only| !only.contains(&key)) {
                continue;
            }
            let (waiting, holder) = self.waiting_and_holder(tasks);
            let chains = [waiting.as_ref(), holder.as_ref()]
                .into_iter()
                .flatten()
                .map(|task_id| (task_id.clone(), self.kept_chain(task_id, kept_chains)))
                .collect();
            self.occupancy.restore(&key, waiting, holder, chains);
        }
    }

    /// Every task held that has a coalescing key, by key.
    fn generations_by_key(&self) -> BTreeMap<Key, Vec<TaskId>> {
        let mut generations: BTreeMap<Key, Vec<TaskId>> = BTreeMap::new();
        for task_id in self.tasks.keys() {
            if let Some(key) = self.coalescing_key_of(task_id) {
                generations.entry(key).or_default().push(task_id.clone());
            }
        }
        generations
    }

    /// Of `tasks`, the generations of one key: the newest unfinished one that
    /// was never claimed and still waits, and the newest one ever claimed,
    /// which holds the key.
    fn waiting_and_holder(&self, mut tasks: Vec<TaskId>) -> (Option<TaskId>, Option<TaskId>) {
        tasks.retain(|task_id| !self.retention.holds(task_id));
        tasks.sort_by_cached_key(|task_id| {
            let task = &self.tasks[task_id];
            (
                task.submitted_at.as_ref().map(|at| at.unix_millis),
                task_id.clone(),
            )
        });
        let waiting = tasks
            .iter()
            .rev()
            .find(|task_id| {
                !self.was_claimed(task_id)
                    && matches!(
                        self.runs[&self.current_run[*task_id]].current_state(),
                        TaskRunState::Scheduled | TaskRunState::Queued
                    )
            })
            .cloned();
        let holder = tasks
            .iter()
            .rev()
            .find(|task_id| self.was_claimed(task_id))
            .cloned();
        (waiting, holder)
    }

    /// Whether any run of `task_id` was ever selected for a worker.
    fn was_claimed(&self, task_id: &TaskId) -> bool {
        self.runs_of_task[task_id]
            .iter()
            .any(|run| self.runs[run].selected_worker.is_some())
    }

    /// The chain `task_id` keeps: the one its record carries, or, for a task
    /// held before this batch, the one it had. An absorbed generation whose
    /// record is not installed cannot be folded either, so it is left out.
    fn kept_chain(
        &self,
        task_id: &TaskId,
        kept_chains: &BTreeMap<TaskId, Vec<ChainItem>>,
    ) -> Vec<ChainItem> {
        let chain = match kept_chains.get(task_id) {
            Some(chain) => chain.clone(),
            None => self.occupancy.chain(task_id).to_vec(),
        };
        chain
            .into_iter()
            .filter(|item| match item {
                ChainItem::Absorbed(absorbed) => self.tasks.contains_key(absorbed),
                ChainItem::Folded(_) => true,
            })
            .collect()
    }
}

/// When an installed pending task may first start, if not at once, and when
/// it expires. Only a task's first attempt waits out a delay (while still
/// scheduled) or an expiry: a later one follows a run that was claimed. Both
/// count from the submission by the wall clock, plus the reconcile skew
/// margin, so they err late.
fn installed_wait_bounds(
    task: &Task,
    state: TaskRunState,
    first_attempt: bool,
    now: Instant,
    wall_now: WallTime,
) -> (Option<Instant>, Option<Instant>) {
    let submitted_at = task.submitted_at.map_or(wall_now, WallTime::from);
    let deadline = |after_submission: u64| {
        submitted_at.deadline(
            Duration::from_millis(
                after_submission.saturating_add(RECONCILE_SKEW_MARGIN.as_ticks()),
            ),
            now,
            wall_now,
        )
    };
    let not_before = (state == TaskRunState::Scheduled && first_attempt)
        .then_some(task.delay_millis)
        .flatten()
        .map(deadline);
    let expires_at = first_attempt
        .then_some(task.expiry_millis)
        .flatten()
        .map(deadline);
    (not_before, expires_at)
}

/// The order records are installed in, oldest submission first, so the
/// waiting room's queue follows submission order.
pub(super) fn submission_order(record: &TaskRecord) -> (Option<u64>, Option<TaskId>) {
    let task = record.task.as_ref();
    (
        task.and_then(|task| task.submitted_at.as_ref().map(|at| at.unix_millis)),
        task.map(|task| task.task_id()),
    )
}

/// The chain `record` still carries, oldest entry first. An absorbed entry
/// that names no generation is left out; a folded one whose digest is missing
/// or unreadable gets the digest of its payload.
pub(super) fn chain_of(record: &TaskRecord) -> Vec<ChainItem> {
    chain_items(&record.retained_chain)
}

fn chain_items(entries: &[ChainEntry]) -> Vec<ChainItem> {
    entries
        .iter()
        .filter_map(|entry| match entry.entry.as_ref()? {
            chain_entry::Entry::Absorbed(absorbed) => absorbed
                .task_id
                .clone()
                .map(|id| ChainItem::Absorbed(TaskId::from(id))),
            chain_entry::Entry::Folded(folded) => Some(ChainItem::Folded(Folded {
                generations: folded
                    .generations
                    .iter()
                    .cloned()
                    .map(TaskId::from)
                    .collect(),
                digest: folded
                    .input_digest
                    .as_ref()
                    .and_then(|digest| Digest::try_from(digest).ok())
                    .unwrap_or_else(|| Digest::blake3(&folded.serialized_input)),
                payload: folded.serialized_input.clone(),
            })),
        })
        .collect()
}
