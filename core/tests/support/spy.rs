//! A recording spy on a scheduler's observer seam.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{IdGenerator, TaskId, TaskRunId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{Task, TaskRun};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{Change, Counts, Observer, Scheduler};
use kabudachi_core::time::Clock;

/// A recording spy on a scheduler. Hand the scheduler a clone
/// (`Scheduler::with_observer`) and read through the one you keep: every
/// clone shares one log. It records what it is told and indexes it by task;
/// it never works anything out for itself.
#[derive(Debug, Clone, Default)]
pub struct Spy(Rc<RefCell<Log>>);

/// The notes in order, plus indexes filled from the notifications themselves.
/// The indexes keep reads off a scan of the log (the proptest model reads
/// states after every step).
#[derive(Debug, Default)]
struct Log {
    notes: Vec<Note>,
    tasks: BTreeMap<TaskId, Task>,
    runs: BTreeMap<TaskId, TaskRun>,
    forgotten: BTreeSet<TaskId>,
    leading: bool,
    revisions: Vec<TaskRecord>,
    only_latest_revision: bool,
}

/// One notification as the spy recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    pub change: Noted,
    pub counts: Counts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Noted {
    TaskRecorded(TaskId),
    Run {
        task: TaskId,
        run: TaskRunId,
        state: TaskRunState,
    },
    TaskForgotten(TaskId),
    Leadership(bool),
    Memory,
    SlowDown(bool),
}

impl Observer for Spy {
    fn notify(&mut self, change: Change<'_>, counts: Counts) {
        let mut log = self.0.borrow_mut();
        let noted = match change {
            Change::TaskRecorded(task) => {
                log.tasks.insert(task.task_id(), task.clone());
                Noted::TaskRecorded(task.task_id())
            }
            Change::Run(run) => {
                log.runs.insert(run.task_id(), run.clone());
                Noted::Run {
                    task: run.task_id(),
                    run: run.task_run_id(),
                    state: run.current_state(),
                }
            }
            Change::TaskForgotten(task) => {
                log.forgotten.insert(task.clone());
                Noted::TaskForgotten(task.clone())
            }
            Change::Leadership(leading) => {
                log.leading = leading;
                Noted::Leadership(leading)
            }
            Change::Memory => Noted::Memory,
            Change::SlowDown(active) => Noted::SlowDown(active),
        };
        log.notes.push(Note {
            change: noted,
            counts,
        });
    }

    fn revision(&mut self, revision: TaskRecord) {
        let mut log = self.0.borrow_mut();
        if log.only_latest_revision
            && let Some(last) = log.revisions.last_mut()
            && last.task == revision.task
        {
            *last = revision;
            return;
        }
        log.revisions.push(revision);
    }
}

impl Spy {
    /// How many notes so far: a mark for `since` and `queued_since`.
    pub fn mark(&self) -> usize {
        self.0.borrow().notes.len()
    }

    /// The notes recorded after `mark`, in order: what the calls since changed.
    pub fn since(&self, mark: usize) -> Vec<Note> {
        self.0.borrow().notes[mark..].to_vec()
    }

    /// The counts the last note carried; zero before any, as for a new scheduler.
    pub fn pending(&self) -> usize {
        self.last_counts().pending
    }

    pub fn memory_in_use(&self) -> u64 {
        self.last_counts().memory_in_use
    }

    /// What the last leadership note said; `false` before any, as a new
    /// scheduler does not lead.
    pub fn leading(&self) -> bool {
        self.0.borrow().leading
    }

    /// From now on keeps only the latest revision while the same task keeps
    /// being revised, so a test that drives one task through many large
    /// revisions does not hold every one of them in memory.
    pub fn keep_only_the_latest_revision_of_a_busy_task(&self) {
        self.0.borrow_mut().only_latest_revision = true;
    }

    /// Every revision the scheduler published, in order.
    pub fn revisions(&self) -> Vec<TaskRecord> {
        self.0.borrow().revisions.clone()
    }

    /// Every revision published since the last call, oldest first. A
    /// `Cluster` drains this after each step to write the revisions, so a
    /// cluster node's spy has an empty `revisions()` once a step has run.
    pub fn take_revisions(&self) -> Vec<TaskRecord> {
        std::mem::take(&mut self.0.borrow_mut().revisions)
    }

    /// `task` as `submit` recorded it. Panics if it was never recorded.
    pub fn task(&self, task: &TaskId) -> Task {
        self.0
            .borrow()
            .tasks
            .get(task)
            .unwrap_or_else(|| panic!("the spy was never told of {task:?}"))
            .clone()
    }

    /// The last run notified for `task` (its current run), as the call that
    /// last changed it left it. Panics if none.
    pub fn run_of(&self, task: &TaskId) -> TaskRun {
        self.0
            .borrow()
            .runs
            .get(task)
            .unwrap_or_else(|| panic!("the spy was never told of a run of {task:?}"))
            .clone()
    }

    pub fn state_of(&self, task: &TaskId) -> TaskRunState {
        self.run_of(task).current_state()
    }

    pub fn forgotten(&self, task: &TaskId) -> bool {
        self.0.borrow().forgotten.contains(task)
    }

    /// The tasks whose runs were notified entering `Queued` after `mark`, in order.
    pub fn queued_since(&self, mark: usize) -> Vec<TaskId> {
        self.since(mark)
            .into_iter()
            .filter_map(|note| match note.change {
                Noted::Run {
                    task,
                    state: TaskRunState::Queued,
                    ..
                } => Some(task),
                _ => None,
            })
            .collect()
    }

    /// `state_of(task)`, after checking the notes against the scheduler's
    /// own public reads: its newest run (`runs_of(task).last()`) is the run
    /// last notified for `task`, in the notified state (`task_run`). A missed
    /// notification fails here. Panics on a task never notified or forgotten.
    pub fn checked_state_of<C: Clock, I: IdGenerator, O: Observer>(
        &self,
        scheduler: &Scheduler<C, I, O>,
        task: &TaskId,
    ) -> TaskRunState {
        let noted = self.run_of(task);
        let newest = scheduler
            .runs_of(task)
            .last()
            .cloned()
            .unwrap_or_else(|| panic!("the scheduler has no run of {task:?}"));
        assert_eq!(
            noted.task_run_id(),
            newest,
            "the spy was not told of {task:?}'s newest run"
        );
        let actual = scheduler
            .task_run(&newest)
            .unwrap_or_else(|| panic!("the scheduler has no run {newest:?}"))
            .current_state();
        assert_eq!(
            noted.current_state(),
            actual,
            "the spy missed a change to {newest:?}"
        );
        actual
    }

    fn last_counts(&self) -> Counts {
        self.0
            .borrow()
            .notes
            .last()
            .map(|note| note.counts)
            .unwrap_or_default()
    }
}
