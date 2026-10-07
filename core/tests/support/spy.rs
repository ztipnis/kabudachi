//! A recording observer: it keeps the revisions a scheduler publishes, which
//! only the observer is handed. Everything else a test asks of a scheduler it
//! reads from the scheduler's own public state.

use std::cell::RefCell;
use std::rc::Rc;

use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::protocol::messages::Task;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::scheduler::Observer;

/// Hand the scheduler a clone (`Scheduler::with_observer`) and read through
/// the one you keep: every clone shares one log.
#[derive(Debug, Clone, Default)]
pub struct Spy(Rc<RefCell<Vec<TaskRecord>>>);

impl Observer for Spy {
    fn revision(&mut self, revision: TaskRecord) {
        self.0.borrow_mut().push(revision);
    }
}

impl Spy {
    /// How many revisions so far: a mark for `revised_since`.
    pub fn mark(&self) -> usize {
        self.0.borrow().len()
    }

    /// The tasks revised after `mark`, in order of publication: what the
    /// calls since changed, while the scheduler led.
    pub fn revised_since(&self, mark: usize) -> Vec<TaskId> {
        self.0.borrow()[mark..]
            .iter()
            .map(|record| record_task(record).task_id())
            .collect()
    }

    /// Every revision the scheduler published, in order.
    pub fn revisions(&self) -> Vec<TaskRecord> {
        self.0.borrow().clone()
    }

    /// Every revision the scheduler published of `task`, in order.
    pub fn revisions_of(&self, task: &TaskId) -> Vec<TaskRecord> {
        self.revisions()
            .into_iter()
            .filter(|record| record_task(record).task_id() == *task)
            .collect()
    }

    /// The newest revision published of `task`, if any. It borrows the log
    /// and clones one record, so a caller that asks per step stays linear.
    pub fn newest_revision_of(&self, task: &TaskId) -> Option<TaskRecord> {
        self.0
            .borrow()
            .iter()
            .rev()
            .find(|record| record_task(record).task_id() == *task)
            .cloned()
    }

    /// Every revision published since the last call, oldest first. A
    /// `Cluster` drains this after each step to write the revisions, so a
    /// cluster node's spy has an empty `revisions()` once a step has run.
    pub fn take_revisions(&self) -> Vec<TaskRecord> {
        std::mem::take(&mut *self.0.borrow_mut())
    }

    /// `task` as its newest revision carries it. Panics if none was
    /// published.
    pub fn task(&self, task: &TaskId) -> Task {
        self.newest_revision_of(task)
            .map(|record| record_task(&record))
            .unwrap_or_else(|| panic!("no revision of {task:?} was published"))
    }
}

fn record_task(record: &TaskRecord) -> Task {
    record.task.clone().expect("a revision carries its task")
}
