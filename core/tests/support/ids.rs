use std::cell::Cell;
use std::rc::Rc;

use kabudachi_core::protocol::ids::{IdGenerator, TaskId, TaskRunId};

/// Hands out `task-1`, `task-2`, ... and `run-1`, `run-2`, ... so tests can
/// name the IDs they expect. `Clone` shares the counters.
#[derive(Clone, Default)]
pub struct SequentialIds {
    next_task: Rc<Cell<u64>>,
    next_run: Rc<Cell<u64>>,
}

impl SequentialIds {
    pub fn new() -> Self {
        Self::default()
    }
}

fn bump(counter: &Cell<u64>) -> u64 {
    counter.set(counter.get() + 1);
    counter.get()
}

impl IdGenerator for SequentialIds {
    fn next_task_id(&self) -> TaskId {
        TaskId::new(format!("task-{}", bump(&self.next_task)))
    }

    fn next_task_run_id(&self) -> TaskRunId {
        TaskRunId::new(format!("run-{}", bump(&self.next_run)))
    }
}
