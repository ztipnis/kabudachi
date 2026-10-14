//! The task input versions a worker can run.

use std::collections::BTreeMap;

use crate::protocol::ids::TaskDefinitionId;
use crate::protocol::messages::Task;
use crate::protocol::messages::prelude::*;

/// The task input version a worker runs, per task definition. A task whose
/// input is newer cannot run there; one at an older version may, after its
/// migrations, which the worker judges itself. A definition it does not list
/// is not judged here.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunnableVersions {
    current: BTreeMap<TaskDefinitionId, u32>,
}

impl RunnableVersions {
    /// This worker runs `definition`'s input at version `current`.
    #[must_use]
    pub fn with(mut self, definition: TaskDefinitionId, current: u32) -> Self {
        self.current.insert(definition, current);
        self
    }

    /// Why this worker cannot run `task`, or `None` if it may.
    pub fn newer(&self, task: &Task) -> Option<String> {
        let definition = task.task_definition_id();
        let current = *self.current.get(&definition)?;
        (task.source_version > current).then(|| {
            format!(
                "version {} of task {}'s input is newer than this worker's {current}",
                task.source_version,
                definition.as_str()
            )
        })
    }
}
