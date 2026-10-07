//! The seam through which whoever watches a scheduler takes the records it
//! publishes. The runtime installs the node's record store; a scheduler that
//! nobody watches installs [`NoObserver`]; a test installs a recording spy.

use crate::protocol::generated::TaskRecord;

/// Whoever the scheduler hands its published records to.
pub trait Observer {
    /// Takes the whole record of one task as a call left it, stamped with
    /// the next version of this leader's term. Called at the end of every
    /// call that changed the task, once per changed task, while the scheduler
    /// leads. Nothing is published once the lease has ended: a change made
    /// then is published at the next call that finds the scheduler leading.
    fn revision(&mut self, revision: TaskRecord);
}

/// The observer production uses: it ignores every revision.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoObserver;

impl Observer for NoObserver {
    fn revision(&mut self, _: TaskRecord) {}
}
