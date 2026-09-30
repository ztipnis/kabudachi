//! The seam through which whoever watches a scheduler hears about every
//! change it makes. Production installs nothing ([`NoObserver`]); a test
//! installs a recording spy.

use crate::protocol::ids::TaskId;
use crate::protocol::messages::{Task, TaskRun};

/// Whoever the scheduler tells about each change it makes. The scheduler
/// calls `notify` once per change, before the call that made it returns:
/// a task recorded or forgotten, a run created or changed, a change of
/// leadership it noticed, a change of memory in use, or `SlowDown` raised or
/// cleared. Within one call, notifications come in the order the changes
/// were made, and a change that `take_events` also reports is notified where
/// its event is recorded, so the two orders agree. Nothing observes in
/// production ([`NoObserver`]); a test installs a recording spy.
pub trait Observer {
    fn notify(&mut self, change: Change<'_>, counts: Counts);
}

/// One change the scheduler made.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Change<'a> {
    /// `submit` recorded this task. A task never changes afterwards.
    TaskRecorded(&'a Task),
    /// This run was created, or the call changed it. It is shown as the call
    /// left it: state, selected worker, result digest, failure kind.
    Run(&'a TaskRun),
    /// This finished task was forgotten, with every run of it.
    TaskForgotten(&'a TaskId),
    /// The scheduler found that it now leads (`true`) or no longer does:
    /// when handed a grant, or at the first call that reads its clock after
    /// its lease ended. `next_deadline` reports that end, so a caller that
    /// calls `catch_up` at its deadline hears the lapse at its instant.
    Leadership(bool),
    /// Memory in use changed; `Counts::memory_in_use` is the new value.
    Memory,
    /// `SlowDown` was raised (`true`) or cleared, as the `Event::SlowDown`
    /// recorded with it says.
    SlowDown(bool),
}

/// The scheduler's counts just after a change, so an observer never has to
/// work them out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// Tasks queued to be claimed, including generations their coalescing
    /// key holds back. A delayed task counts once it is due.
    pub pending: usize,
    /// Serialized bytes of every task that has not finished, and of every
    /// superseded one still needed by the generation that absorbed it.
    pub memory_in_use: u64,
}

/// The observer production uses: it ignores every change.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoObserver;

impl Observer for NoObserver {
    fn notify(&mut self, _: Change<'_>, _: Counts) {}
}
