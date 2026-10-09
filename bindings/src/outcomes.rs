//! Typed values that cross to Python in place of strings: what kind of event
//! the scheduler reported, how a cancel ended, and what state a run is in.
//! Each is a thin, one-to-one mirror of a core type, and every conversion is
//! exhaustive so a new core variant fails to compile here.

use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{Cancellation, Event};
use pyo3::prelude::*;

/// What a scheduler event is about. `SLOW_DOWN` says memory use crossed the
/// soft limit; the event's `active` says whether it was raised or cleared.
/// The last four come only from a networked worker.
#[pyclass(name = "EventKind", eq, eq_int, frozen, hash, skip_from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PyEventKind {
    #[pyo3(name = "EXPIRED")]
    Expired,
    #[pyo3(name = "SUPERSEDED")]
    Superseded,
    #[pyo3(name = "SLOW_DOWN")]
    SlowDown,
    #[pyo3(name = "CANCELLED")]
    Cancelled,
    #[pyo3(name = "RECORD_FULL")]
    RecordFull,
    #[pyo3(name = "COALESCED_PAYLOAD_TOO_LARGE")]
    CoalescedPayloadTooLarge,
    /// A networked worker's submission was stored by its shard's leader.
    #[pyo3(name = "ACCEPTED")]
    Accepted,
    /// A networked worker's submission was refused for good; `reason` says why.
    #[pyo3(name = "REFUSED")]
    Refused,
    /// A run this worker holds may be run again elsewhere once
    /// `seconds_left` has passed: its leader cannot be shown to hear it.
    #[pyo3(name = "ABORT")]
    Abort,
    /// The leader hears this worker again: a run's pending abort is lifted.
    #[pyo3(name = "ABORT_WITHDRAWN")]
    AbortWithdrawn,
}

impl From<&Event> for PyEventKind {
    fn from(event: &Event) -> Self {
        match event {
            Event::Expired { .. } => PyEventKind::Expired,
            Event::Superseded { .. } => PyEventKind::Superseded,
            Event::SlowDown { .. } => PyEventKind::SlowDown,
            Event::Cancelled { .. } => PyEventKind::Cancelled,
            Event::RecordFull { .. } => PyEventKind::RecordFull,
            Event::CoalescedPayloadTooLarge { .. } => PyEventKind::CoalescedPayloadTooLarge,
        }
    }
}

/// How a request to cancel a task ended.
#[pyclass(name = "CancelOutcome", eq, eq_int, frozen, hash, skip_from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PyCancelOutcome {
    #[pyo3(name = "CANCELLED")]
    Cancelled,
    #[pyo3(name = "ALREADY_FINISHED")]
    AlreadyFinished,
    #[pyo3(name = "UNKNOWN_TASK")]
    UnknownTask,
}

impl From<Cancellation> for PyCancelOutcome {
    fn from(outcome: Cancellation) -> Self {
        match outcome {
            Cancellation::Cancelled { .. } => PyCancelOutcome::Cancelled,
            Cancellation::AlreadyFinished => PyCancelOutcome::AlreadyFinished,
            Cancellation::UnknownTask => PyCancelOutcome::UnknownTask,
        }
    }
}

/// Where a task run is in its life.
#[pyclass(name = "RunState", eq, eq_int, frozen, hash, skip_from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PyRunState {
    #[pyo3(name = "SCHEDULED")]
    Scheduled,
    #[pyo3(name = "QUEUED")]
    Queued,
    #[pyo3(name = "CLAIMED")]
    Claimed,
    #[pyo3(name = "RUNNING")]
    Running,
    #[pyo3(name = "SUCCEEDED")]
    Succeeded,
    #[pyo3(name = "FAILED")]
    Failed,
    #[pyo3(name = "EXPIRED")]
    Expired,
    #[pyo3(name = "SUPERSEDED")]
    Superseded,
    #[pyo3(name = "CANCELLED")]
    Cancelled,
    #[pyo3(name = "LOST")]
    Lost,
    #[pyo3(name = "ORPHANED")]
    Orphaned,
}

impl From<TaskRunState> for PyRunState {
    fn from(state: TaskRunState) -> Self {
        match state {
            TaskRunState::Scheduled => PyRunState::Scheduled,
            TaskRunState::Queued => PyRunState::Queued,
            TaskRunState::Claimed => PyRunState::Claimed,
            TaskRunState::Running => PyRunState::Running,
            TaskRunState::Succeeded => PyRunState::Succeeded,
            TaskRunState::Failed => PyRunState::Failed,
            TaskRunState::Expired => PyRunState::Expired,
            TaskRunState::Superseded => PyRunState::Superseded,
            TaskRunState::Cancelled => PyRunState::Cancelled,
            TaskRunState::Lost => PyRunState::Lost,
            TaskRunState::Orphaned => PyRunState::Orphaned,
        }
    }
}

/// The coalescing key and drop-oldest opt-in a submission carries, or `None`
/// for a task that does not coalesce. A coalescing task always has a key (its
/// default is ""), no other kind has one, and only a coalescing task may drop
/// its oldest retained payloads.
pub fn coalescing(
    kind: &str,
    key: Option<String>,
    drop_oldest: bool,
) -> Result<Option<(String, bool)>, String> {
    match (kind, key) {
        ("coalescing", Some(key)) => Ok(Some((key, drop_oldest))),
        ("coalescing", None) => Err("a coalescing task needs a key".into()),
        ("task" | "ephemeral", None) if !drop_oldest => Ok(None),
        ("task" | "ephemeral", None) => {
            Err("only a coalescing task may drop its oldest payloads".into())
        }
        ("task" | "ephemeral", Some(_)) => Err(format!("a {kind} task takes no key")),
        (other, _) => Err(format!("{other:?} is not a task kind")),
    }
}
