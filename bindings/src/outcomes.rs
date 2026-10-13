//! Typed values that cross to Python in place of strings: what kind of event
//! the scheduler reported, how a cancel ended, and what state a run is in.
//! Each is a thin, one-to-one mirror of a core type, and every conversion is
//! exhaustive so a new core variant fails to compile here.

use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{Cancellation, Event};
use pyo3::prelude::*;
use pyo3::types::PyType;

/// What a scheduler event is about. `SLOW_DOWN` says memory use crossed the
/// soft limit; the event's `active` says whether it was raised or cleared.
/// The networked ones (from `ACCEPTED` on) come only from a networked worker.
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
    /// A task this worker submitted has a result delivered, not certified yet.
    #[pyo3(name = "RESULT")]
    Result,
    /// The leader certified a run of a task this worker submitted.
    #[pyo3(name = "CERTIFIED")]
    Certified,
    /// A task this worker submitted ended without a result, or its shard
    /// no longer knows it (`state` is `None`).
    #[pyo3(name = "OUTCOME")]
    Outcome,
    /// A composite the shard's leader drove is over: `composite_id` names it
    /// and, when it was a task's continuation, `parent_task_id` names the task.
    #[pyo3(name = "COMPOSITE_FINISHED")]
    CompositeFinished,
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
            Event::CompositeFinished(_) => PyEventKind::CompositeFinished,
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

/// Where a task run is in its life. It pickles as its variant's name, so it
/// crosses to another process (a task process reading a task's status) as
/// the same variant.
#[pyclass(
    name = "RunState",
    module = "kabudachi._native",
    eq,
    eq_int,
    frozen,
    hash,
    skip_from_py_object
)]
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

/// What `__reduce__` gives pickle: a callable and the arguments that rebuild
/// the value.
type Reduced<'py> = (Bound<'py, PyAny>, (Bound<'py, PyType>, &'static str));

#[pymethods]
impl PyRunState {
    /// `getattr(RunState, name)`: unpickling looks the variant up by name.
    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<Reduced<'py>> {
        let getattr = py.import("builtins")?.getattr("getattr")?;
        Ok((getattr, (py.get_type::<Self>(), self.python_name())))
    }
}

impl PyRunState {
    /// The variant's name in Python.
    fn python_name(self) -> &'static str {
        match self {
            PyRunState::Scheduled => "SCHEDULED",
            PyRunState::Queued => "QUEUED",
            PyRunState::Claimed => "CLAIMED",
            PyRunState::Running => "RUNNING",
            PyRunState::Succeeded => "SUCCEEDED",
            PyRunState::Failed => "FAILED",
            PyRunState::Expired => "EXPIRED",
            PyRunState::Superseded => "SUPERSEDED",
            PyRunState::Cancelled => "CANCELLED",
            PyRunState::Lost => "LOST",
            PyRunState::Orphaned => "ORPHANED",
        }
    }
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
