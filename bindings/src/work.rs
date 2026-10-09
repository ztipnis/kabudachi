//! What Python sees of the scheduler: claims, certifications and events, and
//! the Task records a networked shard holds.

use kabudachi_core::protocol::ids::{TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{TaskRecord, TaskRun, chain_entry};
use kabudachi_core::protocol::task::TaskRunState as DomainRunState;
use kabudachi_core::scheduler::{Certification, Claim, Event};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::outcomes::{PyEventKind, PyRunState};

/// A task handed to a worker to run, and the run that now belongs to it.
#[pyclass(name = "Claim", frozen)]
pub struct PyClaim {
    #[pyo3(get)]
    task_id: String,
    #[pyo3(get)]
    task_run_id: String,
    #[pyo3(get)]
    definition_id: String,
    #[pyo3(get)]
    source_version: u32,
    serialized_input: Vec<u8>,
    #[pyo3(get)]
    queue: String,
    /// Which attempt this is: 1 for the first, then one more per retry.
    #[pyo3(get)]
    attempt_number: u32,
    /// Whether this is a compaction run: `chain` holds the payloads to fold,
    /// oldest first, and `serialized_input` is empty. Its result is the folded
    /// payload, reported with `complete_compaction`.
    #[pyo3(get)]
    compaction: bool,
    chain: Vec<Vec<u8>>,
    /// The run's reconnect timeout in milliseconds, resolved: its task's
    /// own, or the shard's.
    #[pyo3(get)]
    reconnect_timeout_ms: u64,
}

#[pymethods]
impl PyClaim {
    /// The task's input exactly as it was submitted.
    #[getter]
    fn serialized_input<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.serialized_input)
    }

    /// The inputs of the generations this one superseded, oldest first, for
    /// the worker to fold before running the task. Empty for a task that
    /// absorbed none.
    #[getter]
    fn chain<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyBytes>> {
        self.chain
            .iter()
            .map(|payload| PyBytes::new(py, payload))
            .collect()
    }
}

impl From<Claim> for PyClaim {
    fn from(claim: Claim) -> Self {
        PyClaim {
            task_id: claim.task.task_id().as_str().to_owned(),
            task_run_id: claim.task_run_id.as_str().to_owned(),
            definition_id: claim.task.task_definition_id().as_str().to_owned(),
            source_version: claim.task.source_version,
            serialized_input: claim.task.serialized_input,
            queue: claim.task.queue,
            attempt_number: claim.attempt_number,
            compaction: claim.task.compacts.is_some(),
            chain: claim.chain,
            reconnect_timeout_ms: claim.reconnect_timeout.as_ticks(),
        }
    }
}

/// The leader's word that a run's result is the authoritative one.
#[pyclass(name = "Certification", frozen)]
pub struct PyCertification {
    #[pyo3(get)]
    task_id: String,
    #[pyo3(get)]
    task_run_id: String,
    result_digest: Vec<u8>,
}

#[pymethods]
impl PyCertification {
    /// The digest the worker reported for the run's result.
    #[getter]
    fn result_digest<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.result_digest)
    }
}

impl From<Certification> for PyCertification {
    fn from(certification: Certification) -> Self {
        PyCertification {
            task_id: certification.task_id.as_str().to_owned(),
            task_run_id: certification.task_run_id.as_str().to_owned(),
            result_digest: certification.result_digest.value().to_vec(),
        }
    }
}

/// Something the scheduler decided because time passed, for Python to act on.
#[pyclass(name = "Event", frozen)]
pub struct PyEvent {
    #[pyo3(get)]
    kind: PyEventKind,
    #[pyo3(get)]
    task_id: String,
    #[pyo3(get)]
    task_run_id: String,
    /// For `"cancelled"`: whether a worker had already claimed the task.
    #[pyo3(get)]
    was_running: bool,
    /// For `"superseded"`: the generation that replaced it.
    #[pyo3(get)]
    superseded_by: Option<String>,
    /// For `SLOW_DOWN`: whether the slow-down was raised (`True`) or cleared.
    #[pyo3(get)]
    active: bool,
    /// For `REFUSED`: why the leader refused the submission.
    #[pyo3(get)]
    reason: Option<String>,
    /// For `ABORT`: seconds until the run may be run again elsewhere, as of
    /// when the event was made.
    #[pyo3(get)]
    seconds_left: f64,
}

impl PyEvent {
    /// An event a networked worker raises itself: about `task_id`'s run
    /// `task_run_id` (empty for a submission).
    pub fn networked(kind: PyEventKind, task_id: &str, task_run_id: &str) -> Self {
        PyEvent {
            kind,
            task_id: task_id.to_owned(),
            task_run_id: task_run_id.to_owned(),
            was_running: matches!(kind, PyEventKind::Cancelled),
            superseded_by: None,
            active: false,
            reason: None,
            seconds_left: 0.0,
        }
    }

    /// The leader refused `task_id`'s submission for good, for `reason`.
    pub fn refused(task_id: &str, reason: &str) -> Self {
        PyEvent {
            reason: Some(reason.to_owned()),
            ..PyEvent::networked(PyEventKind::Refused, task_id, "")
        }
    }

    /// The run `task_run_id` of `task_id` may be run again elsewhere in
    /// `seconds_left` seconds.
    pub fn abort(task_id: &str, task_run_id: &str, seconds_left: f64) -> Self {
        PyEvent {
            seconds_left,
            ..PyEvent::networked(PyEventKind::Abort, task_id, task_run_id)
        }
    }
}

impl From<Event> for PyEvent {
    fn from(event: Event) -> Self {
        let kind = PyEventKind::from(&event);
        let nothing = PyEvent {
            kind,
            task_id: String::new(),
            task_run_id: String::new(),
            was_running: false,
            superseded_by: None,
            active: false,
            reason: None,
            seconds_left: 0.0,
        };
        match event {
            Event::Expired {
                task_id,
                task_run_id,
            } => PyEvent {
                task_id: task_id.as_str().to_owned(),
                task_run_id: task_run_id.as_str().to_owned(),
                ..nothing
            },
            Event::Cancelled {
                task_id,
                task_run_id,
                was_running,
            } => PyEvent {
                task_id: task_id.as_str().to_owned(),
                task_run_id: task_run_id.as_str().to_owned(),
                was_running,
                ..nothing
            },
            Event::SlowDown { active } => PyEvent { active, ..nothing },
            Event::RecordFull { task_id } => PyEvent {
                task_id: task_id.as_str().to_owned(),
                ..nothing
            },
            Event::CoalescedPayloadTooLarge {
                task_id,
                task_run_id,
            } => PyEvent {
                task_id: task_id.as_str().to_owned(),
                task_run_id: task_run_id.as_str().to_owned(),
                ..nothing
            },
            Event::Superseded {
                task_id,
                task_run_id,
                by,
            } => PyEvent {
                task_id: task_id.as_str().to_owned(),
                task_run_id: task_run_id.as_str().to_owned(),
                superseded_by: Some(by.as_str().to_owned()),
                ..nothing
            },
        }
    }
}

/// A Task record as the shard holds it: what a networked worker's tests
/// read the outcome of a run from.
#[pyclass(name = "TaskRecord", frozen)]
pub struct PyTaskRecord {
    /// No run of the task will change again.
    #[pyo3(get)]
    finished: bool,
    /// Every run, oldest attempt first.
    #[pyo3(get)]
    runs: Vec<PyRunRecord>,
    /// Whether the payloads the task retained from superseded generations
    /// start with a fold a compaction run made.
    #[pyo3(get)]
    folded: bool,
}

/// One run in a Task record.
#[pyclass(name = "RunRecord", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyRunRecord {
    #[pyo3(get)]
    task_run_id: String,
    #[pyo3(get)]
    state: PyRunState,
    /// The worker that claimed it; `None` while it is pending.
    #[pyo3(get)]
    worker: Option<String>,
    /// The type name of the error that failed it; empty unless it failed.
    #[pyo3(get)]
    failure_kind: String,
    result_digest: Option<Vec<u8>>,
}

#[pymethods]
impl PyRunRecord {
    /// The digest of the certified result; `None` unless the run succeeded.
    #[getter]
    fn result_digest<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.result_digest
            .as_deref()
            .map(|digest| PyBytes::new(py, digest))
    }
}

impl From<TaskRecord> for PyTaskRecord {
    fn from(record: TaskRecord) -> Self {
        PyTaskRecord {
            finished: record.finished,
            runs: record.runs.iter().filter_map(PyRunRecord::read).collect(),
            folded: record
                .retained_chain
                .iter()
                .any(|entry| matches!(entry.entry, Some(chain_entry::Entry::Folded(_)))),
        }
    }
}

impl PyRunRecord {
    /// `run` as Python sees it; `None` for a run whose state this build
    /// cannot read.
    fn read(run: &TaskRun) -> Option<Self> {
        let state = DomainRunState::try_from(run.state()).ok()?;
        Some(PyRunRecord {
            task_run_id: run
                .identity
                .as_ref()
                .and_then(|identity| identity.task_run_id.clone())
                .map(|id| TaskRunId::from(id).as_str().to_owned())
                .unwrap_or_default(),
            state: state.into(),
            worker: run
                .selected_worker
                .clone()
                .map(|worker| WorkerId::from(worker).as_str().to_owned()),
            failure_kind: run.failure_kind.clone(),
            result_digest: run
                .result_digest
                .as_ref()
                .map(|digest| digest.value.clone()),
        })
    }
}
