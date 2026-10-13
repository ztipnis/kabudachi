//! What Python sees of the scheduler: claims, certifications and events, and
//! the Task records a shard holds.

use kabudachi_core::protocol::ids::{TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{TaskRecord, TaskRun, chain_entry};
use kabudachi_core::protocol::task::TaskRunState as DomainRunState;
use kabudachi_core::manifest::Failure;
use kabudachi_core::scheduler::composite::EXPANSION_DEFINITION;
use kabudachi_core::scheduler::{Certification, Claim, Event};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::composite::state_name;
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
    /// The worker awaiting the result; None for a task nobody awaits across workers.
    #[pyo3(get)]
    client: Option<String>,
    /// The composite this task is a stage or member of; `None` for a task
    /// that is not.
    #[pyo3(get)]
    composite_id: Option<String>,
    /// Which stage or member of its composite this task is; 0 when it is
    /// none.
    #[pyo3(get)]
    composite_index: u64,
    /// Whether this is an expansion run: its input is a list a map is to be
    /// split over, and it reports the map that results.
    #[pyo3(get)]
    expansion: bool,
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
        let client = claim.task.client().map(|client| client.as_str().to_owned());
        let composite_id = claim.task.composite_id().map(|id| id.as_str().to_owned());
        let expansion = claim.task.task_definition_id().as_str() == EXPANSION_DEFINITION;
        PyClaim {
            client,
            composite_id,
            composite_index: claim.task.composite_index,
            expansion,
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
    /// For `RESULT`: the run's result, not certified yet.
    payload: Vec<u8>,
    /// For `CERTIFIED`: the digest the leader certified.
    result_digest: Vec<u8>,
    /// For `OUTCOME`: the state the task's run ended in; `None` when the
    /// shard does not know the task.
    run_state: Option<PyRunState>,
    /// For `COMPOSITE_FINISHED`: how the composite ended, `"succeeded"`,
    /// `"failed"` or `"cancelled"`.
    composite_state: Option<&'static str>,
    /// For `OUTCOME` and `COMPOSITE_FINISHED`: the error's type name, when
    /// the run or composite failed.
    #[pyo3(get)]
    failure_kind: String,
    /// For `COMPOSITE_FINISHED`: the error's message, when the composite
    /// failed.
    #[pyo3(get)]
    failure_message: String,
    /// For `COMPOSITE_FINISHED`: the Task whose run raised the error that
    /// ended the composite, when a run did.
    #[pyo3(get)]
    failure_task_id: Option<String>,
    /// For `COMPOSITE_FINISHED`: the composite that is over.
    #[pyo3(get)]
    composite_id: String,
    /// For `COMPOSITE_FINISHED`: the task whose continuation it is, if any.
    #[pyo3(get)]
    parent_task_id: Option<String>,
}

#[pymethods]
impl PyEvent {
    /// For `OUTCOME`: the `RunState` the task's run ended in; `None` when the
    /// shard does not know the task. For `COMPOSITE_FINISHED`: how the
    /// composite ended, `"succeeded"`, `"failed"` or `"cancelled"`.
    #[getter]
    fn state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self.composite_state {
            Some(state) => state.into_bound_py_any(py),
            None => self.run_state.into_bound_py_any(py),
        }
    }

    /// For `RESULT`: the run's result, not certified yet.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.payload)
    }

    /// For `CERTIFIED`: the digest the leader certified.
    #[getter]
    fn result_digest<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.result_digest)
    }
}

impl PyEvent {
    /// An event a networked worker raises itself: about `task_id`'s run
    /// `task_run_id` (empty for a submission).
    pub fn networked(kind: PyEventKind, task_id: &str, task_run_id: &str) -> Self {
        PyEvent {
            task_id: task_id.to_owned(),
            task_run_id: task_run_id.to_owned(),
            was_running: matches!(kind, PyEventKind::Cancelled),
            ..PyEvent::blank(kind)
        }
    }

    /// An event of `kind` about nothing yet: every other field empty.
    fn blank(kind: PyEventKind) -> Self {
        PyEvent {
            kind,
            task_id: String::new(),
            task_run_id: String::new(),
            was_running: false,
            superseded_by: None,
            active: false,
            reason: None,
            seconds_left: 0.0,
            payload: Vec::new(),
            result_digest: Vec::new(),
            run_state: None,
            composite_state: None,
            failure_kind: String::new(),
            failure_message: String::new(),
            failure_task_id: None,
            composite_id: String::new(),
            parent_task_id: None,
        }
    }

    /// What the event is about.
    pub fn kind(&self) -> PyEventKind {
        self.kind
    }

    /// A result of a run of `task_id`, delivered here, not certified yet.
    pub fn result(task_id: &str, task_run_id: &str, payload: Vec<u8>) -> Self {
        PyEvent {
            payload,
            ..PyEvent::networked(PyEventKind::Result, task_id, task_run_id)
        }
    }

    /// The leader certified the result of `task_id`'s run `task_run_id`.
    pub fn certified(task_id: &str, task_run_id: &str, result_digest: Vec<u8>) -> Self {
        PyEvent {
            result_digest,
            ..PyEvent::networked(PyEventKind::Certified, task_id, task_run_id)
        }
    }

    /// `task_id` ended in `state` without a result, or is unknown (`None`).
    pub fn outcome(
        task_id: &str,
        task_run_id: &str,
        state: Option<PyRunState>,
        failure_kind: String,
        superseded_by: Option<String>,
    ) -> Self {
        PyEvent {
            run_state: state,
            failure_kind,
            superseded_by,
            ..PyEvent::networked(PyEventKind::Outcome, task_id, task_run_id)
        }
    }

    /// The leader refused `task_id`'s submission for good, for `reason`.
    pub fn refused(task_id: &str, reason: &str) -> Self {
        PyEvent {
            reason: Some(reason.to_owned()),
            ..PyEvent::networked(PyEventKind::Refused, task_id, "")
        }
    }

    /// Submitters are to wait (`active`), or may send again.
    pub fn slow_down(active: bool) -> Self {
        PyEvent {
            active,
            ..PyEvent::blank(PyEventKind::SlowDown)
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
        let nothing = PyEvent::blank(PyEventKind::from(&event));
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
            Event::CompositeFinished(finished) => {
                let failure = finished.failure.unwrap_or(Failure::new("", ""));
                PyEvent {
                    failure_task_id: failure.task.map(|task| task.as_str().to_owned()),
                    composite_id: finished.composite_id.as_str().to_owned(),
                    parent_task_id: finished.parent_task.map(|task| task.as_str().to_owned()),
                    composite_state: Some(state_name(finished.state)),
                    failure_kind: failure.kind,
                    failure_message: failure.message,
                    ..nothing
                }
            }
        }
    }
}

/// A Task record as the shard holds it: what a task's status and the
/// outcome of its runs are read from.
#[pyclass(name = "TaskRecord", frozen)]
pub struct PyTaskRecord {
    #[pyo3(get)]
    task_id: String,
    /// When the client submitted the task, in milliseconds since the Unix
    /// epoch by the submitting worker's wall clock; 0 when unset.
    #[pyo3(get)]
    submitted_at_ms: u64,
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
    /// The error's message; empty unless it failed.
    #[pyo3(get)]
    failure_message: String,
    /// When a worker claimed it, in milliseconds since the Unix epoch by the
    /// leader's clock; `None` until then.
    #[pyo3(get)]
    claimed_at_ms: Option<u64>,
    /// When it started, as `claimed_at_ms`.
    #[pyo3(get)]
    started_at_ms: Option<u64>,
    result_digest: Option<Vec<u8>>,
    /// Which attempt this is: 1 for the first, then one more per retry.
    #[pyo3(get)]
    attempt_number: u32,
    /// When the leader created the run, in milliseconds since the Unix
    /// epoch by its wall clock; 0 when unset.
    #[pyo3(get)]
    created_at_ms: u64,
    /// When the leader last changed the run, as `created_at_ms`.
    #[pyo3(get)]
    updated_at_ms: u64,
    /// Whether the run's result reached its task's client before the run
    /// was reported completed.
    #[pyo3(get)]
    delivered: bool,
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
        let task = record.task.as_ref();
        PyTaskRecord {
            task_id: task
                .and_then(|task| task.task_id.clone())
                .map(|id| TaskId::from(id).as_str().to_owned())
                .unwrap_or_default(),
            submitted_at_ms: task
                .and_then(|task| task.submitted_at.as_ref())
                .map_or(0, |time| time.unix_millis),
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
            failure_message: run.failure_message.clone(),
            claimed_at_ms: run.claimed_at.as_ref().map(|time| time.unix_millis),
            started_at_ms: run.started_at.as_ref().map(|time| time.unix_millis),
            result_digest: run
                .result_digest
                .as_ref()
                .map(|digest| digest.value.clone()),
            attempt_number: run
                .identity
                .as_ref()
                .map_or(0, |identity| identity.attempt_number),
            created_at_ms: run.created_at.as_ref().map_or(0, |time| time.unix_millis),
            updated_at_ms: run.updated_at.as_ref().map_or(0, |time| time.unix_millis),
            delivered: run.delivered,
        })
    }
}
