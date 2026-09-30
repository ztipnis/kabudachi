//! What Python sees of the scheduler: claims, certifications and events.

use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::scheduler::{Certification, Claim, Event};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

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
    chain: Vec<Vec<u8>>,
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
            chain: claim.chain,
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
            result_digest: certification.result_digest,
        }
    }
}

/// Something the scheduler decided because time passed, for Python to act on.
#[pyclass(name = "Event", frozen)]
pub struct PyEvent {
    /// What happened, by name, for example `"expired"`.
    #[pyo3(get)]
    kind: &'static str,
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
}

impl From<Event> for PyEvent {
    fn from(event: Event) -> Self {
        match event {
            Event::Expired {
                task_id,
                task_run_id,
            } => PyEvent {
                kind: "expired",
                task_id: task_id.as_str().to_owned(),
                task_run_id: task_run_id.as_str().to_owned(),
                was_running: false,
                superseded_by: None,
            },
            Event::Cancelled {
                task_id,
                task_run_id,
                was_running,
            } => PyEvent {
                kind: "cancelled",
                task_id: task_id.as_str().to_owned(),
                task_run_id: task_run_id.as_str().to_owned(),
                was_running,
                superseded_by: None,
            },
            Event::SlowDown { active } => PyEvent {
                kind: if active {
                    "slow_down"
                } else {
                    "slow_down_cleared"
                },
                task_id: String::new(),
                task_run_id: String::new(),
                was_running: false,
                superseded_by: None,
            },
            Event::Superseded {
                task_id,
                task_run_id,
                by,
            } => PyEvent {
                kind: "superseded",
                task_id: task_id.as_str().to_owned(),
                task_run_id: task_run_id.as_str().to_owned(),
                was_running: false,
                superseded_by: Some(by.as_str().to_owned()),
            },
        }
    }
}
