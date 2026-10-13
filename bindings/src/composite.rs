//! What Python sees of composites: a step described as a plain tree, the
//! opaque manifest of a payload stored for a composite, and the inputs and
//! results a reader resolves.
//!
//! A tree is built from tuples, so the same step always encodes the same
//! bytes:
//!
//! - a composite is `(kind, on_error, stages)`, with `kind` one of `"flow"`,
//!   `"group"` or `"map"` and `on_error` one of `"fail_fast"` or
//!   `"collect_all"`;
//! - a task is `("task", definition_id, version, options, value, fields)`,
//!   where `options` is a dict with the key `queue` and, each optional,
//!   `retries`, `ephemeral`, `non_retriable` and `reconnect_timeout_ms`, and
//!   `value` is the task's whole input as `bytes`, `("manifest", manifest)`
//!   for one stored with `store_composite_payload`, or `None` when the task
//!   takes its input from before it, and each field is `(name, kind, value)`
//!   with `kind` one of `"int"`, `"uint"`, `"double"`, `"bool"`, `"string"` or
//!   `"bytes"`.

use kabudachi_core::manifest::Manifest;
use kabudachi_core::protocol::generated::{
    self, BoundField, BoundTaskStep, StageDescription, StepDescription, bound_field, bound_task_step,
    stage_description,
};
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskRunId};
use kabudachi_core::scheduler::composite::{
    BoundInput, BoundStep, CompositeState, ExpansionRequest, FieldValue, MemberInput, OnError, Resolved, Stage,
    Step,
};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use pyo3::IntoPyObjectExt;

/// The manifest of a payload stored for a composite, which goes into a tree
/// as `("manifest", manifest)`. Opaque to Python.
#[pyclass(name = "Manifest", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyManifest(pub(crate) Manifest);

/// What an expansion run is asked: the task to run on every item of a list
/// another stage returned, and how the map over the items treats a failure.
/// The list itself is read with `NativeRuntime.resolve_expansion`.
#[pyclass(name = "Expansion", frozen, skip_from_py_object)]
pub struct PyExpansion(ExpansionRequest);

#[pymethods]
impl PyExpansion {
    /// The task the map runs on every item.
    #[getter]
    fn definition_id(&self) -> &str {
        self.0.task.definition.as_str()
    }

    #[getter]
    fn version(&self) -> u32 {
        self.0.task.version
    }

    /// `"fail_fast"` or `"collect_all"`.
    #[getter]
    fn on_error(&self) -> &'static str {
        match self.0.on_error {
            OnError::FailFast => "fail_fast",
            OnError::CollectAll => "collect_all",
        }
    }

    /// The task's options as a tree names them (see `describe_step`).
    #[getter]
    fn settings<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let options = &self.0.task.options;
        let settings = PyDict::new(py);
        settings.set_item("queue", &options.queue)?;
        settings.set_item("retries", options.retries)?;
        settings.set_item("ephemeral", options.ephemeral)?;
        settings.set_item("non_retriable", options.non_retriable)?;
        settings.set_item(
            "reconnect_timeout_ms",
            options.reconnect_timeout.map(|timeout| timeout.as_millis() as u64),
        )?;
        Ok(settings)
    }
}

/// The request an expansion run's claim carries as its input. Raises
/// `ValueError` for bytes that are not one.
#[pyfunction]
pub fn read_expansion(serialized_input: &[u8]) -> PyResult<PyExpansion> {
    ExpansionRequest::decode(serialized_input)
        .map(PyExpansion)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

/// The id of the composite a task's run returned as its continuation.
#[pyfunction]
pub fn continuation_of(task_run_id: &str) -> String {
    CompositeId::continuation_of(&TaskRunId::new(task_run_id)).as_str().to_owned()
}

/// Encodes the step `tree` describes: the same tree always gives the same
/// bytes. Raises `TypeError` for a tree of the wrong shape and `ValueError`
/// for one that describes a step no leader would accept.
#[pyfunction]
pub fn describe_step<'py>(py: Python<'py>, tree: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let step = step_from_tree(tree)?;
    Ok(PyBytes::new(py, &step.encode()))
}

/// The step `tree` describes, checked as the leader checks it.
pub(crate) fn step_from_tree(tree: &Bound<'_, PyAny>) -> PyResult<Step> {
    let description = composite_description(tree)?;
    Step::try_from(&description).map_err(|error| PyValueError::new_err(error.to_string()))
}

fn composite_description(tree: &Bound<'_, PyAny>) -> PyResult<StepDescription> {
    let (kind, on_error, stages): (String, String, Vec<Bound<'_, PyAny>>) = tree.extract()?;
    let kind = match kind.as_str() {
        "flow" => generated::CompositeKind::Flow,
        "group" => generated::CompositeKind::Group,
        "map" => generated::CompositeKind::Map,
        other => return Err(PyValueError::new_err(format!("{other:?} is not a composite kind"))),
    };
    let on_error = match on_error.as_str() {
        "fail_fast" => generated::OnError::FailFast,
        "collect_all" => generated::OnError::CollectAll,
        other => return Err(PyValueError::new_err(format!("{other:?} is not an error policy"))),
    };
    let stages = stages.iter().map(stage_description).collect::<PyResult<Vec<_>>>()?;
    Ok(StepDescription { kind: kind as i32, on_error: on_error as i32, stages })
}

fn stage_description(stage: &Bound<'_, PyAny>) -> PyResult<StageDescription> {
    let first = stage.get_item(0)?;
    let step = if first.extract::<&str>().is_ok_and(|kind| kind == "task") {
        stage_description::Step::Task(task_description(stage)?)
    } else {
        stage_description::Step::Composite(composite_description(stage)?)
    };
    Ok(StageDescription { step: Some(step) })
}

fn task_description(task: &Bound<'_, PyAny>) -> PyResult<BoundTaskStep> {
    let (_, definition_id, version, options, value, fields): (
        String,
        String,
        u32,
        Bound<'_, PyDict>,
        Bound<'_, PyAny>,
        Vec<(String, String, Bound<'_, PyAny>)>,
    ) = task.extract()?;
    let mut fields = fields
        .into_iter()
        .map(|(name, kind, value)| field_description(name, &kind, &value))
        .collect::<PyResult<Vec<_>>>()?;
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(BoundTaskStep {
        definition: Some(TaskDefinitionId::new(definition_id).into()),
        version,
        options: Some(options_description(&options)?),
        input: input_description(&value)?,
        fields,
    })
}

fn options_description(options: &Bound<'_, PyDict>) -> PyResult<generated::TaskOptions> {
    let mut described = generated::TaskOptions::default();
    for (key, value) in options.iter() {
        if value.is_none() {
            continue;
        }
        match key.extract::<&str>()? {
            "queue" => described.queue = value.extract()?,
            "retries" => described.max_retries = value.extract()?,
            "ephemeral" => described.ephemeral = value.extract()?,
            "non_retriable" => described.non_retriable = value.extract()?,
            "reconnect_timeout_ms" => described.reconnect_timeout_ms = value.extract()?,
            other => return Err(PyTypeError::new_err(format!("{other:?} is not a task option"))),
        }
    }
    Ok(described)
}

fn input_description(value: &Bound<'_, PyAny>) -> PyResult<Option<bound_task_step::Input>> {
    if value.is_none() {
        return Ok(None);
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(Some(bound_task_step::Input::Value(bytes.as_bytes().to_vec())));
    }
    let (marker, manifest): (String, PyRef<'_, PyManifest>) = value.extract()?;
    if marker != "manifest" {
        return Err(PyTypeError::new_err("a task's value is bytes, (\"manifest\", manifest) or None"));
    }
    Ok(Some(bound_task_step::Input::ValueManifest(manifest.0.clone().into())))
}

fn field_description(name: String, kind: &str, value: &Bound<'_, PyAny>) -> PyResult<BoundField> {
    let value = match kind {
        "int" => bound_field::Value::IntValue(value.extract()?),
        "uint" => bound_field::Value::UintValue(value.extract()?),
        "double" => bound_field::Value::DoubleValue(value.extract()?),
        "bool" => bound_field::Value::BoolValue(value.extract()?),
        "string" => bound_field::Value::StringValue(value.extract()?),
        "bytes" => bound_field::Value::BytesValue(value.extract()?),
        other => return Err(PyValueError::new_err(format!("{other:?} is not a field kind"))),
    };
    Ok(BoundField { name, value: Some(value) })
}

/// Replaces every bound value of `step` above `inline_bound`, nested
/// composites' included, with the manifest of its chunks, and returns the
/// values to store, depth first in stage order. The caller stores them all
/// under the root of the tree (the composite it submits, or the continuation
/// it certifies), whose record is held unfinished until the whole tree is
/// over.
pub(crate) fn take_large_values(step: &mut Step, inline_bound: usize) -> Vec<Vec<u8>> {
    let mut taken = Vec::new();
    for stage in &mut step.stages {
        match stage {
            Stage::Task(bound) => {
                if let BoundInput::Value(value) = &mut bound.input
                    && value.len() > inline_bound
                {
                    let bytes = std::mem::take(value);
                    bound.input = BoundInput::Chunked(Manifest::chunk(&bytes).manifest);
                    taken.push(bytes);
                }
            }
            Stage::Composite(nested) => taken.extend(take_large_values(nested, inline_bound)),
        }
    }
    taken
}

/// Bytes an awaitable resolves to, as a Python `bytes`.
pub(crate) struct PyBytesValue(pub(crate) Vec<u8>);

impl<'py> IntoPyObject<'py> for PyBytesValue {
    type Target = PyBytes;
    type Output = Bound<'py, PyBytes>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Bound<'py, PyBytes>, Self::Error> {
        Ok(PyBytes::new(py, &self.0))
    }
}

/// A [`Resolved`] as Python reads it: `("value", producer, bytes)`,
/// `("failure", kind, message)` or `("list", [...])`.
pub(crate) struct PyResolved(pub(crate) Resolved);

impl<'py> IntoPyObject<'py> for PyResolved {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        resolved_object(py, self.0)
    }
}

fn resolved_object(py: Python<'_>, resolved: Resolved) -> PyResult<Bound<'_, PyAny>> {
    match resolved {
        Resolved::Value { producer, bytes } => {
            ("value", producer.as_str(), PyBytes::new(py, &bytes)).into_bound_py_any(py)
        }
        Resolved::Failure(failure) => ("failure", failure.kind, failure.message).into_bound_py_any(py),
        Resolved::List(items) => {
            let items = items
                .into_iter()
                .map(|item| resolved_object(py, item))
                .collect::<PyResult<Vec<_>>>()?;
            ("list", PyList::new(py, items)?).into_bound_py_any(py)
        }
    }
}

/// What a member's worker needs besides its claim, as Python reads it:
/// `(fields, input)`. `fields` are the `(name, value)` pairs bound into the
/// member's input. `input` is what it takes from before it as a [`Resolved`];
/// `("input", bytes)` when the member's whole input is bound as stored blobs
/// (its own serializer decodes it); `None` when the claim's `serialized_input`
/// is the whole input.
pub(crate) struct PyMemberInput(pub(crate) MemberInput);

impl<'py> IntoPyObject<'py> for PyMemberInput {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let MemberInput { step, input, own_input } = self.0;
        let fields = bound_fields(py, &step)?;
        let input = match (input, own_input) {
            (Some(resolved), _) => Some(resolved_object(py, resolved)?),
            (None, Some(own)) => Some(("input", PyBytes::new(py, &own)).into_bound_py_any(py)?),
            (None, None) => None,
        };
        PyTuple::new(py, [fields.into_any(), input.into_bound_py_any(py)?]).map(Bound::into_any)
    }
}

fn bound_fields<'py>(py: Python<'py>, step: &BoundStep) -> PyResult<Bound<'py, PyList>> {
    let fields = step
        .fields
        .iter()
        .map(|field| {
            let value = match &field.value {
                FieldValue::Int(value) => value.into_bound_py_any(py)?,
                FieldValue::Uint(value) => value.into_bound_py_any(py)?,
                FieldValue::Double(value) => value.into_bound_py_any(py)?,
                FieldValue::Bool(value) => value.into_bound_py_any(py)?,
                FieldValue::Text(value) => value.into_bound_py_any(py)?,
                FieldValue::Bytes(value) => PyBytes::new(py, value).into_any(),
            };
            (field.name.as_str(), value).into_bound_py_any(py)
        })
        .collect::<PyResult<Vec<_>>>()?;
    PyList::new(py, fields)
}

/// A composite's state as Python names it.
pub(crate) fn state_name(state: CompositeState) -> &'static str {
    match state {
        CompositeState::Running => "running",
        CompositeState::Succeeded => "succeeded",
        CompositeState::Failed => "failed",
        CompositeState::Cancelled => "cancelled",
    }
}
