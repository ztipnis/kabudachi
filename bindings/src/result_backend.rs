//! Result backends as Python hands them to a runtime, the mirroring of
//! certified results into them, and reading results back for `resume`.
//!
//! A backend call blocks, and a Python backend's runs user code, so every
//! call is made on a blocking thread, never on the runtime's own threads,
//! holds no lock, and takes the GIL only to log. A backend failure is logged
//! and becomes a persistence state; it never reaches a run's outcome.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::blob::{BlobKey, BlobOwner};
use kabudachi_core::manifest::{Failure, Manifest};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskId, TaskRunId};
use kabudachi_core::protocol::messages::TaskRecord;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::result_backend::{
    PersistenceState, ReadError, ResultBackend, ResultBounds, ResultEntry, ResultTree, TaskStanding,
    read_composite, read_task_value, store_unless_complete, task_entries, task_standing,
};
use kabudachi_core::scheduler::composite::{CompositeSource, Resolved, results};
use kabudachi_core::time::{RealClock, WallTime};
use kabudachi_py_result_backend::PythonResultBackend;
use kabudachi_redis_authority::RedisAuthorityConfig;
use kabudachi_redis_result_backend::RedisResultBackend;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

use crate::composite::PyResolved;

/// A Redis or Valkey server as a result backend.
#[pyclass(name = "RedisResultBackend", frozen)]
pub struct PyRedisResultBackend {
    backend: Arc<dyn ResultBackend>,
}

#[pymethods]
impl PyRedisResultBackend {
    /// A backend on the server at `url` (`redis://` or `rediss://`, the
    /// database as its path or `database`), keeping its keys under
    /// `key_prefix` (`kabudachi:` unless given). Connects at its first use.
    /// Raises `ValueError` for a URL it cannot use, or a cluster.
    #[new]
    #[pyo3(signature = (url, *, key_prefix = None, database = 0))]
    fn new(url: String, key_prefix: Option<String>, database: u16) -> PyResult<Self> {
        let mut config = RedisAuthorityConfig::new(vec![url]);
        if let Some(prefix) = key_prefix {
            config.key_prefix = prefix;
        }
        config.database = database;
        let backend = RedisResultBackend::connect(config)
            .map_err(|error| PyValueError::new_err(format!("the result backend cannot be used: {error}")))?;
        Ok(PyRedisResultBackend { backend: Arc::new(backend) })
    }
}

/// A Python object with async `store` and `load` as a result backend.
#[pyclass(name = "PythonResultBackend", frozen)]
pub struct PyPythonResultBackend {
    backend: Arc<dyn ResultBackend>,
}

#[pymethods]
impl PyPythonResultBackend {
    /// `backend`'s coroutines run on `event_loop`, the loop the run's
    /// session runs on.
    #[new]
    fn new(backend: Py<PyAny>, event_loop: Py<PyAny>) -> Self {
        PyPythonResultBackend { backend: Arc::new(PythonResultBackend::new(backend, event_loop)) }
    }
}

/// The backend a runtime was handed, if any. Raises `TypeError` for anything
/// but the two backend classes.
pub(crate) fn backend_of(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Arc<dyn ResultBackend>>> {
    let Some(value) = value.filter(|value| !value.is_none()) else {
        return Ok(None);
    };
    if let Ok(redis) = value.cast::<PyRedisResultBackend>() {
        return Ok(Some(Arc::clone(&redis.get().backend)));
    }
    if let Ok(python) = value.cast::<PyPythonResultBackend>() {
        return Ok(Some(Arc::clone(&python.get().backend)));
    }
    Err(PyTypeError::new_err(
        "result_backend must be a RedisResultBackend or a PythonResultBackend",
    ))
}

/// What a runner keeps of a run it will mirror once the run is certified.
pub(crate) struct Mirror {
    pub(crate) task: TaskId,
    pub(crate) run: TaskRunId,
    pub(crate) digest: Digest,
    pub(crate) payload: Vec<u8>,
    /// The task's own retention, else the runner's `result_ttl`.
    pub(crate) keep_for: StdDuration,
}

impl Mirror {
    /// The entries that mirror this run's value, expiring `keep_for` after now.
    pub(crate) fn entries(&self) -> Vec<ResultEntry> {
        task_entries(
            &ResultBounds::DEFAULT,
            &self.task,
            &self.run,
            &self.digest,
            &self.payload,
            expiring_in(self.keep_for),
        )
    }
}

/// How long a result is kept: the task's own retention when it chose a
/// positive one, else the runtime's.
pub(crate) fn keep_for(task_ttl_ms: Option<u64>, runtime_ttl_ms: u64) -> StdDuration {
    StdDuration::from_millis(task_ttl_ms.filter(|ttl| *ttl > 0).unwrap_or(runtime_ttl_ms))
}

/// The wall-clock moment `keep_for` from now.
pub(crate) fn expiring_in(keep_for: StdDuration) -> WallTime {
    let millis = u64::try_from(keep_for.as_millis()).unwrap_or(u64::MAX);
    WallTime::from_unix_millis(WallTime::now(&RealClock::new()).as_unix_millis().saturating_add(millis))
}

/// The persistence a run's completion reports: `Pending` when there is a
/// backend, a result of the task's own to mirror that matches the digest the
/// run is certified under, and the task keeps its result; a composite's
/// member never mirrors.
pub(crate) fn persistence_for(
    backend: bool,
    result: Option<(&[u8], &Digest)>,
    member: bool,
    result_ttl_ms: Option<u64>,
) -> PersistenceState {
    let matching = result.is_some_and(|(payload, digest)| &Digest::blake3(payload) == digest);
    if backend && matching && !member && result_ttl_ms != Some(0) {
        PersistenceState::Pending
    } else {
        PersistenceState::Disabled
    }
}

/// Stores the entries `build` makes (parts first, head last) on a blocking
/// thread unless the backend already holds them complete, and says how that
/// went. A failure is logged, never raised.
pub(crate) async fn mirror(
    backend: Arc<dyn ResultBackend>,
    build: impl FnOnce() -> Vec<ResultEntry> + Send + 'static,
) -> PersistenceState {
    let stored = tokio::task::spawn_blocking(move || {
        let entries = build();
        let key = entries.last().map(|head| head.key.as_text()).unwrap_or_default();
        store_unless_complete(&*backend, &entries).map_err(|error| (key, error))
    })
    .await;
    match stored {
        Ok(Ok(_)) => PersistenceState::Stored,
        Ok(Err((key, error))) => {
            log_failure("store", &key, &error.to_string());
            PersistenceState::Failed
        }
        Err(_) => {
            log_failure("store", "a result", "the backend panicked");
            PersistenceState::Failed
        }
    }
}

/// `results`, a tree from Python (`("value", producer, serializer, bytes)`,
/// `("failure", kind, message, task_id)` or `("list", [...])`), as a
/// `ResultTree`. Raises `TypeError` for any other shape.
pub(crate) fn tree_from_py(results: &Bound<'_, PyAny>) -> PyResult<ResultTree> {
    let malformed = || PyTypeError::new_err("a result tree is a value, failure or list tuple");
    let tuple = results.cast::<PyTuple>().map_err(|_| malformed())?;
    let tag: String = tuple.get_item(0).and_then(|tag| tag.extract()).map_err(|_| malformed())?;
    match (tag.as_str(), tuple.len()) {
        ("value", 4) => {
            let bytes = tuple.get_item(3)?;
            let bytes = bytes.cast::<PyBytes>().map_err(|_| malformed())?;
            Ok(ResultTree::Value {
                producer: TaskDefinitionId::new(tuple.get_item(1)?.extract::<String>()?),
                serializer: tuple.get_item(2)?.extract()?,
                bytes: bytes.as_bytes().to_vec(),
            })
        }
        ("failure", 4) => {
            let failure = Failure::new(tuple.get_item(1)?.extract::<String>()?, tuple.get_item(2)?.extract::<String>()?);
            Ok(ResultTree::Failure(match tuple.get_item(3)?.extract::<Option<String>>()? {
                Some(task) => failure.of_task(TaskId::new(task)),
                None => failure,
            }))
        }
        ("list", 2) => {
            let items = tuple.get_item(1)?;
            let items = items.cast::<PyList>().map_err(|_| malformed())?;
            items
                .iter()
                .map(|item| tree_from_py(&item))
                .collect::<PyResult<Vec<_>>>()
                .map(ResultTree::List)
        }
        _ => Err(malformed()),
    }
}

/// `tree` as Python's tuple form, the inverse of [`tree_from_py`].
pub(crate) fn tree_to_py(py: Python<'_>, tree: ResultTree) -> PyResult<Bound<'_, PyAny>> {
    match tree {
        ResultTree::Value { producer, serializer, bytes } => {
            ("value", producer.as_str(), serializer, PyBytes::new(py, &bytes)).into_bound_py_any(py)
        }
        ResultTree::Failure(failure) => {
            let task = failure.task.map(|task| task.as_str().to_owned());
            ("failure", failure.kind, failure.message, task).into_bound_py_any(py)
        }
        ResultTree::List(items) => {
            let items = items
                .into_iter()
                .map(|item| tree_to_py(py, item))
                .collect::<PyResult<Vec<_>>>()?;
            ("list", PyList::new(py, items)?).into_bound_py_any(py)
        }
    }
}

/// Logs a backend failure on the Python logger `kabudachi.result_backend`,
/// never with a payload. Call it holding no lock: it takes the GIL.
pub(crate) fn log_failure(action: &str, key: &str, error: &str) {
    Python::attach(|py| {
        let logged = py
            .import("logging")
            .and_then(|logging| logging.call_method1("getLogger", ("kabudachi.result_backend",)))
            .and_then(|logger| logger.call_method1("warning", ("the result backend failed to %s %s: %s", action, key, error)));
        if let Err(error) = logged {
            error.print(py);
        }
    });
}

/// How a persistence state is named in Python.
pub(crate) fn persistence_name(state: PersistenceState) -> &'static str {
    match state {
        PersistenceState::Disabled => "disabled",
        PersistenceState::Pending => "pending",
        PersistenceState::Stored => "stored",
        PersistenceState::Failed => "failed",
    }
}

/// A run state as `Resumed.state` names it.
fn run_state_name(state: generated::TaskRunState) -> &'static str {
    use generated::TaskRunState as State;
    match state {
        State::Scheduled | State::Queued | State::Claimed | State::Running => "unfinished",
        State::Succeeded => "succeeded",
        State::Failed => "failed",
        State::Expired => "expired",
        State::Superseded => "superseded",
        State::Cancelled => "cancelled",
        State::Lost => "lost",
        State::Orphaned => "orphaned",
        State::Unspecified => "unknown",
    }
}

/// Where `resume` reads: the shard's records, and the backend.
pub(crate) enum Resumption {
    /// The shard holds no record of it.
    Unknown,
    Unfinished,
    /// It ended without a value.
    Ended {
        state: generated::TaskRunState,
        failure_kind: String,
        failure_message: String,
    },
    /// A task's value, read from the backend and checked against its record.
    Value {
        definition: Option<TaskDefinitionId>,
        payload: Vec<u8>,
        persistence: Option<PersistenceState>,
    },
    /// A composite's results, read from the backend and checked.
    Results { results: ResultTree },
    /// A finished composite the backend does not hold whole, read from the
    /// shard: Python adds serializer names, mirrors and decodes it.
    ShardResults { resolved: Resolved },
    /// It succeeded but no value can be had now.
    Unavailable {
        reason: String,
        persistence: Option<PersistenceState>,
    },
}

/// What a read of the backend ended in, for a value or a composite.
enum Read<T> {
    Found(T),
    Missing(String),
}

/// Loads from `backend` on a blocking thread, logging a failure of the
/// backend itself (never a missing or mismatching entry, which is the answer).
async fn read_backend<T: Send + 'static>(
    backend: Option<Arc<dyn ResultBackend>>,
    key: String,
    read: impl FnOnce(&dyn ResultBackend) -> Result<Option<T>, ReadError> + Send + 'static,
) -> Read<T> {
    let Some(backend) = backend else {
        return Read::Missing("no result backend is configured".to_owned());
    };
    let read = tokio::task::spawn_blocking(move || read(&*backend)).await;
    match read {
        Ok(Ok(Some(found))) => Read::Found(found),
        Ok(Ok(None)) => Read::Missing("the result backend has no entry for it".to_owned()),
        Ok(Err(ReadError::Incomplete)) => Read::Missing("the stored result is incomplete".to_owned()),
        Ok(Err(ReadError::Mismatch(mismatch))) => {
            Read::Missing(format!("the stored result does not match the record ({mismatch})"))
        }
        Ok(Err(ReadError::Backend(error))) => {
            log_failure("load", &key, &error.to_string());
            Read::Missing(format!("the result backend failed: {error}"))
        }
        Err(_) => {
            log_failure("load", &key, "the backend panicked");
            Read::Missing("the result backend failed".to_owned())
        }
    }
}

/// How `task` stands and, if it succeeded, its value: from `backend`, checked
/// against the record, or else from the blobs its worker stored in the shard
/// when the record names them, which are then mirrored to `backend` again
/// (never over an entry it holds complete) for `runtime_ttl_ms`, or the
/// task's own retention. A task that returned a step resumes as its
/// composite. Returns the composite as well when it did.
pub(crate) async fn resume_task(
    task: TaskId,
    record: Option<TaskRecord>,
    source: &impl CompositeSource,
    backend: Option<Arc<dyn ResultBackend>>,
    runtime_ttl_ms: u64,
) -> (Option<CompositeId>, Resumption) {
    match task_standing(record.as_ref()) {
        TaskStanding::Unknown => (None, Resumption::Unknown),
        TaskStanding::Unfinished => (None, Resumption::Unfinished),
        TaskStanding::Ended { state, failure_kind } => {
            let failure_message = record
                .as_ref()
                .and_then(|record| record.runs.last())
                .map(|run| run.failure_message.clone())
                .unwrap_or_default();
            (None, Resumption::Ended { state, failure_kind, failure_message })
        }
        TaskStanding::Continued(composite) => {
            let resumption = resume_composite(composite.clone(), source, backend).await;
            (Some(composite), resumption)
        }
        TaskStanding::Succeeded { run, digest, persistence, result_manifest } => {
            let key = format!("task/{}", task.as_str());
            let task_record = record.as_ref().and_then(|record| record.task.as_ref());
            let definition = task_record.map(|task| task.task_definition_id());
            let task_ttl_ms = task_record.and_then(|task| task.result_ttl_ms);
            let owner = task.clone();
            let (read_run, read_digest) = (run.clone(), digest.clone());
            let read = read_backend(backend.clone(), key, move |backend| {
                read_task_value(backend, &owner, &read_run, &read_digest)
            })
            .await;
            let resumption = match read {
                Read::Found(payload) => Resumption::Value { definition, payload, persistence },
                Read::Missing(reason) => {
                    let shard = match &result_manifest {
                        Some(manifest) => shard_value(source, &task, manifest, &digest).await,
                        None => None,
                    };
                    match shard {
                        Some(payload) => {
                            // A result kept for no time is not put back.
                            if let (Some(backend), true) = (backend, task_ttl_ms != Some(0)) {
                                let mirror_back = Mirror {
                                    task,
                                    run,
                                    digest,
                                    payload: payload.clone(),
                                    keep_for: keep_for(task_ttl_ms, runtime_ttl_ms),
                                };
                                tokio::spawn(async move {
                                    mirror(backend, move || mirror_back.entries()).await;
                                });
                            }
                            Resumption::Value { definition, payload, persistence }
                        }
                        None => Resumption::Unavailable { reason, persistence },
                    }
                }
            };
            (None, resumption)
        }
    }
}

/// The value of `task`, read from the blobs its worker stored in the shard:
/// the blob named by `manifest_digest` holds the manifest of the chunks. `None`
/// unless every blob is found and the joined bytes are the result `digest`
/// certifies.
async fn shard_value(
    source: &impl CompositeSource,
    task: &TaskId,
    manifest_digest: &Digest,
    digest: &Digest,
) -> Option<Vec<u8>> {
    let owner = BlobOwner::Task(task.clone());
    let encoded = source.blob(&owner, &BlobKey::from(manifest_digest)).await?;
    let manifest = Manifest::decode(&encoded).ok()?;
    manifest.check_cut_payload().ok()?;
    let payload = source.payload(&owner, &manifest).await.ok()?;
    (&Digest::blake3(&payload) == digest).then_some(payload)
}

/// How composite `id` stands and, once it succeeded, its results: from
/// `backend`, reassembled and checked, or else from the shard while it still
/// holds them.
pub(crate) async fn resume_composite(
    id: CompositeId,
    source: &impl CompositeSource,
    backend: Option<Arc<dyn ResultBackend>>,
) -> Resumption {
    let record = source.composite(&id).await;
    if let Some(record) = &record {
        match generated::CompositeState::try_from(record.state) {
            Ok(generated::CompositeState::Succeeded) => {}
            Ok(state @ (generated::CompositeState::Failed | generated::CompositeState::Cancelled)) => {
                let failure = record.failure.clone().unwrap_or_default();
                let state = if state == generated::CompositeState::Cancelled {
                    generated::TaskRunState::Cancelled
                } else {
                    generated::TaskRunState::Failed
                };
                return Resumption::Ended {
                    state,
                    failure_kind: failure.kind,
                    failure_message: failure.message,
                };
            }
            _ => return Resumption::Unfinished,
        }
    }
    let owner = id.clone();
    let read = read_backend(backend, format!("composite/{}", id.as_str()), move |backend| {
        read_composite(backend, &owner)
    })
    .await;
    let reason = match read {
        Read::Found(results) => return Resumption::Results { results },
        Read::Missing(reason) => reason,
    };
    if record.is_none() {
        return Resumption::Unknown;
    }
    match results(source, &id).await {
        Ok(resolved) => Resumption::ShardResults { resolved },
        Err(error) => Resumption::Unavailable {
            reason: format!("{reason}, and the shard cannot give it: {error}"),
            persistence: None,
        },
    }
}

/// What `resume_task` and `resume_composite` resolve to.
#[pyclass(name = "Resumed", frozen)]
pub struct PyResumed {
    /// `"unknown"`, `"unfinished"`, `"succeeded"`, `"failed"`, `"expired"`,
    /// `"superseded"`, `"cancelled"`, `"lost"` or `"orphaned"`.
    #[pyo3(get)]
    state: &'static str,
    /// How mirroring the task's result went: `"disabled"`, `"pending"`,
    /// `"stored"` or `"failed"`; `None` if its record does not say.
    #[pyo3(get)]
    persistence: Option<&'static str>,
    #[pyo3(get)]
    failure_kind: String,
    #[pyo3(get)]
    failure_message: String,
    /// The definition that produced a task's value, whose serializer decodes it.
    #[pyo3(get)]
    definition_id: Option<String>,
    payload: Option<Vec<u8>>,
    results: Option<Py<PyAny>>,
    resolved: Option<Py<PyAny>>,
    /// Whether `resolved` is a finished composite read from the shard, which
    /// the caller mirrors to the backend.
    #[pyo3(get)]
    from_shard: bool,
    /// The composite a task that returned a step resumed as, or the one asked for.
    #[pyo3(get)]
    composite_id: Option<String>,
    /// Why a succeeded task's or composite's value cannot be had; `None` otherwise.
    #[pyo3(get)]
    unavailable: Option<String>,
}

#[pymethods]
impl PyResumed {
    /// A task's value, checked against its record.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.payload.as_deref().map(|payload| PyBytes::new(py, payload))
    }

    /// A composite's results from the backend: `("value", producer,
    /// serializer, bytes)`, `("failure", kind, message, task_id)` or `("list",
    /// [...])`.
    #[getter]
    fn results(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.results.as_ref().map(|results| results.clone_ref(py))
    }

    /// A composite's results from the shard, as `composite_results` has them.
    #[getter]
    fn resolved(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.resolved.as_ref().map(|resolved| resolved.clone_ref(py))
    }
}

impl PyResumed {
    fn of(state: &'static str) -> Self {
        PyResumed {
            state,
            persistence: None,
            failure_kind: String::new(),
            failure_message: String::new(),
            definition_id: None,
            payload: None,
            results: None,
            resolved: None,
            from_shard: false,
            composite_id: None,
            unavailable: None,
        }
    }
}

/// A [`Resumption`] on its way to Python as a `Resumed`.
pub(crate) struct ResumedValue(pub(crate) Option<CompositeId>, pub(crate) Resumption);

impl<'py> IntoPyObject<'py> for ResumedValue {
    type Target = PyResumed;
    type Output = Bound<'py, PyResumed>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyResumed>> {
        let ResumedValue(composite, resumption) = self;
        let mut resumed = match resumption {
            Resumption::Unknown => PyResumed::of("unknown"),
            Resumption::Unfinished => PyResumed::of("unfinished"),
            Resumption::Ended { state, failure_kind, failure_message } => PyResumed {
                failure_kind,
                failure_message,
                ..PyResumed::of(run_state_name(state))
            },
            Resumption::Value { definition, payload, persistence } => PyResumed {
                definition_id: definition.map(|definition| definition.as_str().to_owned()),
                payload: Some(payload),
                persistence: persistence.map(persistence_name),
                ..PyResumed::of("succeeded")
            },
            Resumption::Results { results } => PyResumed {
                results: Some(tree_to_py(py, results)?.unbind()),
                ..PyResumed::of("succeeded")
            },
            Resumption::ShardResults { resolved } => PyResumed {
                resolved: Some(PyResolved(resolved).into_pyobject(py)?.unbind()),
                from_shard: true,
                ..PyResumed::of("succeeded")
            },
            Resumption::Unavailable { reason, persistence } => PyResumed {
                unavailable: Some(reason),
                persistence: persistence.map(persistence_name),
                ..PyResumed::of("succeeded")
            },
        };
        resumed.composite_id = composite.map(|composite| composite.as_str().to_owned());
        Bound::new(py, resumed)
    }
}
