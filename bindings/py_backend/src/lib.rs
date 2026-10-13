//! A Python object as a [`ResultBackend`]: the adapter the `kabudachi`
//! extension wraps any object with async `store` and `load` in. It lives in
//! its own crate so the result backend contract can run against it from a
//! test-only extension.

use kabudachi_core::result_backend::{BackendError, ResultBackend, ResultEntry, ResultKey};
use kabudachi_core::time::{RealClock, WallTime};
use pyo3::exceptions::PyTimeoutError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

/// How long one call to a Python backend may take before it is cancelled.
pub const PY_BACKEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A Python object with `async def store(key: str, entry: bytes,
/// expires_at: datetime)` and `async def load(key: str) -> bytes | None`, as
/// a [`ResultBackend`]. Its coroutines run on `event_loop`, which must be
/// running on another thread than the caller's: a call made on the loop's
/// own thread would wait for itself until the timeout.
///
/// The object keeps bytes; the adapter encodes entries, and drops on load
/// any entry that has expired, so an object that never expires anything is a
/// complete backend.
pub struct PythonResultBackend {
    object: Py<PyAny>,
    event_loop: Py<PyAny>,
    clock: RealClock,
    timeout: std::time::Duration,
}

impl PythonResultBackend {
    pub fn new(object: Py<PyAny>, event_loop: Py<PyAny>) -> Self {
        PythonResultBackend {
            object,
            event_loop,
            clock: RealClock::new(),
            timeout: PY_BACKEND_TIMEOUT,
        }
    }

    /// Calls the coroutine method `method` with `args` on the object's
    /// event loop, and waits for it, the GIL released while it waits, for up
    /// to the timeout; past it the coroutine is cancelled.
    fn call(
        &self,
        method: &str,
        args: impl for<'py> FnOnce(Python<'py>) -> PyResult<Bound<'py, PyTuple>>,
    ) -> Result<Py<PyAny>, BackendError> {
        Python::attach(|py| {
            let coroutine = self
                .object
                .bind(py)
                .call_method1(method, args(py).map_err(|error| refused(py, &error))?)
                .map_err(|error| refused(py, &error))?;
            let future = py
                .import("asyncio")
                .and_then(|asyncio| {
                    asyncio.call_method1(
                        "run_coroutine_threadsafe",
                        (coroutine, self.event_loop.bind(py)),
                    )
                })
                .map_err(|error| BackendError::Unavailable(describe(py, &error)))?;
            // `result` waits on a lock, which lets go of the GIL while it waits.
            match future.call_method1("result", (self.timeout.as_secs_f64(),)) {
                Ok(value) => Ok(value.unbind()),
                Err(error) => {
                    let _ = future.call_method0("cancel");
                    if error.is_instance_of::<PyTimeoutError>(py) {
                        Err(BackendError::Unavailable(format!(
                            "no answer within {:?}",
                            self.timeout
                        )))
                    } else {
                        Err(refused(py, &error))
                    }
                }
            }
        })
    }
}

impl ResultBackend for PythonResultBackend {
    fn store(&self, entry: &ResultEntry) -> Result<(), BackendError> {
        if entry.has_expired(WallTime::now(&self.clock)) {
            return Ok(());
        }
        let key = entry.key.as_text();
        let bytes = entry.encode();
        let seconds = entry.expires_at.as_unix_millis() as f64 / 1000.0;
        self.call("store", |py| {
            let datetime = py.import("datetime")?;
            let utc = datetime.getattr("timezone")?.getattr("utc")?;
            let expires_at = datetime
                .getattr("datetime")?
                .call_method1("fromtimestamp", (seconds, utc))?;
            PyTuple::new(
                py,
                [
                    key.into_pyobject(py)?.into_any(),
                    PyBytes::new(py, &bytes).into_any(),
                    expires_at,
                ],
            )
        })
        .map(|_| ())
    }

    fn load(&self, key: &ResultKey) -> Result<Option<ResultEntry>, BackendError> {
        let key = key.as_text();
        let value = self.call("load", |py| {
            PyTuple::new(py, [key.into_pyobject(py)?.into_any()])
        })?;
        let bytes = Python::attach(|py| {
            let value = value.bind(py);
            if value.is_none() {
                return Ok(None);
            }
            value
                .cast::<PyBytes>()
                .map(|bytes| Some(bytes.as_bytes().to_vec()))
                .map_err(|_| BackendError::Corrupt)
        })?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let entry = ResultEntry::decode(&bytes)?;
        Ok((!entry.has_expired(WallTime::now(&self.clock))).then_some(entry))
    }
}

/// The error's type and message, never a traceback.
fn describe(py: Python<'_>, error: &PyErr) -> String {
    format!(
        "{}: {}",
        error
            .get_type(py)
            .name()
            .map(|name| name.to_string())
            .unwrap_or_default(),
        error.value(py)
    )
}

fn refused(py: Python<'_>, error: &PyErr) -> BackendError {
    BackendError::Refused(describe(py, error))
}
