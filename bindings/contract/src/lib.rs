//! A test-only extension that runs the result backend contract against
//! Python objects, through the adapter users' objects are wrapped in.

use std::panic::AssertUnwindSafe;

use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::result_backend::{ResultBackend, ResultKey};
use kabudachi_core::time::{Duration, RealClock, WallTime};
use kabudachi_py_result_backend::PythonResultBackend;
use kabudachi_testkit::{BackendAdapter, PassTime, check_result_backend_contract};
use pyo3::exceptions::PyAssertionError;
use pyo3::prelude::*;

struct PythonObjects {
    make_backend: Py<PyAny>,
    event_loop: Py<PyAny>,
}

impl BackendAdapter for PythonObjects {
    type Backend = PythonResultBackend;

    fn fresh(&self) -> PythonResultBackend {
        Python::attach(|py| {
            let object = self
                .make_backend
                .call0(py)
                .expect("the factory makes a backend");
            PythonResultBackend::new(object, self.event_loop.clone_ref(py))
        })
    }

    fn now(&self) -> WallTime {
        WallTime::now(&RealClock::new())
    }
}

struct RealTime;

impl PassTime for RealTime {
    fn pass(&self, duration: Duration) {
        std::thread::sleep(std::time::Duration::from_millis(duration.as_ticks()));
    }
}

/// Runs the contract against a fresh `make_backend()` per clause, its
/// coroutines on `event_loop` (running on another thread). Raises
/// `AssertionError` naming the clause a backend broke.
#[pyfunction]
fn check_result_backend(
    py: Python<'_>,
    make_backend: Py<PyAny>,
    event_loop: Py<PyAny>,
) -> PyResult<()> {
    let adapter = PythonObjects {
        make_backend,
        event_loop,
    };
    py.detach(|| {
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            check_result_backend_contract(&adapter, &RealTime)
        }))
    })
    .map_err(|panic| {
        let message = panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
            .unwrap_or_else(|| "the contract failed".to_owned());
        PyAssertionError::new_err(message)
    })
}

/// What loading a key from `make_backend()` ends in, as text: the error the
/// adapter answers with (waiting at most `timeout_ms` for the object), or
/// `"stored nothing"` when the load answered. Called from any thread, the
/// loop's own included.
#[pyfunction]
fn load_error(py: Python<'_>, make_backend: Py<PyAny>, event_loop: Py<PyAny>, timeout_ms: u64) -> PyResult<String> {
    let object = make_backend.call0(py)?;
    let backend = PythonResultBackend::new(object, event_loop)
        .with_timeout(std::time::Duration::from_millis(timeout_ms));
    let key = ResultKey::Task(TaskId::new("task"));
    Ok(py.detach(|| match backend.load(&key) {
        Ok(_) => "stored nothing".to_owned(),
        Err(error) => error.to_string(),
    }))
}

#[pymodule]
fn _kabudachi_contract(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(check_result_backend, module)?)?;
    module.add_function(wrap_pyfunction!(load_error, module)?)
}
