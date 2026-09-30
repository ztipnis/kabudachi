//! The exception classes `kabudachi._native` defines. `kabudachi.errors`
//! re-exports both and derives every other kabudachi error from
//! `KabudachiError`, so no Rust code imports a Python module to raise one.

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};

// The module argument becomes `__module__`, so it names the real import path
// (`module_name` in bindings/BUILD.bazel), not bare `_native`.
create_exception!(
    kabudachi._native,
    KabudachiError,
    PyException,
    "Base class of every exception kabudachi raises on purpose."
);

const BACKPRESSURE_DOC: &str = "A task was not submitted because the scheduler holds as much \
pending work as its hard memory limit allows. Nothing was queued; submit again once running \
tasks have finished.";

static BACKPRESSURE_ERROR: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// `BackpressureError(KabudachiError, RuntimeError)`, the ancestry the
/// pure-Python class had. `create_exception!` takes one base, so the class is
/// built once with `type(name, bases, namespace)`, as CPython's own
/// `PyErr_NewException` does for a tuple of bases.
pub fn backpressure_error_type(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    BACKPRESSURE_ERROR
        .get_or_try_init(py, || -> PyResult<Py<PyType>> {
            let bases = PyTuple::new(
                py,
                [
                    py.get_type::<KabudachiError>(),
                    py.get_type::<PyRuntimeError>(),
                ],
            )?;
            let namespace = PyDict::new(py);
            namespace.set_item("__module__", "kabudachi._native")?;
            namespace.set_item("__doc__", BACKPRESSURE_DOC)?;
            let class = py
                .get_type::<PyType>()
                .call1(("BackpressureError", bases, namespace))?;
            Ok(class.cast_into::<PyType>()?.unbind())
        })
        .map(|class| class.bind(py))
}

/// The error a submission past the hard memory limit raises. The class was
/// built when the module was imported, so building it cannot fail here.
pub fn backpressure_error(py: Python<'_>, message: String) -> PyErr {
    match backpressure_error_type(py) {
        Ok(class) => PyErr::from_type(class.clone(), message),
        Err(error) => error,
    }
}
