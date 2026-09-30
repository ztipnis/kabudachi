use pyo3::prelude::*;

mod bridge;
mod door;
mod election;
mod errors;
mod local_node;
mod outcomes;
mod runtime;
mod timers;
mod work;

pub fn native_version() -> String {
    kabudachi_core::version().to_string()
}

#[pyfunction]
fn version() -> String {
    native_version()
}

#[pymodule]
fn _native(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_class::<runtime::NativeRuntime>()?;
    m.add_class::<work::PyClaim>()?;
    m.add_class::<work::PyCertification>()?;
    m.add_class::<work::PyEvent>()?;
    m.add_class::<outcomes::PyEventKind>()?;
    m.add_class::<outcomes::PyCancelOutcome>()?;
    m.add_class::<outcomes::PyRunState>()?;
    m.add("KabudachiError", m.py().get_type::<errors::KabudachiError>())?;
    m.add("BackpressureError", errors::backpressure_error_type(m.py())?)?;
    Ok(())
}
