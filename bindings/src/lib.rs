use pyo3::prelude::*;

mod bridge;
mod door;
mod election;
mod local_node;
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
    Ok(())
}
