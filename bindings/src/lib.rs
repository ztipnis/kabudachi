use pyo3::prelude::*;

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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_version_matches_core() {
        assert_eq!(native_version(), kabudachi_core::version());
    }
}
