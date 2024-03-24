use pyo3::types::PyType;
use pyo3::{pyclass, pyfunction, pymethods, Py, PyResult, Python};
use std::sync::Arc;
use vortex::array::ENCODINGS;

use vortex::compress::{CompressConfig, CompressCtx};

use crate::array::PyArray;
use crate::error::PyVortexError;

#[derive(Clone)]
#[pyclass(name = "CompressConfig", module = "vortex")]
pub struct PyCompressConfig {
    inner: CompressConfig,
}

#[pymethods]
impl PyCompressConfig {
    #[classmethod]
    pub fn default(cls: &PyType) -> PyResult<Py<PyCompressConfig>> {
        Py::new(cls.py(), <Self as Default>::default())
    }
}

impl Default for PyCompressConfig {
    fn default() -> Self {
        Self {
            inner: CompressConfig::new().with_enabled(ENCODINGS.iter().cloned()),
        }
    }
}

#[pyfunction]
#[pyo3(signature = (arr, opts = None))]
pub fn compress(
    py: Python<'_>,
    arr: &PyArray,
    opts: Option<PyCompressConfig>,
) -> PyResult<Py<PyArray>> {
    let compress_opts = opts.unwrap_or_default().inner;
    let ctx = CompressCtx::new(Arc::new(compress_opts));
    let compressed = py
        .allow_threads(|| ctx.compress(arr.unwrap(), None))
        .map_err(PyVortexError::map_err)?;
    PyArray::wrap(py, compressed)
}
