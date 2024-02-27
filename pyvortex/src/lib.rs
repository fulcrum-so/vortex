use log::debug;
use pyo3::prelude::*;

use dtype::PyDType;
use vortex::dtype::DType;

use crate::array::*;
use crate::compress::PyCompressConfig;

mod array;
mod compress;
mod dtype;
mod vortex_arrow;
mod encode;
mod error;
mod serde;

/// A Python module implemented in Rust.
#[pymodule]
fn _lib(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();

    debug!(
        "Discovered encodings: {:?}",
        vortex::array::ENCODINGS
            .iter()
            .map(|e| e.id().to_string())
            .collect::<Vec<String>>()
    );

    m.add_function(wrap_pyfunction!(encode::encode, m)?)?;
    m.add_function(wrap_pyfunction!(compress::compress, m)?)?;
    m.add_function(wrap_pyfunction!(serde::write, m)?)?;
    m.add_function(wrap_pyfunction!(serde::read, m)?)?;

    m.add_class::<PyArray>()?;
    m.add_class::<PyBoolArray>()?;
    m.add_class::<PyChunkedArray>()?;
    m.add_class::<PyConstantArray>()?;
    m.add_class::<PyFFORArray>()?;
    m.add_class::<PyPrimitiveArray>()?;
    m.add_class::<PyREEArray>()?;
    m.add_class::<PySparseArray>()?;
    m.add_class::<PyStructArray>()?;
    m.add_class::<PyTypedArray>()?;
    m.add_class::<PyVarBinArray>()?;
    m.add_class::<PyVarBinViewArray>()?;

    m.add_class::<PyRoaringBoolArray>()?;
    m.add_class::<PyRoaringIntArray>()?;

    m.add_class::<PyZigZagArray>()?;

    m.add_class::<PyDType>()?;

    m.add_class::<PyCompressConfig>()?;

    m.add_function(wrap_pyfunction!(dtype_int, m)?)?;
    m.add_function(wrap_pyfunction!(dtype_float, m)?)?;
    m.add_function(wrap_pyfunction!(dtype_bool, m)?)?;
    m.add_function(wrap_pyfunction!(dtype_utf8, m)?)?;

    Ok(())
}

#[pyfunction(name = "bool")]
#[pyo3(signature = (nullable = false))]
fn dtype_bool(py: Python<'_>, nullable: bool) -> PyResult<Py<PyDType>> {
    PyDType::wrap(py, DType::Bool(nullable.into()))
}

#[pyfunction(name = "int")]
#[pyo3(signature = (width = None, signed = true, nullable = false))]
fn dtype_int(
    py: Python<'_>,
    width: Option<i8>,
    signed: bool,
    nullable: bool,
) -> PyResult<Py<PyDType>> {
    PyDType::wrap(
        py,
        DType::Int(width.unwrap_or(0).into(), signed.into(), nullable.into()),
    )
}

#[pyfunction(name = "float")]
#[pyo3(signature = (width = None, nullable = false))]
fn dtype_float(py: Python<'_>, width: Option<i8>, nullable: bool) -> PyResult<Py<PyDType>> {
    PyDType::wrap(py, DType::Float(width.unwrap_or(0).into(), nullable.into()))
}

#[pyfunction(name = "utf8")]
#[pyo3(signature = (nullable = false))]
fn dtype_utf8(py: Python<'_>, nullable: bool) -> PyResult<Py<PyDType>> {
    PyDType::wrap(py, DType::Utf8(nullable.into()))
}
