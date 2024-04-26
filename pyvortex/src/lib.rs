use dtype::PyDType;
use log::debug;
use pyo3::prelude::*;
use vortex::encoding::VORTEX_ENCODINGS;
use vortex_schema::DType;
use vortex_schema::Signedness::{Signed, Unsigned};

use crate::array::*;

mod array;
mod dtype;
mod encode;
mod error;
mod vortex_arrow;

/// A Python module implemented in Rust.
#[pymodule]
fn _lib(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();

    debug!(
        "Discovered encodings: {:?}",
        VORTEX_ENCODINGS
            .iter()
            .map(|e| e.id().to_string())
            .collect::<Vec<String>>()
    );

    m.add_function(wrap_pyfunction!(encode::encode, m)?)?;
    // m.add_function(wrap_pyfunction!(compress::compress, m)?)?;

    m.add_class::<PyArray>()?;
    m.add_class::<PyBoolArray>()?;
    m.add_class::<PyBitPackedArray>()?;
    m.add_class::<PyChunkedArray>()?;
    m.add_class::<PyCompositeArray>()?;
    m.add_class::<PyConstantArray>()?;
    m.add_class::<PyDeltaArray>()?;
    m.add_class::<PyDictArray>()?;
    m.add_class::<PyFoRArray>()?;
    m.add_class::<PyPrimitiveArray>()?;
    m.add_class::<PyREEArray>()?;
    m.add_class::<PyRoaringBoolArray>()?;
    m.add_class::<PyRoaringIntArray>()?;
    m.add_class::<PySparseArray>()?;
    m.add_class::<PyStructArray>()?;
    m.add_class::<PyVarBinArray>()?;
    m.add_class::<PyVarBinViewArray>()?;
    m.add_class::<PyZigZagArray>()?;
    m.add_class::<PyALPArray>()?;

    m.add_class::<PyDType>()?;

    m.add_function(wrap_pyfunction!(dtype_int, m)?)?;
    m.add_function(wrap_pyfunction!(dtype_uint, m)?)?;
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
#[pyo3(signature = (width = None, nullable = false))]
fn dtype_int(py: Python<'_>, width: Option<u16>, nullable: bool) -> PyResult<Py<PyDType>> {
    PyDType::wrap(
        py,
        DType::Int(width.unwrap_or(64).into(), Signed, nullable.into()),
    )
}

#[pyfunction(name = "uint")]
#[pyo3(signature = (width = None, nullable = false))]
fn dtype_uint(py: Python<'_>, width: Option<u16>, nullable: bool) -> PyResult<Py<PyDType>> {
    PyDType::wrap(
        py,
        DType::Int(width.unwrap_or(64).into(), Unsigned, nullable.into()),
    )
}

#[pyfunction(name = "float")]
#[pyo3(signature = (width = None, nullable = false))]
fn dtype_float(py: Python<'_>, width: Option<i8>, nullable: bool) -> PyResult<Py<PyDType>> {
    PyDType::wrap(
        py,
        DType::Float(width.unwrap_or(64).into(), nullable.into()),
    )
}

#[pyfunction(name = "utf8")]
#[pyo3(signature = (nullable = false))]
fn dtype_utf8(py: Python<'_>, nullable: bool) -> PyResult<Py<PyDType>> {
    PyDType::wrap(py, DType::Utf8(nullable.into()))
}
