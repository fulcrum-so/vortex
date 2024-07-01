use dtype::PyDType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use vortex_dtype::{DType, PType};

use crate::array::*;

mod array;
mod dtype;
mod encode;
mod error;
mod vortex_arrow;

/// A Python module implemented in Rust.
#[pymodule]
fn _lib(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    pyo3_log::init();

    m.add_function(wrap_pyfunction!(encode::encode, m)?)?;
    // m.add_function(wrap_pyfunction!(compress::compress, m)?)?;

    m.add_class::<PyArray>()?;
    m.add_class::<PyBoolArray>()?;
    m.add_class::<PyBitPackedArray>()?;
    m.add_class::<PyChunkedArray>()?;
    m.add_class::<PyConstantArray>()?;
    m.add_class::<PyDeltaArray>()?;
    m.add_class::<PyDictArray>()?;
    m.add_class::<PyFoRArray>()?;
    m.add_class::<PyPrimitiveArray>()?;
    m.add_class::<PyRunEndArray>()?;
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
    let dtype = if let Some(width) = width {
        match width {
            8 => DType::Primitive(PType::I8, nullable.into()),
            16 => DType::Primitive(PType::I16, nullable.into()),
            32 => DType::Primitive(PType::I32, nullable.into()),
            64 => DType::Primitive(PType::I64, nullable.into()),
            _ => return Err(PyValueError::new_err("Invalid int width")),
        }
    } else {
        DType::Primitive(PType::I64, nullable.into())
    };
    PyDType::wrap(py, dtype)
}

#[pyfunction(name = "uint")]
#[pyo3(signature = (width = None, nullable = false))]
fn dtype_uint(py: Python<'_>, width: Option<u16>, nullable: bool) -> PyResult<Py<PyDType>> {
    let dtype = if let Some(width) = width {
        match width {
            8 => DType::Primitive(PType::U8, nullable.into()),
            16 => DType::Primitive(PType::U16, nullable.into()),
            32 => DType::Primitive(PType::U32, nullable.into()),
            64 => DType::Primitive(PType::U64, nullable.into()),
            _ => return Err(PyValueError::new_err("Invalid uint width")),
        }
    } else {
        DType::Primitive(PType::U64, nullable.into())
    };
    PyDType::wrap(py, dtype)
}

#[pyfunction(name = "float")]
#[pyo3(signature = (width = None, nullable = false))]
fn dtype_float(py: Python<'_>, width: Option<i8>, nullable: bool) -> PyResult<Py<PyDType>> {
    let dtype = if let Some(width) = width {
        match width {
            16 => DType::Primitive(PType::F16, nullable.into()),
            32 => DType::Primitive(PType::F32, nullable.into()),
            64 => DType::Primitive(PType::F64, nullable.into()),
            _ => return Err(PyValueError::new_err("Invalid float width")),
        }
    } else {
        DType::Primitive(PType::F64, nullable.into())
    };
    PyDType::wrap(py, dtype)
}

#[pyfunction(name = "utf8")]
#[pyo3(signature = (nullable = false))]
fn dtype_utf8(py: Python<'_>, nullable: bool) -> PyResult<Py<PyDType>> {
    PyDType::wrap(py, DType::Utf8(nullable.into()))
}
