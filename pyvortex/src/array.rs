use paste::paste;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use vortex::array::bool::BoolArray;
use vortex::array::chunked::ChunkedArray;
use vortex::array::composite::CompositeArray;
use vortex::array::constant::ConstantArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::array::struct_::StructArray;
use vortex::array::varbin::VarBinArray;
use vortex::array::varbinview::VarBinViewArray;
use vortex::array::{Array, ArrayKind, ArrayRef};
use vortex_alp::{ALPArray, ALPEncoding};
use vortex_dict::{DictArray, DictEncoding};
use vortex_fastlanes::{BitPackedArray, BitPackedEncoding, FoRArray, FoREncoding};
use vortex_ree::{REEArray, REEEncoding};
use vortex_roaring::{RoaringBoolArray, RoaringBoolEncoding, RoaringIntArray, RoaringIntEncoding};
use vortex_zigzag::{ZigZagArray, ZigZagEncoding};

use crate::dtype::PyDType;
use crate::error::PyVortexError;
use crate::vortex_arrow;
use std::sync::Arc;
#[pyclass(name = "Array", module = "vortex", sequence, subclass)]
pub struct PyArray {
    inner: ArrayRef,
}

macro_rules! pyarray {
    ($T:ident, $TName:tt) => {
        paste! {
            #[pyclass(name = $TName, module = "vortex", extends = PyArray, sequence, subclass)]
            pub struct [<Py $T>] {
                inner: Arc<$T>,
            }

           impl [<Py $T>] {
               pub fn wrap(py: Python<'_>, inner: Arc<$T>) -> PyResult<Py<Self>> {
                   let init = PyClassInitializer::from(PyArray { inner: inner.clone() })
                        .add_subclass([<Py $T>] { inner });
                   Py::new(py, init)
               }

               pub fn unwrap(&self) -> &$T {
                   &self.inner
               }
           }
        }
    };
}

pyarray!(BoolArray, "BoolArray");
pyarray!(ChunkedArray, "ChunkedArray");
pyarray!(CompositeArray, "CompositeArray");
pyarray!(ConstantArray, "ConstantArray");
pyarray!(PrimitiveArray, "PrimitiveArray");
pyarray!(SparseArray, "SparseArray");
pyarray!(StructArray, "StructArray");
pyarray!(VarBinArray, "VarBinArray");
pyarray!(VarBinViewArray, "VarBinViewArray");

pyarray!(ALPArray, "ALPArray");
pyarray!(BitPackedArray, "BitPackedArray");
pyarray!(FoRArray, "FoRArray");
pyarray!(DictArray, "DictArray");
pyarray!(REEArray, "REEArray");
pyarray!(RoaringBoolArray, "RoaringBoolArray");
pyarray!(RoaringIntArray, "RoaringIntArray");
pyarray!(ZigZagArray, "ZigZagArray");

impl PyArray {
    pub fn wrap(py: Python<'_>, inner: ArrayRef) -> PyResult<Py<Self>> {
        // This is the one place where we'd want to have owned kind enum but there's no other place this is used
        match ArrayKind::from(inner.as_ref()) {
            ArrayKind::Bool(_) => {
                PyBoolArray::wrap(py, inner.into_any().downcast::<BoolArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Chunked(_) => {
                PyChunkedArray::wrap(py, inner.into_any().downcast::<ChunkedArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Composite(_) => {
                PyCompositeArray::wrap(py, inner.into_any().downcast::<CompositeArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Constant(_) => {
                PyConstantArray::wrap(py, inner.into_any().downcast::<ConstantArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Primitive(_) => {
                PyPrimitiveArray::wrap(py, inner.into_any().downcast::<PrimitiveArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Sparse(_) => {
                PySparseArray::wrap(py, inner.into_any().downcast::<SparseArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::Struct(_) => {
                PyStructArray::wrap(py, inner.into_any().downcast::<StructArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::VarBin(_) => {
                PyVarBinArray::wrap(py, inner.into_any().downcast::<VarBinArray>().unwrap())?
                    .extract(py)
            }
            ArrayKind::VarBinView(_) => PyVarBinViewArray::wrap(
                py,
                inner.into_any().downcast::<VarBinViewArray>().unwrap(),
            )?
            .extract(py),
            ArrayKind::Other(other) => match *other.encoding().id() {
                // PyEnc chooses to expose certain encodings as first-class objects.
                // For the remainder, we should have a generic EncArray implementation that supports basic functions.
                ALPEncoding::ID => {
                    PyALPArray::wrap(py, inner.into_any().downcast::<ALPArray>().unwrap())?
                        .extract(py)
                }
                DictEncoding::ID => {
                    PyDictArray::wrap(py, inner.into_any().downcast::<DictArray>().unwrap())?
                        .extract(py)
                }
                FoREncoding::ID => {
                    PyFoRArray::wrap(py, inner.into_any().downcast::<FoRArray>().unwrap())?
                        .extract(py)
                }
                BitPackedEncoding::ID => PyBitPackedArray::wrap(
                    py,
                    inner.into_any().downcast::<BitPackedArray>().unwrap(),
                )?
                .extract(py),
                REEEncoding::ID => {
                    PyREEArray::wrap(py, inner.into_any().downcast::<REEArray>().unwrap())?
                        .extract(py)
                }
                RoaringBoolEncoding::ID => PyRoaringBoolArray::wrap(
                    py,
                    inner.into_any().downcast::<RoaringBoolArray>().unwrap(),
                )?
                .extract(py),
                RoaringIntEncoding::ID => PyRoaringIntArray::wrap(
                    py,
                    inner.into_any().downcast::<RoaringIntArray>().unwrap(),
                )?
                .extract(py),
                ZigZagEncoding::ID => {
                    PyZigZagArray::wrap(py, inner.into_any().downcast::<ZigZagArray>().unwrap())?
                        .extract(py)
                }
                _ => Err(PyValueError::new_err(format!(
                    "Cannot convert {:?} to enc array",
                    inner
                ))),
            },
        }
    }

    pub fn unwrap(&self) -> &ArrayRef {
        &self.inner
    }
}

#[pymethods]
impl PyArray {
    fn to_pyarrow(self_: PyRef<'_, Self>) -> PyResult<&PyAny> {
        vortex_arrow::export_array(self_.py(), &self_.inner)
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }

    #[getter]
    fn encoding(&self) -> String {
        self.inner.encoding().id().to_string()
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.inner.nbytes()
    }

    #[getter]
    fn dtype(self_: PyRef<Self>) -> PyResult<Py<PyDType>> {
        PyDType::wrap(self_.py(), self_.inner.dtype().clone())
    }
}

#[pymethods]
impl PyRoaringBoolArray {
    #[staticmethod]
    fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
        RoaringBoolArray::encode(array.unwrap())
            .map_err(PyVortexError::map_err)
            .and_then(|zarray| PyArray::wrap(array.py(), zarray.into_array()))
    }
}

#[pymethods]
impl PyRoaringIntArray {
    #[staticmethod]
    fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
        RoaringIntArray::encode(array.unwrap())
            .map_err(PyVortexError::map_err)
            .and_then(|zarray| PyArray::wrap(array.py(), zarray.into_array()))
    }
}

#[pymethods]
impl PyZigZagArray {
    #[staticmethod]
    fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
        ZigZagArray::encode(array.unwrap())
            .map_err(PyVortexError::map_err)
            .and_then(|zarray| PyArray::wrap(array.py(), zarray))
    }
}
