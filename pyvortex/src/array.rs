use paste::paste;
use pyo3::prelude::*;
use vortex::array::bool::{Bool, BoolArray, BoolEncoding, OwnedBoolArray};
use vortex::array::chunked::{Chunked, ChunkedArray, ChunkedEncoding, OwnedChunkedArray};
use vortex::array::composite::{Composite, CompositeArray, CompositeEncoding, OwnedCompositeArray};
use vortex::array::constant::{Constant, ConstantArray, ConstantEncoding, OwnedConstantArray};
use vortex::array::primitive::{OwnedPrimitiveArray, Primitive, PrimitiveArray, PrimitiveEncoding};
use vortex::array::r#struct::{OwnedStructArray, Struct, StructArray, StructEncoding};
use vortex::array::sparse::{OwnedSparseArray, Sparse, SparseArray, SparseEncoding};
use vortex::array::varbin::{OwnedVarBinArray, VarBin, VarBinArray, VarBinEncoding};
use vortex::array::varbinview::{
    OwnedVarBinViewArray, VarBinView, VarBinViewArray, VarBinViewEncoding,
};
use vortex::compute::take::take;
use vortex::encoding::EncodingRef;
use vortex::ToStatic;
use vortex::{ArrayDType, ArrayData, IntoArray, OwnedArray};
use vortex::{ArrayDef, IntoArrayData};
use vortex_alp::{ALPArray, ALPEncoding, OwnedALPArray, ALP};
use vortex_dict::{Dict, DictArray, DictEncoding, OwnedDictArray};
use vortex_ree::{OwnedREEArray, REEArray, REEEncoding, REE};

use crate::dtype::PyDType;
use crate::error::PyVortexError;
use crate::vortex_arrow;

#[pyclass(name = "Array", module = "vortex", sequence, subclass)]
pub struct PyArray {
    inner: OwnedArray,
}

macro_rules! pyarray {
    ($E:ident, $T:ident, $TName:tt) => {
        paste! {
            #[pyclass(name = $TName, module = "vortex", extends = PyArray, sequence, subclass)]
            pub struct [<Py $T>] {
                inner: [<Owned $T>],
                #[allow(dead_code)]
                encoding: EncodingRef,
            }

           impl [<Py $T>] {
               pub fn wrap(py: Python<'_>, inner: [<Owned $T>]) -> PyResult<Py<Self>> {
                   let init = PyClassInitializer::from(PyArray { inner: inner.array().to_static() })
                        .add_subclass([<Py $T>] { inner, encoding: &$E });
                   Py::new(py, init)
               }

               pub fn unwrap(&self) -> &$T {
                   &self.inner
               }
           }
        }
    };
}

pyarray!(BoolEncoding, BoolArray, "BoolArray");
pyarray!(ChunkedEncoding, ChunkedArray, "ChunkedArray");
pyarray!(CompositeEncoding, CompositeArray, "CompositeArray");
pyarray!(ConstantEncoding, ConstantArray, "ConstantArray");
pyarray!(PrimitiveEncoding, PrimitiveArray, "PrimitiveArray");
pyarray!(SparseEncoding, SparseArray, "SparseArray");
pyarray!(StructEncoding, StructArray, "StructArray");
pyarray!(VarBinEncoding, VarBinArray, "VarBinArray");
pyarray!(VarBinViewEncoding, VarBinViewArray, "VarBinViewArray");

pyarray!(ALPEncoding, ALPArray, "ALPArray");
// pyarray!(BitPackedEncoding, BitPackedArray, "BitPackedArray");
// pyarray!(FoREncoding, FoRArray, "FoRArray");
// pyarray!(DeltaEncoding, DeltaArray, "DeltaArray");
pyarray!(DictEncoding, DictArray, "DictArray");
pyarray!(REEEncoding, REEArray, "REEArray");
// pyarray!(RoaringBoolEncoding, RoaringBoolArray, "RoaringBoolArray");
// pyarray!(RoaringIntEncoding, RoaringIntArray, "RoaringIntArray");
// pyarray!(ZigZagEncoding, ZigZagArray, "ZigZagArray");

impl PyArray {
    pub fn wrap(py: Python<'_>, inner: ArrayData) -> PyResult<Py<Self>> {
        // This is the one place where we'd want to have owned kind enum but there's no other place this is used
        match inner.encoding().id() {
            Bool::ID => PyBoolArray::wrap(
                py,
                OwnedBoolArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Chunked::ID => PyChunkedArray::wrap(
                py,
                OwnedChunkedArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Composite::ID => PyCompositeArray::wrap(
                py,
                OwnedCompositeArray::try_from(inner.into_array())
                    .map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Constant::ID => PyConstantArray::wrap(
                py,
                OwnedConstantArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Primitive::ID => PyPrimitiveArray::wrap(
                py,
                OwnedPrimitiveArray::try_from(inner.into_array())
                    .map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Sparse::ID => PySparseArray::wrap(
                py,
                OwnedSparseArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Struct::ID => PyStructArray::wrap(
                py,
                OwnedStructArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            VarBin::ID => PyVarBinArray::wrap(
                py,
                OwnedVarBinArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            VarBinView::ID => PyVarBinViewArray::wrap(
                py,
                OwnedVarBinViewArray::try_from(inner.into_array())
                    .map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Dict::ID => PyDictArray::wrap(
                py,
                OwnedDictArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            REE::ID => PyREEArray::wrap(
                py,
                OwnedREEArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            ALP::ID => PyALPArray::wrap(
                py,
                OwnedALPArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            _ => Py::new(
                py,
                Self {
                    inner: inner.into_array(),
                },
            ),
            // ArrayKind::Other(other) => match other.encoding().id() {
            //     // PyEnc chooses to expose certain encodings as first-class objects.
            //     // For the remainder, we should have a generic EncArray implementation that supports basic functions.
            //     ALPEncoding::ID => {
            //         PyALPArray::wrap(py, inner.into_any().downcast::<ALPArray>().unwrap())?
            //             .extract(py)
            //     }
            //     DeltaEncoding::ID => {
            //         PyDeltaArray::wrap(py, inner.into_any().downcast::<DeltaArray>().unwrap())?
            //             .extract(py)
            //     }
            //     DictEncoding::ID => {
            //         PyDictArray::wrap(py, inner.into_any().downcast::<DictArray>().unwrap())?
            //             .extract(py)
            //     }
            //     FoREncoding::ID => {
            //         PyFoRArray::wrap(py, inner.into_any().downcast::<FoRArray>().unwrap())?
            //             .extract(py)
            //     }
            //     BitPackedEncoding::ID => PyBitPackedArray::wrap(
            //         py,
            //         inner.into_any().downcast::<BitPackedArray>().unwrap(),
            //     )?
            //     .extract(py),
            //     REEEncoding::ID => {
            //         PyREEArray::wrap(py, inner.into_any().downcast::<REEArray>().unwrap())?
            //             .extract(py)
            //     }
            //     RoaringBoolEncoding::ID => PyRoaringBoolArray::wrap(
            //         py,
            //         inner.into_any().downcast::<RoaringBoolArray>().unwrap(),
            //     )?
            //     .extract(py),
            //     RoaringIntEncoding::ID => PyRoaringIntArray::wrap(
            //         py,
            //         inner.into_any().downcast::<RoaringIntArray>().unwrap(),
            //     )?
            //     .extract(py),
            //     ZigZagEncoding::ID => {
            //         PyZigZagArray::wrap(py, inner.into_any().downcast::<ZigZagArray>().unwrap())?
            //             .extract(py)
            //     }
            //     _ => Py::new(py, Self { inner }),
            //},
        }
    }

    pub fn unwrap(&self) -> &OwnedArray {
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

    fn take(&self, indices: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
        take(&self.inner, indices.unwrap())
            .map_err(PyVortexError::map_err)
            .and_then(|arr| PyArray::wrap(indices.py(), arr.into_array_data()))
    }
}
//
// #[pymethods]
// impl PyRoaringBoolArray {
//     #[staticmethod]
//     fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
//         RoaringBoolArray::encode(array.unwrap())
//             .map_err(PyVortexError::map_err)
//             .and_then(|zarray| PyArray::wrap(array.py(), zarray.into_array()))
//     }
// }
//
// #[pymethods]
// impl PyRoaringIntArray {
//     #[staticmethod]
//     fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
//         RoaringIntArray::encode(array.unwrap())
//             .map_err(PyVortexError::map_err)
//             .and_then(|zarray| PyArray::wrap(array.py(), zarray.into_array()))
//     }
// }
//
// #[pymethods]
// impl PyZigZagArray {
//     #[staticmethod]
//     fn encode(array: PyRef<'_, PyArray>) -> PyResult<Py<PyArray>> {
//         ZigZagArray::encode(array.unwrap())
//             .map_err(PyVortexError::map_err)
//             .and_then(|zarray| PyArray::wrap(array.py(), zarray))
//     }
// }
