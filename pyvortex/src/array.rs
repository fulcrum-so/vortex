use paste::paste;
use pyo3::prelude::*;
use vortex::array::bool::{Bool, BoolArray, BoolEncoding};
use vortex::array::chunked::{Chunked, ChunkedArray, ChunkedEncoding};
use vortex::array::constant::{Constant, ConstantArray, ConstantEncoding};
use vortex::array::primitive::{Primitive, PrimitiveArray, PrimitiveEncoding};
use vortex::array::sparse::{Sparse, SparseArray, SparseEncoding};
use vortex::array::struct_::{Struct, StructArray, StructEncoding};
use vortex::array::varbin::{VarBin, VarBinArray, VarBinEncoding};
use vortex::array::varbinview::{VarBinView, VarBinViewArray, VarBinViewEncoding};
use vortex::compute::take::take;
use vortex::encoding::EncodingRef;
use vortex::ToArray;
use vortex::{Array, ArrayDType, ArrayData, IntoArray};
use vortex::{ArrayDef, IntoArrayData};
use vortex_alp::{ALPArray, ALPEncoding, ALP};
use vortex_dict::{Dict, DictArray, DictEncoding};
use vortex_fastlanes::{
    BitPacked, BitPackedArray, BitPackedEncoding, Delta, DeltaArray, DeltaEncoding, FoR, FoRArray,
    FoREncoding,
};
use vortex_roaring::{
    RoaringBool, RoaringBoolArray, RoaringBoolEncoding, RoaringInt, RoaringIntArray,
    RoaringIntEncoding,
};
use vortex_runend::{RunEnd, RunEndArray, RunEndEncoding};
use vortex_zigzag::{ZigZag, ZigZagArray, ZigZagEncoding};

use crate::dtype::PyDType;
use crate::error::PyVortexError;
use crate::vortex_arrow;

#[pyclass(name = "Array", module = "vortex", sequence, subclass)]
pub struct PyArray {
    inner: Array,
}

macro_rules! pyarray {
    ($E:ident, $T:ident, $TName:tt) => {
        paste! {
            #[pyclass(name = $TName, module = "vortex", extends = PyArray, sequence, subclass)]
            pub struct [<Py $T>] {
                inner: $T,
                #[allow(dead_code)]
                encoding: EncodingRef,
            }

           impl [<Py $T>] {
               pub fn wrap(py: Python<'_>, inner: $T) -> PyResult<Py<Self>> {
                   let init = PyClassInitializer::from(PyArray { inner: inner.to_array().clone() })
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
pyarray!(ConstantEncoding, ConstantArray, "ConstantArray");
pyarray!(PrimitiveEncoding, PrimitiveArray, "PrimitiveArray");
pyarray!(SparseEncoding, SparseArray, "SparseArray");
pyarray!(StructEncoding, StructArray, "StructArray");
pyarray!(VarBinEncoding, VarBinArray, "VarBinArray");
pyarray!(VarBinViewEncoding, VarBinViewArray, "VarBinViewArray");

pyarray!(ALPEncoding, ALPArray, "ALPArray");
pyarray!(BitPackedEncoding, BitPackedArray, "BitPackedArray");
pyarray!(FoREncoding, FoRArray, "FoRArray");
pyarray!(DeltaEncoding, DeltaArray, "DeltaArray");
pyarray!(DictEncoding, DictArray, "DictArray");
pyarray!(RunEndEncoding, RunEndArray, "RunEndArray");
pyarray!(RoaringBoolEncoding, RoaringBoolArray, "RoaringBoolArray");
pyarray!(RoaringIntEncoding, RoaringIntArray, "RoaringIntArray");
pyarray!(ZigZagEncoding, ZigZagArray, "ZigZagArray");

impl PyArray {
    pub fn wrap(py: Python<'_>, inner: ArrayData) -> PyResult<Py<Self>> {
        // This is the one place where we'd want to have owned kind enum but there's no other place this is used
        match inner.encoding().id() {
            Bool::ID => PyBoolArray::wrap(
                py,
                BoolArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Chunked::ID => PyChunkedArray::wrap(
                py,
                ChunkedArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Constant::ID => PyConstantArray::wrap(
                py,
                ConstantArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Primitive::ID => PyPrimitiveArray::wrap(
                py,
                PrimitiveArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Sparse::ID => PySparseArray::wrap(
                py,
                SparseArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Struct::ID => PyStructArray::wrap(
                py,
                StructArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            VarBin::ID => PyVarBinArray::wrap(
                py,
                VarBinArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            VarBinView::ID => PyVarBinViewArray::wrap(
                py,
                VarBinViewArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Dict::ID => PyDictArray::wrap(
                py,
                DictArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            RunEnd::ID => PyRunEndArray::wrap(
                py,
                RunEndArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            Delta::ID => PyDeltaArray::wrap(
                py,
                DeltaArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            FoR::ID => PyFoRArray::wrap(
                py,
                FoRArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            BitPacked::ID => PyBitPackedArray::wrap(
                py,
                BitPackedArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),

            ALP::ID => PyALPArray::wrap(
                py,
                ALPArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            RoaringBool::ID => PyBitPackedArray::wrap(
                py,
                BitPackedArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            RoaringInt::ID => PyBitPackedArray::wrap(
                py,
                BitPackedArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
            )?
            .extract(py),
            ZigZag::ID => PyZigZagArray::wrap(
                py,
                ZigZagArray::try_from(inner.into_array()).map_err(PyVortexError::map_err)?,
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

    pub fn unwrap(&self) -> &Array {
        &self.inner
    }
}

#[pymethods]
impl PyArray {
    fn to_pyarrow(self_: PyRef<'_, Self>) -> PyResult<Bound<PyAny>> {
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

    fn take(&self, indices: PyRef<'_, Self>) -> PyResult<Py<Self>> {
        take(&self.inner, indices.unwrap())
            .map_err(PyVortexError::map_err)
            .and_then(|arr| Self::wrap(indices.py(), arr.into_array_data()))
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
