use arrow::array::{make_array, ArrayData};
use arrow::datatypes::{DataType, Field};
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatchReader;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::array::PyArray;
use crate::vortex_arrow::map_arrow_err;
use vortex::array::chunked::ChunkedArray;
use vortex::array::IntoArray;
use vortex::array::{Array, ArrayRef};
use vortex::arrow::FromArrowType;
use vortex::encode::FromArrowArray;
use vortex_schema::DType;

/// The main entry point for creating enc arrays from other Python objects.
///
#[pyfunction]
pub fn encode(obj: &PyAny) -> PyResult<Py<PyArray>> {
    let pa = obj.py().import("pyarrow")?;
    let pa_array = pa.getattr("Array")?;
    let chunked_array = pa.getattr("ChunkedArray")?;
    let table = pa.getattr("Table")?;

    if obj.is_instance(pa_array)? {
        let arrow_array = ArrayData::from_pyarrow(obj).map(make_array)?;
        let enc_array = ArrayRef::from_arrow(arrow_array, false);
        PyArray::wrap(obj.py(), enc_array)
    } else if obj.is_instance(chunked_array)? {
        let chunks: Vec<&PyAny> = obj.getattr("chunks")?.extract()?;
        let encoded_chunks = chunks
            .iter()
            .map(|a| {
                ArrayData::from_pyarrow(a)
                    .map(make_array)
                    .map(|a| ArrayRef::from_arrow(a, false))
            })
            .collect::<PyResult<Vec<ArrayRef>>>()?;
        let dtype: DType = obj
            .getattr("type")
            .and_then(DataType::from_pyarrow)
            .map(|dt| DType::from_arrow(&Field::new("_", dt, false)))?;
        PyArray::wrap(
            obj.py(),
            ChunkedArray::new(encoded_chunks, dtype).into_array(),
        )
    } else if obj.is_instance(table)? {
        let array_stream = ArrowArrayStreamReader::from_pyarrow(obj)?;
        let dtype = DType::from_arrow(array_stream.schema());
        let chunks = array_stream
            .into_iter()
            .map(|b| b.map(|bb| bb.into_array()).map_err(map_arrow_err))
            .collect::<PyResult<Vec<ArrayRef>>>()?;
        PyArray::wrap(obj.py(), ChunkedArray::new(chunks, dtype).into_array())
    } else {
        Err(PyValueError::new_err("Cannot convert object to enc array"))
    }
}
