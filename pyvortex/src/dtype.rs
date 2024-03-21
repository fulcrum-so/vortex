use arrow::datatypes::{DataType, Field};
use arrow::pyarrow::FromPyArrow;
use pyo3::types::PyType;
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult, Python};
use vortex::arrow::FromArrowType;

use vortex_schema::DType;

#[pyclass(name = "DType", module = "vortex", subclass)]
pub struct PyDType {
    inner: DType,
}

impl PyDType {
    pub fn wrap(py: Python<'_>, inner: DType) -> PyResult<Py<Self>> {
        Py::new(py, Self { inner })
    }

    pub fn unwrap(&self) -> &DType {
        &self.inner
    }
}

#[pymethods]
impl PyDType {
    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }

    #[classmethod]
    fn from_pyarrow(
        cls: &PyType,
        #[pyo3(from_py_with = "import_arrow_dtype")] arrow_dtype: DataType,
        nullable: bool,
    ) -> PyResult<Py<Self>> {
        PyDType::wrap(
            cls.py(),
            DType::from_arrow(&Field::new("_", arrow_dtype, nullable)),
        )
    }
}

fn import_arrow_dtype(obj: &PyAny) -> PyResult<DataType> {
    DataType::from_pyarrow(obj)
}
