use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::PyErr;

use enc::error::EncError;

pub struct PyEncError(EncError);

impl PyEncError {
    pub fn new(error: EncError) -> Self {
        Self(error)
    }

    pub fn map_err(error: EncError) -> PyErr {
        PyEncError::new(error).into()
    }
}

impl From<PyEncError> for PyErr {
    fn from(value: PyEncError) -> Self {
        match value.0 {
            EncError::OutOfBounds(_, _, _) => PyValueError::new_err(value.0.to_string()),
            EncError::LengthMismatch => PyValueError::new_err(value.0.to_string()),
            EncError::ComputeError(_) => PyValueError::new_err(value.0.to_string()),
            EncError::InvalidDType(_) => PyTypeError::new_err(value.0.to_string()),
            EncError::InvalidEncoding(_) => PyTypeError::new_err(value.0.to_string()),
            EncError::IncompatibleTypes(_, _) => PyTypeError::new_err(value.0.to_string()),
            EncError::InvalidArrowDataType(_) => PyTypeError::new_err(value.0.to_string()),
            EncError::PolarsError(_) => PyValueError::new_err(value.0.to_string()),
            EncError::MalformedPatches(_) => PyValueError::new_err(value.0.to_string()),
            EncError::MismatchedTypes(_, _) => PyTypeError::new_err(value.0.to_string()),
        }
    }
}
