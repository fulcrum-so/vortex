use std::fmt::{Display, Formatter};
use vortex_schema::{DType, Nullability};

use crate::error::{VortexError, VortexResult};
use crate::scalar::Scalar;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Utf8Scalar {
    value: Option<String>,
}

impl Utf8Scalar {
    pub fn new(value: Option<String>) -> Self {
        Self { value }
    }

    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &DType::Utf8(Nullability::NonNullable)
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }

    pub fn nbytes(&self) -> usize {
        self.value().map(|v| v.len()).unwrap_or(0)
    }
}

impl From<String> for Scalar {
    fn from(value: String) -> Self {
        Utf8Scalar::new(Some(value)).into()
    }
}

impl From<&str> for Scalar {
    fn from(value: &str) -> Self {
        Utf8Scalar::new(Some(value.to_string())).into()
    }
}

impl TryFrom<Scalar> for String {
    type Error = VortexError;

    fn try_from(value: Scalar) -> Result<Self, Self::Error> {
        let Scalar::Utf8(u) = value else {
            return Err(VortexError::InvalidDType(value.dtype().clone()));
        };
        match u.value {
            None => Err(VortexError::InvalidDType(u.dtype().clone())),
            Some(s) => Ok(s),
        }
    }
}

impl TryFrom<&Scalar> for String {
    type Error = VortexError;

    fn try_from(value: &Scalar) -> Result<Self, Self::Error> {
        let Scalar::Utf8(u) = value else {
            return Err(VortexError::InvalidDType(value.dtype().clone()));
        };
        match u.value() {
            None => Err(VortexError::InvalidDType(u.dtype().clone())),
            Some(s) => Ok(s.to_string()),
        }
    }
}

impl Display for Utf8Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value() {
            None => write!(f, "<none>"),
            Some(v) => Display::fmt(v, f),
        }
    }
}
