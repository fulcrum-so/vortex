use std::fmt::{Display, Formatter};

use vortex_error::{VortexError, VortexResult};
use vortex_schema::{DType, Nullability};

use crate::scalar::Scalar;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct BinaryScalar {
    value: Option<Vec<u8>>,
}

impl BinaryScalar {
    pub fn new(value: Option<Vec<u8>>) -> Self {
        Self { value }
    }

    pub fn none() -> Self {
        Self { value: None }
    }

    pub fn some(value: Vec<u8>) -> Self {
        Self { value: Some(value) }
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &DType::Binary(Nullability::NonNullable)
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }

    pub fn nbytes(&self) -> usize {
        self.value().map(|s| s.len()).unwrap_or(1)
    }
}

impl From<Vec<u8>> for Scalar {
    fn from(value: Vec<u8>) -> Self {
        BinaryScalar::new(Some(value)).into()
    }
}

impl TryFrom<Scalar> for Vec<u8> {
    type Error = VortexError;

    fn try_from(value: Scalar) -> VortexResult<Self> {
        let Scalar::Binary(b) = value else {
            return Err(VortexError::InvalidDType(value.dtype().clone()));
        };
        let dtype = b.dtype().clone();
        b.value.ok_or_else(|| VortexError::InvalidDType(dtype))
    }
}

impl Display for BinaryScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value() {
            None => write!(f, "bytes[none]"),
            Some(b) => write!(f, "bytes[{}]", b.len()),
        }
    }
}
