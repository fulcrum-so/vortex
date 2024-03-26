use std::fmt::{Display, Formatter};

use vortex_error::{VortexError, VortexResult};
use vortex_schema::{DType, Nullability};

use crate::scalar::Scalar;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct BoolScalar {
    value: Option<bool>,
}

impl BoolScalar {
    pub fn new(value: Option<bool>) -> Self {
        Self { value }
    }

    pub fn none() -> Self {
        Self { value: None }
    }

    pub fn some(value: bool) -> Self {
        Self { value: Some(value) }
    }

    pub fn value(&self) -> Option<bool> {
        self.value
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &DType::Bool(Nullability::NonNullable)
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Scalar> {
        match dtype {
            DType::Bool(_) => Ok(self.clone().into()),
            _ => Err(VortexError::InvalidDType(dtype.clone())),
        }
    }

    pub fn nbytes(&self) -> usize {
        1
    }
}

impl From<bool> for Scalar {
    #[inline]
    fn from(value: bool) -> Self {
        BoolScalar::new(Some(value)).into()
    }
}

impl TryFrom<Scalar> for bool {
    type Error = VortexError;

    fn try_from(value: Scalar) -> VortexResult<Self> {
        let Scalar::Bool(b) = value else {
            return Err(VortexError::InvalidDType(value.dtype().clone()));
        };

        b.value()
            .ok_or_else(|| VortexError::InvalidDType(b.dtype().clone()))
    }
}

impl Display for BoolScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value() {
            None => write!(f, "null"),
            Some(b) => Display::fmt(&b, f),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn into_from() {
        let scalar: Scalar = false.into();
        assert_eq!(scalar.try_into(), Ok(false));
    }
}
