use std::fmt::{Display, Formatter};
use vortex_schema::DType;

use crate::error::VortexResult;
use crate::scalar::Scalar;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct NullScalar;

impl Default for NullScalar {
    fn default() -> Self {
        Self::new()
    }
}

impl NullScalar {
    #[inline]
    pub fn new() -> Self {
        Self {}
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &DType::Null
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }

    pub fn nbytes(&self) -> usize {
        1
    }
}

impl Display for NullScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "null")
    }
}
