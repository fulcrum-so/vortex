use std::fmt::{Display, Formatter};

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::scalar::Scalar;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct CompositeScalar {
    dtype: DType,
    scalar: Box<Scalar>,
}

impl CompositeScalar {
    pub fn new(dtype: DType, scalar: Box<Scalar>) -> Self {
        Self { dtype, scalar }
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn scalar(&self) -> &Scalar {
        self.scalar.as_ref()
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }

    pub fn nbytes(&self) -> usize {
        self.scalar.nbytes()
    }
}

impl Display for CompositeScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.scalar, self.dtype)
    }
}
