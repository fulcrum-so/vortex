use vortex_error::{VortexError, VortexResult};

use crate::array::Array;
use crate::scalar::Scalar;

pub trait ScalarAtFn {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar>;
}

pub fn scalar_at(array: &dyn Array, index: usize) -> VortexResult<Scalar> {
    if index >= array.len() {
        return Err(VortexError::OutOfBounds(index, 0, array.len()));
    }

    array
        .scalar_at()
        .map(|t| t.scalar_at(index))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "scalar_at",
                array.encoding().id().name(),
            ))
        })
}
