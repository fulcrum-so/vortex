use vortex_error::{VortexError, VortexResult};

use crate::array::{Array, ArrayRef, WithArrayCompute};

pub trait PatchFn {
    fn patch(&self, patch: &dyn Array) -> VortexResult<ArrayRef>;
}

/// Returns a new array where the non-null values from the patch array are replaced in the original.
pub fn patch(array: &dyn Array, patch: &dyn Array) -> VortexResult<ArrayRef> {
    if array.len() != patch.len() {
        return Err(VortexError::InvalidArgument(
            format!(
                "patch array {} must have the same length as the original array {}",
                patch, array
            )
            .into(),
        ));
    }

    if array.dtype().as_nullable() != patch.dtype().as_nullable() {
        return Err(VortexError::MismatchedTypes(
            array.dtype().clone(),
            patch.dtype().clone(),
        ));
    }

    array.with_compute(|c| {
        c.patch().map(|t| t.patch(patch)).unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "take",
                array.encoding().id().name(),
            ))
        })
    })
}
