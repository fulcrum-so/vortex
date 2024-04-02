use vortex_error::{vortex_err, VortexResult};
use vortex_schema::DType;

use crate::array::{Array, ArrayRef};

pub trait CastFn {
    fn cast(&self, dtype: &DType) -> VortexResult<ArrayRef>;
}

pub fn cast(array: &dyn Array, dtype: &DType) -> VortexResult<ArrayRef> {
    if array.dtype() == dtype {
        return Ok(array.to_array());
    }

    // TODO(ngates): check for null_count if dtype is non-nullable
    array
        .cast()
        .map(|f| f.cast(dtype))
        .unwrap_or_else(|| Err(vortex_err!(NotImplemented: "cast", array.encoding().id().name())))
}
