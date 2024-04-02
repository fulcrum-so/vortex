use log::info;
use vortex_error::{VortexError, VortexResult};

use crate::array::{Array, ArrayRef};
use crate::compute::flatten::flatten;

pub trait TakeFn {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef>;
}

pub fn take(array: &dyn Array, indices: &dyn Array) -> VortexResult<ArrayRef> {
    if let Some(take) = array.compute().and_then(|c| c.take()) {
        return take.take(indices);
    }

    // Otherwise, flatten and try again.
    info!("TakeFn not implemented for {}, flattening", array);
    flatten(array)?
        .into_array()
        .take()
        .map(|t| t.take(indices))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "take",
                array.encoding().id().name(),
            ))
        })
}
