use crate::array::{Array, ArrayRef};
use crate::error::{VortexError, VortexResult};

pub trait FillForwardFn {
    fn fill_forward(&self) -> VortexResult<ArrayRef>;
}

pub fn fill_forward(array: &dyn Array) -> VortexResult<ArrayRef> {
    if !array.dtype().is_nullable() {
        return Ok(dyn_clone::clone_box(array));
    }

    array
        .fill_forward()
        .map(|t| t.fill_forward())
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "fill_forward",
                array.encoding().id(),
            ))
        })
}
