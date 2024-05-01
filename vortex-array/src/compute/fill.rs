use vortex_error::{vortex_err, VortexResult};

use crate::{Array, ArrayDType, OwnedArray, ToStatic};

pub trait FillForwardFn {
    fn fill_forward(&self) -> VortexResult<OwnedArray>;
}

pub fn fill_forward(array: &Array) -> VortexResult<OwnedArray> {
    if !array.dtype().is_nullable() {
        return Ok(array.to_static());
    }

    array.with_dyn(|a| {
        a.fill_forward()
            .map(|t| t.fill_forward())
            .unwrap_or_else(|| {
                Err(vortex_err!(
                    NotImplemented: "fill_forward",
                    array.encoding().id()
                ))
            })
    })
}
