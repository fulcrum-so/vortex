use log::info;
use vortex_error::{vortex_err, VortexResult};

use crate::{Array, IntoArray, OwnedArray};

pub trait TakeFn {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray>;
}

pub fn take(array: &Array, indices: &Array) -> VortexResult<OwnedArray> {
    array.with_dyn(|a| {
        if let Some(take) = a.take() {
            return take.take(indices);
        }

        // Otherwise, flatten and try again.
        info!("TakeFn not implemented for {}, flattening", array);
        array.clone().flatten()?.into_array().with_dyn(|a| {
            a.take()
                .map(|t| t.take(indices))
                .unwrap_or_else(|| Err(vortex_err!(NotImplemented: "take", array.encoding().id())))
        })
    })
}
