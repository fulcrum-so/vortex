use vortex_error::{vortex_err, VortexResult};

use crate::scalar::Scalar;
use crate::{Array, OwnedArray};

pub trait ScalarSubtractFn {
    fn scalar_subtract(&self, summand: Scalar) -> VortexResult<OwnedArray>;
}

pub fn scalar_subtract(array: &Array, summand: Scalar) -> VortexResult<OwnedArray> {
    array.with_dyn(|c| {
        c.scalar_subtract()
            .map(|t| t.scalar_subtract(summand.clone()))
            .unwrap_or_else(|| {
                Err(vortex_err!(
                    NotImplemented: "scalar_subtract",
                    array.encoding().id().name()
                ))
            })
    })
}
