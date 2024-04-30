use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};

use crate::scalar::Scalar;
use crate::{Array, ArrayDType, OwnedArray};

pub trait ScalarSubtractFn {
    fn scalar_subtract(&self, to_subtract: &Scalar) -> VortexResult<OwnedArray>;
}

pub fn scalar_subtract<T: Into<Scalar>>(array: &Array, to_subtract: T) -> VortexResult<OwnedArray> {
    let to_subtract = to_subtract.into().cast(array.dtype())?;

    if let Some(subtraction_result) =
        array.with_dyn(|c| c.scalar_subtract().map(|t| t.scalar_subtract(&to_subtract)))
    {
        return subtraction_result;
    }
    // if subtraction is not implemented for the given array type, but the array has a numeric
    // DType, we can flatten the array and apply subtraction to the flattened primitive array
    let result = match array.dtype() {
        DType::Int(..) | DType::Float(..) => {
            let array = array.clone();
            let flat = array.flatten_primitive()?;
            Some(flat.scalar_subtract(&to_subtract))
        }
        _ => None,
    };

    result.unwrap_or_else(|| {
        Err(vortex_err!(
            NotImplemented: "scalar_subtract",
            array.encoding().id().name()
        ))
    })
}
