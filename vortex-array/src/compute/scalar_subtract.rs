use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::{Array, ArrayDType, OwnedArray};

pub trait SubtractScalarFn {
    fn subtract_scalar(&self, to_subtract: &Scalar) -> VortexResult<OwnedArray>;
}

pub fn subtract_scalar(array: &Array, to_subtract: &Scalar) -> VortexResult<OwnedArray> {
    if let Some(subtraction_result) =
        array.with_dyn(|c| c.subtract_scalar().map(|t| t.subtract_scalar(to_subtract)))
    {
        return subtraction_result;
    }
    // if subtraction is not implemented for the given array type, but the array has a numeric
    // DType, we can flatten the array and apply subtraction to the flattened primitive array
    match array.dtype() {
        DType::Primitive(..) => {
            // TODO(@jcasale): pass array instead of ref to get rid of clone?
            // downside is that subtract_scalar then consumes the array, which is not great
            let flat = array.clone().flatten_primitive()?;
            flat.subtract_scalar(to_subtract)
        }
        _ => Err(vortex_err!(
            NotImplemented: "scalar_subtract",
            array.encoding().id()
        )),
    }
}
