use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_expr::operators::Operator;
use vortex_scalar::Scalar;

use crate::{Array, ArrayDType};

pub trait CompareScalarFn {
    fn compare_scalar(&self, comparator: Operator, scalar: &Scalar) -> VortexResult<Array>;
}

pub fn compare_scalar(array: &Array, comparator: Operator, scalar: &Scalar) -> VortexResult<Array> {
    if let Some(matching_indices) = array.with_dyn(|c| {
        c.compare_scalar()
            .map(|t| t.compare_scalar(comparator, scalar))
    }) {
        return matching_indices;
    }
    // if compare_scalar is not implemented for the given array type, but the array has a numeric
    // DType, we can flatten the array and apply filter to the flattened primitive array
    match array.dtype() {
        DType::Primitive(..) => {
            let flat = array.clone().flatten_primitive()?;
            flat.compare_scalar(comparator, scalar)
        }
        _ => Err(vortex_err!(
            NotImplemented: "compare_scalar",
            array.encoding().id()
        )),
    }
}
