use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::scalar_at::ScalarAtFn;
use crate::match_each_native_ptype;
use crate::scalar::{PrimitiveScalar, Scalar};
use crate::validity::ArrayValidity;

impl ScalarAtFn for PrimitiveArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            Ok(match_each_native_ptype!(self.ptype, |$T| self.typed_data::<$T>()[index].into()))
        } else {
            Ok(PrimitiveScalar::none(self.ptype).into())
        }
    }
}
