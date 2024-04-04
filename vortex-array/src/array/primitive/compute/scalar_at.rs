use vortex_error::VortexResult;

use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::primitive::compute::TypedPrimitiveTrait;
use crate::compute::scalar_at::ScalarAtFn;
use crate::match_each_native_ptype;
use crate::scalar::{PrimitiveScalar, Scalar};

impl ScalarAtFn for &dyn PrimitiveTrait {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match_each_native_ptype!(self.ptype(), |$T| {
            Ok(PrimitiveScalar::try_new(
                self.is_valid(index).then(|| self.typed_data::<$T>()[index]),
                self.nullability(),
            )?.into())
        })
    }
}
