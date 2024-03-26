use vortex::compute::scalar_at::ScalarAtFn;
use vortex::compute::ArrayCompute;
use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::RoaringBoolArray;

impl ArrayCompute for RoaringBoolArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for RoaringBoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.bitmap.contains(index as u32) {
            Ok(true.into())
        } else {
            Ok(false.into())
        }
    }
}
