use croaring::Bitmap;
use vortex::compute::slice::SliceFn;
use vortex::compute::unary::scalar_at::ScalarAtFn;
use vortex::compute::ArrayCompute;
use vortex::{Array, IntoArray};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::RoaringBoolArray;

impl ArrayCompute for RoaringBoolArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }
}

impl ScalarAtFn for RoaringBoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(self.bitmap().contains(index as u32).into())
    }
}

impl SliceFn for RoaringBoolArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let slice_bitmap = Bitmap::from_range(start as u32..stop as u32);
        let bitmap = self.bitmap().and(&slice_bitmap).add_offset(-(start as i64));

        Self::try_new(bitmap, stop - start).map(|a| a.into_array())
    }
}
