use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::scalar::Scalar;
use vortex::{Array, OwnedArray};
use vortex_error::VortexResult;

use crate::ALPArray;

impl ArrayCompute for ALPArray<'_> {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for ALPArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        scalar_at(&self.encoded(), index)
    }
}

impl TakeFn for ALPArray<'_> {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        take(&self.encoded(), indices)
    }
}

impl SliceFn for ALPArray<'_> {
    fn slice(&self, start: usize, end: usize) -> VortexResult<OwnedArray> {
        slice(&self.encoded(), start, end)
    }
}
