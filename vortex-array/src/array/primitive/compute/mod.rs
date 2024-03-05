use crate::array::primitive::PrimitiveArray;
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::cast::CastPrimitiveFn;
use crate::compute::fill::FillForwardFn;
use crate::compute::patch::PatchFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;

mod as_contiguous;
mod cast;
mod fill;
mod patch;
mod scalar_at;

impl ArrayCompute for PrimitiveArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }
    fn cast_primitive(&self) -> Option<&dyn CastPrimitiveFn> {
        Some(self)
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        Some(self)
    }

    fn patch(&self) -> Option<&dyn PatchFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}
