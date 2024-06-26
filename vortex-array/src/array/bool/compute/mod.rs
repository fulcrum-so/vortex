use crate::array::bool::BoolArray;
use crate::compute::compare::CompareFn;
use crate::compute::slice::SliceFn;
use crate::compute::take::TakeFn;
use crate::compute::unary::fill_forward::FillForwardFn;
use crate::compute::unary::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;

mod compare;
mod fill;
mod flatten;
mod scalar_at;
mod slice;
mod take;

impl ArrayCompute for BoolArray {
    fn compare(&self) -> Option<&dyn CompareFn> {
        Some(self)
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        Some(self)
    }

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
