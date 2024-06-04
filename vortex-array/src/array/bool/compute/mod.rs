use crate::array::bool::BoolArray;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::compare::CompareFn;
use crate::compute::compare_scalar::CompareScalarFn;
use crate::compute::fill::FillForwardFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::slice::SliceFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;

mod as_arrow;
mod as_contiguous;
mod compare;
mod compare_scalar;
mod fill;
mod flatten;
mod scalar_at;
mod slice;
mod take;

impl ArrayCompute for BoolArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn compare(&self) -> Option<&dyn CompareFn> {
        Some(self)
    }

    fn compare_scalar(&self) -> Option<&dyn CompareScalarFn> {
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
