use as_arrow::AsArrowArray;
use as_contiguous::AsContiguousFn;
use cast::CastFn;
use compare::CompareFn;
use fill::FillForwardFn;
use patch::PatchFn;
use scalar_at::ScalarAtFn;
use search_sorted::SearchSortedFn;
use slice::SliceFn;
use take::TakeFn;

use crate::compute::compare_scalar::CompareScalarFn;
use crate::compute::filter_indices::FilterIndicesFn;
use crate::compute::scalar_subtract::SubtractScalarFn;

pub mod as_arrow;
pub mod as_contiguous;
pub mod cast;
pub mod compare;
pub mod compare_scalar;
pub mod fill;
pub mod filter_indices;
pub mod patch;
pub mod scalar_at;
pub mod scalar_subtract;
pub mod search_sorted;
pub mod slice;
pub mod take;

pub trait ArrayCompute {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        None
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        None
    }

    fn cast(&self) -> Option<&dyn CastFn> {
        None
    }

    fn compare(&self) -> Option<&dyn CompareFn> {
        None
    }

    fn compare_scalar(&self) -> Option<&dyn CompareScalarFn> {
        None
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        None
    }

    fn filter_indices(&self) -> Option<&dyn FilterIndicesFn> {
        None
    }

    fn patch(&self) -> Option<&dyn PatchFn> {
        None
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        None
    }

    fn subtract_scalar(&self) -> Option<&dyn SubtractScalarFn> {
        None
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        None
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        None
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        None
    }
}
