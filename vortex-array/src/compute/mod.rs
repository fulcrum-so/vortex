use crate::array::{Array, ArrayRef};
use as_arrow::AsArrowArray;
use as_contiguous::AsContiguousFn;
use cast::CastFn;
use fill::FillForwardFn;
use flatten::*;
use patch::PatchFn;
use scalar_at::ScalarAtFn;
use search_sorted::SearchSortedFn;
use take::TakeFn;
use vortex_error::VortexResult;

pub mod add;
pub mod as_arrow;
pub mod as_contiguous;
pub mod cast;
pub mod fill;
pub mod flatten;
pub mod patch;
pub mod repeat;
pub mod scalar_at;
pub mod search_sorted;
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

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        None
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        None
    }

    fn patch(&self) -> Option<&dyn PatchFn> {
        None
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        None
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        None
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        None
    }
}

pub trait ComputeVTable<A> {
    fn take(&self) -> Option<&dyn VTakeFn<A>> {
        None
    }
}

pub trait VTakeFn<A> {
    fn take(&self, array: A, indices: &dyn Array) -> VortexResult<ArrayRef>;
}
