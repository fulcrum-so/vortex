use std::fmt::Debug;

use arrow_buffer::{ArrowNativeType, Buffer, ScalarBuffer};

use crate::array::primitive::PrimitiveArray;
use crate::array::validity::ValidityView;
use crate::array::Array;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::cast::CastFn;
use crate::compute::fill::FillForwardFn;
use crate::compute::flatten::FlattenFn;
use crate::compute::patch::PatchFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::search_sorted::SearchSortedFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::ptype::{NativePType, PType};

mod as_arrow;
mod as_contiguous;
mod cast;
mod fill;
mod flatten;
mod patch;
mod scalar_at;
mod search_sorted;
mod take;

pub trait PrimitiveTrait: Array + Debug + Send + Sync {
    fn ptype(&self) -> PType;
    // This seems odd.
    fn validity_view(&self) -> Option<ValidityView>;
    fn buffer(&self) -> &Buffer;
    fn to_primitive(&self) -> PrimitiveArray;
}

pub trait TypedPrimitiveTrait {
    fn scalar_buffer<T: NativePType + ArrowNativeType>(&self) -> ScalarBuffer<T>;
    fn typed_data<T: NativePType>(&self) -> &[T];
}

// TODO(ngates): try implementing like this?
// impl dyn PrimitiveTrait + '_ {

impl<P: PrimitiveTrait> TypedPrimitiveTrait for P {
    fn scalar_buffer<T: NativePType + ArrowNativeType>(&self) -> ScalarBuffer<T> {
        assert_eq!(self.ptype(), T::PTYPE);
        ScalarBuffer::from(self.buffer().clone())
    }

    fn typed_data<T: NativePType>(&self) -> &[T] {
        assert_eq!(self.ptype(), T::PTYPE);
        self.buffer().typed_data::<T>()
    }
}

impl ArrayCompute for &dyn PrimitiveTrait {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn cast(&self) -> Option<&dyn CastFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
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

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}
