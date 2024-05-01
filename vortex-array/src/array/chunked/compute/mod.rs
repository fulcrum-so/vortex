use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::chunked::ChunkedArray;
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::scalar_subtract::SubtractScalarFn;
use crate::compute::slice::SliceFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::{Array, OwnedArray, ToStatic};

mod slice;
mod take;

impl ArrayCompute for ChunkedArray<'_> {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
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

    fn subtract_scalar(&self) -> Option<&dyn SubtractScalarFn> {
        Some(self)
    }
}

impl AsContiguousFn for ChunkedArray<'_> {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<OwnedArray> {
        // Combine all the chunks into one, then call as_contiguous again.
        let mut chunks = Vec::with_capacity(self.nchunks());
        for array in arrays {
            for chunk in ChunkedArray::try_from(array).unwrap().chunks() {
                chunks.push(chunk.to_static());
            }
        }
        as_contiguous(&chunks)
    }
}

impl ScalarAtFn for ChunkedArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let (chunk_index, chunk_offset) = self.find_chunk_idx(index);
        scalar_at(&self.chunk(chunk_index).unwrap(), chunk_offset)
    }
}
