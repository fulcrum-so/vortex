use itertools::Itertools;

use vortex_error::VortexResult;

use crate::array::chunked::ChunkedArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::ArrayRef;
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::ArrayCompute;
use crate::scalar::Scalar;

impl ArrayCompute for ChunkedArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl AsContiguousFn for ChunkedArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        // Combine all the chunks into one, then call as_contiguous again.
        let chunks = arrays
            .iter()
            .flat_map(|a| a.as_chunked().chunks().iter())
            .cloned()
            .collect_vec();
        as_contiguous(chunks)
    }
}

impl FlattenFn for ChunkedArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Chunked(self.clone()))
    }
}

impl ScalarAtFn for ChunkedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let (chunk_index, chunk_offset) = self.find_physical_location(index);
        scalar_at(self.chunks[chunk_index].as_ref(), chunk_offset)
    }
}
