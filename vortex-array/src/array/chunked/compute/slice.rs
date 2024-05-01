use vortex_error::VortexResult;

use crate::array::chunked::ChunkedArray;
use crate::compute::slice::{slice, SliceFn};
use crate::{ArrayDType, IntoArray, OwnedArray};

impl SliceFn for ChunkedArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        let (offset_chunk, offset_in_first_chunk) = self.find_chunk_idx(start);
        let (length_chunk, length_in_last_chunk) = self.find_chunk_idx(stop);

        if length_chunk == offset_chunk {
            if let Some(chunk) = self.chunk(offset_chunk) {
                return ChunkedArray::try_new(
                    vec![slice(&chunk, offset_in_first_chunk, length_in_last_chunk)?],
                    self.dtype().clone(),
                )
                .map(|a| a.into_array());
            }
        }

        let mut chunks = (offset_chunk..length_chunk + 1)
            .map(|i| {
                self.chunk(i)
                    .expect("find_chunk_idx returned an incorrect index")
            })
            .collect::<Vec<_>>();
        if let Some(c) = chunks.first_mut() {
            *c = slice(c, offset_in_first_chunk, c.len())?;
        }

        if length_in_last_chunk == 0 {
            chunks.pop();
        } else if let Some(c) = chunks.last_mut() {
            *c = slice(c, 0, length_in_last_chunk)?;
        }

        ChunkedArray::try_new(chunks, self.dtype().clone()).map(|a| a.into_array())
    }
}
