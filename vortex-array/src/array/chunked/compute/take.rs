use vortex_error::VortexResult;

use crate::array::chunked::ChunkedArray;
use crate::array::primitive::TypedPrimitiveTrait;
use crate::array::{Array, ArrayRef, IntoArray};
use crate::compute::cast::cast;
use crate::compute::flatten::flatten_primitive;
use crate::compute::take::{take, TakeFn};
use crate::ptype::PType;

impl TakeFn for ChunkedArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        if self.len() == indices.len() {
            return Ok(self.to_array());
        }

        let indices = flatten_primitive(cast(indices, PType::U64.into())?.as_ref())?;

        // While the chunk idx remains the same, accumulate a list of chunk indices.
        let mut chunks = Vec::new();
        let mut indices_in_chunk = Vec::new();
        let mut prev_chunk_idx = self
            .find_chunk_idx(indices.typed_data::<u64>()[0] as usize)
            .0;
        for idx in indices.typed_data::<u64>() {
            let (chunk_idx, idx_in_chunk) = self.find_chunk_idx(*idx as usize);

            if chunk_idx != prev_chunk_idx {
                // Start a new chunk
                let indices_in_chunk_array = indices_in_chunk.clone().into_array();
                chunks.push(take(
                    &self.chunks()[prev_chunk_idx],
                    &indices_in_chunk_array,
                )?);
                indices_in_chunk = Vec::new();
            }

            indices_in_chunk.push(idx_in_chunk as u64);
            prev_chunk_idx = chunk_idx;
        }

        if !indices_in_chunk.is_empty() {
            let indices_in_chunk_array = indices_in_chunk.into_array();
            chunks.push(take(
                &self.chunks()[prev_chunk_idx],
                &indices_in_chunk_array,
            )?);
        }

        Ok(ChunkedArray::new(chunks, self.dtype().clone()).into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::chunked::ChunkedArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::IntoArray;
    use crate::compute::as_contiguous::as_contiguous;
    use crate::compute::take::take;

    #[test]
    fn test_take() {
        let a = vec![1i32, 2, 3].into_array();
        let arr = ChunkedArray::new(vec![a.clone(), a.clone(), a.clone()], a.dtype().clone());
        let indices = vec![0, 0, 6, 4].into_array();

        let result = as_contiguous(take(&arr, &indices).unwrap().as_chunked().chunks()).unwrap();
        assert_eq!(result.as_primitive().typed_data::<i32>(), &[1, 1, 1, 2]);
    }
}
