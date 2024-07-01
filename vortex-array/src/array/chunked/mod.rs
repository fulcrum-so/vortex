//! First-class chunked arrays.
//!
//! Vortex is a chunked array library that's able to
use futures_util::stream;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use vortex_dtype::{Nullability, PType};
use vortex_error::vortex_bail;
use vortex_scalar::Scalar;

use crate::array::primitive::PrimitiveArray;
use crate::compute::search_sorted::{search_sorted, SearchResult, SearchSortedSide};
use crate::compute::unary::scalar_at::scalar_at;
use crate::compute::unary::scalar_subtract::{subtract_scalar, SubtractScalarFn};
use crate::iter::{ArrayIterator, ArrayIteratorAdapter};
use crate::stream::{ArrayStream, ArrayStreamAdapter};
use crate::validity::Validity::NonNullable;
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, ArrayDType};

mod canonical;
mod compute;
mod stats;

impl_encoding!("vortex.chunked", Chunked);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkedMetadata;

impl ChunkedArray {
    const ENDS_DTYPE: DType = DType::Primitive(PType::U64, Nullability::NonNullable);

    pub fn try_new(chunks: Vec<Array>, dtype: DType) -> VortexResult<Self> {
        for chunk in &chunks {
            if chunk.dtype() != &dtype {
                vortex_bail!(MismatchedTypes: dtype, chunk.dtype());
            }
        }

        let chunk_ends = PrimitiveArray::from_vec(
            [0u64]
                .into_iter()
                .chain(chunks.iter().map(|c| c.len() as u64))
                .scan(0, |acc, c| {
                    *acc += c;
                    Some(*acc)
                })
                .collect_vec(),
            NonNullable,
        );

        let mut children = vec![chunk_ends.into_array()];
        children.extend(chunks);

        Self::try_from_parts(dtype, ChunkedMetadata, children.into(), StatsSet::new())
    }

    #[inline]
    pub fn chunk(&self, idx: usize) -> Option<Array> {
        // Offset the index since chunk_ends is child 0.
        self.array().child(idx + 1, self.array().dtype())
    }

    pub fn nchunks(&self) -> usize {
        self.chunk_ends().len() - 1
    }

    #[inline]
    pub fn chunk_ends(&self) -> Array {
        self.array()
            .child(0, &Self::ENDS_DTYPE)
            .expect("missing chunk ends")
    }

    pub fn find_chunk_idx(&self, index: usize) -> (usize, usize) {
        assert!(index <= self.len(), "Index out of bounds of the array");

        let index_chunk =
            match search_sorted(&self.chunk_ends(), index, SearchSortedSide::Left).unwrap() {
                SearchResult::Found(i) => i,
                SearchResult::NotFound(i) => i - 1,
            };
        let chunk_start =
            usize::try_from(&scalar_at(&self.chunk_ends(), index_chunk).unwrap()).unwrap();

        let index_in_chunk = index - chunk_start;
        (index_chunk, index_in_chunk)
    }

    pub fn chunks(&self) -> impl Iterator<Item = Array> + '_ {
        (0..self.nchunks()).map(|c| self.chunk(c).unwrap())
    }

    pub fn array_iterator(&self) -> impl ArrayIterator + '_ {
        ArrayIteratorAdapter::new(self.dtype().clone(), self.chunks().map(Ok))
    }

    pub fn array_stream(&self) -> impl ArrayStream + '_ {
        ArrayStreamAdapter::new(self.dtype().clone(), stream::iter(self.chunks().map(Ok)))
    }
}

impl FromIterator<Array> for ChunkedArray {
    fn from_iter<T: IntoIterator<Item = Array>>(iter: T) -> Self {
        let chunks: Vec<Array> = iter.into_iter().collect();
        let dtype = chunks
            .first()
            .map(|c| c.dtype().clone())
            .expect("Cannot create a chunked array from an empty iterator");
        Self::try_new(chunks, dtype).unwrap()
    }
}

impl AcceptArrayVisitor for ChunkedArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("chunk_ends", &self.chunk_ends())?;
        for (idx, chunk) in self.chunks().enumerate() {
            visitor.visit_child(format!("[{}]", idx).as_str(), &chunk)?;
        }
        Ok(())
    }
}

impl ArrayTrait for ChunkedArray {
    fn len(&self) -> usize {
        usize::try_from(&scalar_at(&self.chunk_ends(), self.nchunks()).unwrap()).unwrap()
    }
}

impl ArrayValidity for ChunkedArray {
    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }

    fn logical_validity(&self) -> LogicalValidity {
        todo!()
    }
}

impl SubtractScalarFn for ChunkedArray {
    fn subtract_scalar(&self, to_subtract: &Scalar) -> VortexResult<Array> {
        self.chunks()
            .map(|chunk| subtract_scalar(&chunk, to_subtract))
            .collect::<VortexResult<Vec<_>>>()
            .map(|chunks| {
                Self::try_new(chunks, self.dtype().clone())
                    .expect("Subtraction on chunked array changed dtype")
                    .into_array()
            })
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::{DType, Nullability};
    use vortex_dtype::{NativePType, PType};

    use crate::array::chunked::ChunkedArray;
    use crate::compute::slice::slice;
    use crate::compute::unary::scalar_subtract::subtract_scalar;
    use crate::{Array, IntoArray, IntoArrayVariant, IntoCanonical, ToArray};

    fn chunked_array() -> ChunkedArray {
        ChunkedArray::try_new(
            vec![
                vec![1u64, 2, 3].into_array(),
                vec![4u64, 5, 6].into_array(),
                vec![7u64, 8, 9].into_array(),
            ],
            DType::Primitive(PType::U64, Nullability::NonNullable),
        )
        .unwrap()
    }

    fn assert_equal_slices<T: NativePType>(arr: Array, slice: &[T]) {
        let mut values = Vec::with_capacity(arr.len());
        ChunkedArray::try_from(arr)
            .unwrap()
            .chunks()
            .map(|a| a.into_primitive().unwrap())
            .for_each(|a| values.extend_from_slice(a.maybe_null_slice::<T>()));
        assert_eq!(values, slice);
    }

    #[test]
    pub fn slice_middle() {
        assert_equal_slices(slice(chunked_array().array(), 2, 5).unwrap(), &[3u64, 4, 5])
    }

    #[test]
    pub fn slice_begin() {
        assert_equal_slices(slice(chunked_array().array(), 1, 3).unwrap(), &[2u64, 3]);
    }

    #[test]
    pub fn slice_aligned() {
        assert_equal_slices(slice(chunked_array().array(), 3, 6).unwrap(), &[4u64, 5, 6]);
    }

    #[test]
    pub fn slice_many_aligned() {
        assert_equal_slices(
            slice(chunked_array().array(), 0, 6).unwrap(),
            &[1u64, 2, 3, 4, 5, 6],
        );
    }

    #[test]
    pub fn slice_end() {
        assert_equal_slices(slice(chunked_array().array(), 7, 8).unwrap(), &[8u64]);
    }

    #[test]
    fn test_scalar_subtract() {
        let chunked = chunked_array();
        let to_subtract = 1u64;
        let array = subtract_scalar(&chunked.to_array(), &to_subtract.into()).unwrap();

        let chunked = ChunkedArray::try_from(array).unwrap();
        let mut chunks_out = chunked.chunks();

        let results = chunks_out
            .next()
            .unwrap()
            .into_canonical()
            .unwrap()
            .into_primitive()
            .unwrap()
            .maybe_null_slice::<u64>()
            .to_vec();
        assert_eq!(results, &[0u64, 1, 2]);
        let results = chunks_out
            .next()
            .unwrap()
            .into_canonical()
            .unwrap()
            .into_primitive()
            .unwrap()
            .maybe_null_slice::<u64>()
            .to_vec();
        assert_eq!(results, &[3u64, 4, 5]);
        let results = chunks_out
            .next()
            .unwrap()
            .into_canonical()
            .unwrap()
            .into_primitive()
            .unwrap()
            .maybe_null_slice::<u64>()
            .to_vec();
        assert_eq!(results, &[6u64, 7, 8]);
    }
}
