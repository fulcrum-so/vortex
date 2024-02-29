// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::sync::{Arc, RwLock};
use std::vec::IntoIter;

use arrow::array::ArrayRef as ArrowArrayRef;
use itertools::Itertools;
use linkme::distributed_slice;

use crate::array::{
    check_index_bounds, check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId,
    EncodingRef, ENCODINGS,
};
use crate::compress::EncodingCompression;
use crate::dtype::DType;
use crate::error::{VortexError, VortexResult};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::Scalar;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsSet};

mod compress;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct ChunkedArray {
    chunks: Vec<ArrayRef>,
    chunk_ends: Vec<usize>,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl ChunkedArray {
    pub fn new(chunks: Vec<ArrayRef>, dtype: DType) -> Self {
        Self::try_new(chunks, dtype).unwrap()
    }

    pub fn try_new(chunks: Vec<ArrayRef>, dtype: DType) -> VortexResult<Self> {
        chunks
            .iter()
            .map(|c| c.dtype().as_nullable())
            .all_equal_value()
            .map(|_| ())
            .or_else(|mismatched| match mismatched {
                None => Ok(()),
                Some((fst, snd)) => Err(VortexError::MismatchedTypes(fst, snd)),
            })?;

        let chunk_ends = chunks
            .iter()
            .scan(0usize, |acc, c| {
                *acc += c.len();
                Some(*acc)
            })
            .collect::<Vec<usize>>();

        let dtype = if chunks.iter().any(|c| c.dtype().is_nullable()) && !dtype.is_nullable() {
            dtype.as_nullable()
        } else {
            dtype
        };

        Ok(Self {
            chunks,
            chunk_ends,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn chunks(&self) -> &[ArrayRef] {
        &self.chunks
    }

    fn find_physical_location(&self, index: usize) -> (usize, usize) {
        assert!(index <= self.len(), "Index out of bounds of the array");
        let index_chunk = self
            .chunk_ends
            .binary_search(&index)
            // If the result of binary_search is Ok it means we have exact match, since these are chunk ends EXCLUSIVE we have to add one to move to the next one
            .map(|o| o + 1)
            .unwrap_or_else(|o| o);
        let index_in_chunk = index
            - if index_chunk == 0 {
                0
            } else {
                self.chunk_ends[index_chunk - 1]
            };
        (index_chunk, index_in_chunk)
    }
}

impl Array for ChunkedArray {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn len(&self) -> usize {
        *self.chunk_ends.last().unwrap_or(&0usize)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.chunks.is_empty() || self.len() == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        check_index_bounds(self, index)?;
        let (chunk_index, chunk_offset) = self.find_physical_location(index);
        self.chunks[chunk_index].scalar_at(chunk_offset)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(ChunkedArrowIterator::new(self))
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        let (offset_chunk, offset_in_first_chunk) = self.find_physical_location(start);
        let (length_chunk, length_in_last_chunk) = self.find_physical_location(stop);

        if length_chunk == offset_chunk {
            if let Some(chunk) = self.chunks.get(offset_chunk) {
                return Ok(ChunkedArray::new(
                    vec![chunk.slice(offset_in_first_chunk, length_in_last_chunk)?],
                    self.dtype.clone(),
                )
                .boxed());
            }
        }

        let mut chunks = self.chunks.clone()[offset_chunk..length_chunk + 1].to_vec();
        if let Some(c) = chunks.first_mut() {
            *c = c.slice(offset_in_first_chunk, c.len())?;
        }

        if length_in_last_chunk == 0 {
            chunks.pop();
        } else if let Some(c) = chunks.last_mut() {
            *c = c.slice(0, length_in_last_chunk)?;
        }

        Ok(ChunkedArray::new(chunks, self.dtype.clone()).boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &ChunkedEncoding
    }

    fn nbytes(&self) -> usize {
        self.chunks().iter().map(|arr| arr.nbytes()).sum()
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl FromIterator<ArrayRef> for ChunkedArray {
    fn from_iter<T: IntoIterator<Item = ArrayRef>>(iter: T) -> Self {
        let chunks: Vec<ArrayRef> = iter.into_iter().collect();
        let dtype = chunks
            .first()
            .map(|c| c.dtype().clone())
            .expect("Cannot create a chunked array from an empty iterator");
        Self::new(chunks, dtype)
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for ChunkedArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for ChunkedArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln("chunks:")?;
        f.indent(|indent| {
            for chunk in self.chunks() {
                indent
                    .new_total_size(chunk.nbytes(), |new_total| new_total.array(chunk.as_ref()))?;
            }
            Ok(())
        })
    }
}

#[derive(Debug)]
struct ChunkedEncoding;

pub const CHUNKED_ENCODING: EncodingId = EncodingId::new("vortex.chunked");

#[distributed_slice(ENCODINGS)]
static ENCODINGS_CHUNKED: EncodingRef = &ChunkedEncoding;

impl Encoding for ChunkedEncoding {
    fn id(&self) -> &EncodingId {
        &CHUNKED_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

struct ChunkedArrowIterator {
    chunks_iter: IntoIter<ArrayRef>,
    arrow_iter: Option<Box<ArrowIterator>>,
}

impl ChunkedArrowIterator {
    fn new(array: &ChunkedArray) -> Self {
        let mut chunks_iter = array.chunks.clone().into_iter();
        let arrow_iter = chunks_iter.next().map(|c| c.iter_arrow());
        Self {
            chunks_iter,
            arrow_iter,
        }
    }
}

impl Iterator for ChunkedArrowIterator {
    type Item = ArrowArrayRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.arrow_iter
            .as_mut()
            .and_then(|iter| iter.next())
            .or_else(|| {
                self.chunks_iter.next().and_then(|next_chunk| {
                    self.arrow_iter = Some(next_chunk.iter_arrow());
                    self.next()
                })
            })
    }
}

#[cfg(test)]
mod test {
    use arrow::array::cast::AsArray;
    use arrow::array::types::UInt64Type;
    use arrow::array::ArrayRef as ArrowArrayRef;
    use arrow::array::ArrowPrimitiveType;
    use itertools::Itertools;

    use crate::array::chunked::ChunkedArray;
    use crate::array::Array;
    use crate::dtype::{DType, IntWidth, Nullability, Signedness};

    fn chunked_array() -> ChunkedArray {
        ChunkedArray::new(
            vec![
                vec![1u64, 2, 3].into(),
                vec![4u64, 5, 6].into(),
                vec![7u64, 8, 9].into(),
            ],
            DType::Int(
                IntWidth::_64,
                Signedness::Unsigned,
                Nullability::NonNullable,
            ),
        )
    }

    fn assert_equal_slices<T: ArrowPrimitiveType>(arr: ArrowArrayRef, slice: &[T::Native]) {
        assert_eq!(*arr.as_primitive::<T>().values(), slice);
    }

    #[test]
    pub fn iter() {
        let chunked = ChunkedArray::new(
            vec![vec![1u64, 2, 3].into(), vec![4u64, 5, 6].into()],
            DType::Int(
                IntWidth::_64,
                Signedness::Unsigned,
                Nullability::NonNullable,
            ),
        );

        chunked
            .iter_arrow()
            .zip_eq([[1u64, 2, 3], [4, 5, 6]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }

    #[test]
    pub fn slice_middle() {
        chunked_array()
            .slice(2, 5)
            .unwrap()
            .iter_arrow()
            .zip_eq([vec![3u64], vec![4, 5]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }

    #[test]
    pub fn slice_begin() {
        chunked_array()
            .slice(1, 3)
            .unwrap()
            .iter_arrow()
            .zip_eq([[2u64, 3]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }

    #[test]
    pub fn slice_aligned() {
        chunked_array()
            .slice(3, 6)
            .unwrap()
            .iter_arrow()
            .zip_eq([[4u64, 5, 6]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }

    #[test]
    pub fn slice_many_aligned() {
        chunked_array()
            .slice(0, 6)
            .unwrap()
            .iter_arrow()
            .zip_eq([[1u64, 2, 3], [4, 5, 6]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }

    #[test]
    pub fn slice_end() {
        chunked_array()
            .slice(7, 8)
            .unwrap()
            .iter_arrow()
            .zip_eq([[8u64]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }
}
