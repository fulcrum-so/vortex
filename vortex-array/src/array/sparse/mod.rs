use std::any::Any;
use std::iter;
use std::sync::{Arc, RwLock};

use arrow_array::array::{ArrayRef as ArrowArrayRef, PrimitiveArray as ArrowPrimitiveArray};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_buffer::buffer::{NullBuffer, ScalarBuffer};
use arrow_buffer::BooleanBufferBuilder;
use linkme::distributed_slice;

use crate::array::ENCODINGS;
use crate::array::{
    check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef,
};
use crate::compress::EncodingCompression;
use crate::compute::search_sorted::{search_sorted, SearchSortedSide};
use crate::dtype::DType;
use crate::error::{VortexError, VortexResult};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::match_arrow_numeric_type;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsCompute, StatsSet};

mod compress;
mod compute;
mod serde;

#[derive(Debug, Clone)]
pub struct SparseArray {
    indices: ArrayRef,
    values: ArrayRef,
    // Offset value for patch indices as a result of slicing
    indices_offset: usize,
    len: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl SparseArray {
    pub fn new(indices: ArrayRef, values: ArrayRef, len: usize) -> Self {
        Self::try_new(indices, values, len).unwrap()
    }

    pub fn try_new(indices: ArrayRef, values: ArrayRef, len: usize) -> VortexResult<Self> {
        Self::new_with_offset(indices, values, len, 0)
    }

    pub(crate) fn new_with_offset(
        indices: ArrayRef,
        values: ArrayRef,
        len: usize,
        indices_offset: usize,
    ) -> VortexResult<Self> {
        if !matches!(indices.dtype(), &DType::IDX) {
            return Err(VortexError::InvalidDType(indices.dtype().clone()));
        }

        Ok(Self {
            indices,
            values,
            indices_offset,
            len,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn indices_offset(&self) -> usize {
        self.indices_offset
    }

    #[inline]
    pub fn values(&self) -> &dyn Array {
        self.values.as_ref()
    }

    #[inline]
    pub fn indices(&self) -> &dyn Array {
        self.indices.as_ref()
    }

    /// Return indices as a vector of usize with the indices_offset applied.
    pub fn resolved_indices(&self) -> Vec<usize> {
        let mut indices = Vec::with_capacity(self.len());
        self.indices().iter_arrow().for_each(|c| {
            indices.extend(
                arrow_cast::cast(c.as_ref(), &arrow_schema::DataType::UInt64)
                    .unwrap()
                    .as_primitive::<UInt64Type>()
                    .values()
                    .into_iter()
                    .map(|v| (*v as usize) - self.indices_offset),
            )
        });
        indices
    }
}

impl Array for SparseArray {
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

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.values().dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        // Resolve our indices into a vector of usize applying the offset
        let indices = self.resolved_indices();
        let array: ArrowArrayRef = match_arrow_numeric_type!(self.values().dtype(), |$E| {
            let mut validity = BooleanBufferBuilder::new(self.len());
            validity.append_n(self.len(), false);
            let mut values = vec![<$E as ArrowPrimitiveType>::Native::default(); self.len()];
            let mut offset = 0;
            for values_array in self.values().iter_arrow() {
                for v in values_array.as_primitive::<$E>().values() {
                    let idx = indices[offset];
                    values[idx] = *v;
                    validity.set_bit(idx, true);
                    offset += 1;
                }
            }
            Arc::new(ArrowPrimitiveArray::<$E>::new(
                ScalarBuffer::from(values),
                Some(NullBuffer::from(validity.finish())),
            ))
        });
        Box::new(iter::once(array))
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        // Find the index of the first patch index that is greater than or equal to the offset of this array
        let index_start_index = search_sorted(self.indices(), start, SearchSortedSide::Left)?;
        let index_end_index = search_sorted(self.indices(), stop, SearchSortedSide::Left)?;

        Ok(SparseArray {
            indices_offset: self.indices_offset + start,
            indices: self.indices.slice(index_start_index, index_end_index)?,
            values: self.values.slice(index_start_index, index_end_index)?,
            len: stop - start,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &SparseEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.indices.nbytes() + self.values.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl StatsCompute for SparseArray {}

impl<'arr> AsRef<(dyn Array + 'arr)> for SparseArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for SparseArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("offset", self.indices_offset())?;
        f.child("indices", self.indices())?;
        f.child("values", self.values())
    }
}

#[derive(Debug)]
pub struct SparseEncoding;

impl SparseEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.sparse");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_SPARSE: EncodingRef = &SparseEncoding;

impl Encoding for SparseEncoding {
    fn id(&self) -> &EncodingId {
        &Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use itertools::Itertools;

    use crate::array::sparse::SparseArray;
    use crate::array::Array;
    use crate::compute::scalar_at::scalar_at;
    use crate::error::VortexError;

    fn sparse_array() -> SparseArray {
        // merged array: [null, null, 100, null, null, 200, null, null, 300, null]
        SparseArray::new(vec![2u64, 5, 8].into(), vec![100i32, 200, 300].into(), 10)
    }

    fn assert_sparse_array(sparse: &dyn Array, values: &[Option<i32>]) {
        let sparse_arrow = sparse
            .as_ref()
            .iter_arrow()
            .next()
            .unwrap()
            .as_primitive::<Int32Type>()
            .into_iter()
            .collect_vec();
        assert_eq!(sparse_arrow, values);
    }

    #[test]
    pub fn iter() {
        assert_sparse_array(
            sparse_array().as_ref(),
            &[
                None,
                None,
                Some(100),
                None,
                None,
                Some(200),
                None,
                None,
                Some(300),
                None,
            ],
        );
    }

    #[test]
    pub fn iter_sliced() {
        assert_sparse_array(
            sparse_array().slice(2, 7).unwrap().as_ref(),
            &[Some(100), None, None, Some(200), None],
        );
    }

    #[test]
    pub fn iter_sliced_twice() {
        let sliced_once = sparse_array().slice(1, 8).unwrap();
        assert_sparse_array(
            sliced_once.as_ref(),
            &[None, Some(100), None, None, Some(200), None, None],
        );
        assert_sparse_array(
            sliced_once.slice(1, 6).unwrap().as_ref(),
            &[Some(100), None, None, Some(200), None],
        );
    }

    #[test]
    pub fn test_scalar_at() {
        assert_eq!(
            usize::try_from(scalar_at(sparse_array().as_ref(), 2).unwrap()).unwrap(),
            100
        );
        assert_eq!(
            scalar_at(sparse_array().as_ref(), 10).err().unwrap(),
            VortexError::OutOfBounds(10, 0, 10)
        );
    }

    #[test]
    pub fn scalar_at_sliced() {
        let sliced = sparse_array().slice(2, 7).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(sliced.as_ref(), 0).unwrap()).unwrap(),
            100
        );
        assert_eq!(
            scalar_at(sliced.as_ref(), 5).err().unwrap(),
            VortexError::OutOfBounds(5, 0, 5)
        );
    }

    #[test]
    pub fn scalar_at_sliced_twice() {
        let sliced_once = sparse_array().slice(1, 8).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(sliced_once.as_ref(), 1).unwrap()).unwrap(),
            100
        );
        assert_eq!(
            scalar_at(sliced_once.as_ref(), 7).err().unwrap(),
            VortexError::OutOfBounds(7, 0, 7)
        );

        let sliced_twice = sliced_once.slice(1, 6).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(sliced_twice.as_ref(), 3).unwrap()).unwrap(),
            200
        );
        assert_eq!(
            scalar_at(sliced_twice.as_ref(), 5).err().unwrap(),
            VortexError::OutOfBounds(5, 0, 5)
        );
    }
}
