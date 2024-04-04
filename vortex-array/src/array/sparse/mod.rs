use std::sync::{Arc, RwLock};

use itertools::Itertools;
use linkme::distributed_slice;
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::DType;

use crate::array::validity::Validity;
use crate::array::{check_slice_bounds, Array, ArrayRef};
use crate::compress::EncodingCompression;
use crate::compute::cast::cast;
use crate::compute::flatten::flatten_primitive;
use crate::compute::search_sorted::{search_sorted, SearchSortedSide};
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::ptype::PType;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsCompute, StatsSet};
use crate::{impl_array, impl_array_compute, ArrayWalker};

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
            vortex_bail!("Cannot use {} as indices", indices.dtype());
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
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    #[inline]
    pub fn indices(&self) -> &ArrayRef {
        &self.indices
    }

    /// Return indices as a vector of usize with the indices_offset applied.
    pub fn resolved_indices(&self) -> Vec<usize> {
        flatten_primitive(cast(self.indices(), PType::U64.into()).unwrap().as_ref())
            .unwrap()
            .typed_data::<u64>()
            .iter()
            .map(|v| (*v as usize) - self.indices_offset)
            .collect_vec()
    }
}

impl Array for SparseArray {
    impl_array!();
    impl_array_compute!();

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

    fn validity(&self) -> Option<Validity> {
        todo!()
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
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &SparseEncoding
    }

    fn nbytes(&self) -> usize {
        self.indices.nbytes() + self.values.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.indices())?;
        walker.visit_child(self.values())
    }
}

impl StatsCompute for SparseArray {}

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
    fn id(&self) -> EncodingId {
        Self::ID
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
    use itertools::Itertools;
    use vortex_error::VortexError;

    use crate::array::sparse::SparseArray;
    use crate::array::Array;
    use crate::array::IntoArray;
    use crate::compute::flatten::flatten_primitive;
    use crate::compute::scalar_at::scalar_at;

    fn sparse_array() -> SparseArray {
        // merged array: [null, null, 100, null, null, 200, null, null, 300, null]
        SparseArray::new(
            vec![2u64, 5, 8].into_array(),
            vec![100i32, 200, 300].into_array(),
            10,
        )
    }

    fn assert_sparse_array(sparse: &dyn Array, values: &[Option<i32>]) {
        let sparse_arrow = flatten_primitive(sparse)
            .unwrap()
            .iter::<i32>()
            .collect_vec();
        assert_eq!(sparse_arrow, values);
    }

    #[test]
    pub fn iter() {
        assert_sparse_array(
            &sparse_array(),
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
            usize::try_from(scalar_at(&sparse_array(), 2).unwrap()).unwrap(),
            100
        );
        let error = scalar_at(&sparse_array(), 10).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error else {
            unreachable!()
        };
        assert_eq!(i, 10);
        assert_eq!(start, 0);
        assert_eq!(stop, 10);
    }

    #[test]
    pub fn scalar_at_sliced() {
        let sliced = sparse_array().slice(2, 7).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(sliced.as_ref(), 0).unwrap()).unwrap(),
            100
        );
        let error = scalar_at(sliced.as_ref(), 5).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error else {
            unreachable!()
        };
        assert_eq!(i, 5);
        assert_eq!(start, 0);
        assert_eq!(stop, 5);
    }

    #[test]
    pub fn scalar_at_sliced_twice() {
        let sliced_once = sparse_array().slice(1, 8).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(sliced_once.as_ref(), 1).unwrap()).unwrap(),
            100
        );
        let error = scalar_at(sliced_once.as_ref(), 7).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error else {
            unreachable!()
        };
        assert_eq!(i, 7);
        assert_eq!(start, 0);
        assert_eq!(stop, 7);

        let sliced_twice = sliced_once.slice(1, 6).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(sliced_twice.as_ref(), 3).unwrap()).unwrap(),
            200
        );
        let error2 = scalar_at(sliced_twice.as_ref(), 5).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error2 else {
            unreachable!()
        };
        assert_eq!(i, 5);
        assert_eq!(start, 0);
        assert_eq!(stop, 5);
    }
}
