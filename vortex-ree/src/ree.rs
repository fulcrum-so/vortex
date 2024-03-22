use std::sync::{Arc, RwLock};

use vortex::array::{
    check_slice_bounds, check_validity_buffer, Array, ArrayKind, ArrayRef, Encoding, EncodingId,
    EncodingRef,
};
use vortex::compress::EncodingCompression;
use vortex::compute::search_sorted::SearchSortedSide;
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};
use vortex::{compute, impl_array};
use vortex_schema::DType;

use crate::compress::ree_encode;

#[derive(Debug, Clone)]
pub struct REEArray {
    ends: ArrayRef,
    values: ArrayRef,
    validity: Option<ArrayRef>,
    offset: usize,
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl REEArray {
    pub fn new(
        ends: ArrayRef,
        values: ArrayRef,
        validity: Option<ArrayRef>,
        length: usize,
    ) -> Self {
        Self::try_new(ends, values, validity, length).unwrap()
    }

    pub fn try_new(
        ends: ArrayRef,
        values: ArrayRef,
        validity: Option<ArrayRef>,
        length: usize,
    ) -> VortexResult<Self> {
        check_validity_buffer(validity.as_ref(), length)?;

        if !ends
            .stats()
            .get_as::<bool>(&Stat::IsStrictSorted)
            .unwrap_or(true)
        {
            return Err(VortexError::IndexArrayMustBeStrictSorted);
        }

        // TODO(ngates): https://github.com/fulcrum-so/spiral/issues/873
        Ok(Self {
            ends,
            values,
            validity,
            length,
            offset: 0,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub fn find_physical_index(&self, index: usize) -> VortexResult<usize> {
        compute::search_sorted::search_sorted(
            self.ends(),
            index + self.offset,
            SearchSortedSide::Right,
        )
    }

    pub fn encode(array: &dyn Array) -> VortexResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => {
                let (ends, values) = ree_encode(p);
                Ok(REEArray::new(
                    ends.into_array(),
                    values.into_array(),
                    p.validity().cloned(),
                    p.len(),
                )
                .into_array())
            }
            _ => Err(VortexError::InvalidEncoding(array.encoding().id().clone())),
        }
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    #[inline]
    pub fn ends(&self) -> &ArrayRef {
        &self.ends
    }

    #[inline]
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    #[inline]
    pub fn validity(&self) -> Option<&ArrayRef> {
        self.validity.as_ref()
    }
}

impl Array for REEArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.length
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.length == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.values.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;
        let slice_begin = self.find_physical_index(start)?;
        let slice_end = self.find_physical_index(stop)?;
        Ok(Self {
            ends: self.ends.slice(slice_begin, slice_end + 1)?,
            values: self.values.slice(slice_begin, slice_end + 1)?,
            validity: self
                .validity()
                .map(|v| v.slice(slice_begin, slice_end + 1))
                .transpose()?,
            offset: start,
            length: stop - start,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &REEEncoding
    }

    #[inline]
    // Values and ends have been sliced to the nearest run end value so the size in bytes is accurate
    fn nbytes(&self) -> usize {
        self.values.nbytes() + self.ends.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl StatsCompute for REEArray {}

#[derive(Debug)]
pub struct REEEncoding;

impl REEEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.ree");
}

impl Encoding for REEEncoding {
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

impl ArrayDisplay for REEArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("values", self.values())?;
        f.child("ends", self.ends())
    }
}

#[cfg(test)]
mod test {
    use vortex::array::Array;
    use vortex::array::IntoArray;
    use vortex::compute::flatten::flatten_primitive;
    use vortex::compute::scalar_at::scalar_at;
    use vortex_schema::{DType, IntWidth, Nullability, Signedness};

    use crate::REEArray;

    #[test]
    fn new() {
        let arr = REEArray::new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            None,
            10,
        );
        assert_eq!(arr.len(), 10);
        assert_eq!(
            arr.dtype(),
            &DType::Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable)
        );

        // 0, 1 => 1
        // 2, 3, 4 => 2
        // 5, 6, 7, 8, 9 => 3
        assert_eq!(scalar_at(&arr, 0).unwrap().try_into(), Ok(1));
        assert_eq!(scalar_at(&arr, 2).unwrap().try_into(), Ok(2));
        assert_eq!(scalar_at(&arr, 5).unwrap().try_into(), Ok(3));
        assert_eq!(scalar_at(&arr, 9).unwrap().try_into(), Ok(3));
    }

    #[test]
    fn slice() {
        let arr = REEArray::new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            None,
            10,
        )
        .slice(3, 8)
        .unwrap();
        assert_eq!(
            arr.dtype(),
            &DType::Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable)
        );
        assert_eq!(arr.len(), 5);

        assert_eq!(
            flatten_primitive(&arr).unwrap().typed_data::<i32>(),
            vec![2, 2, 3, 3, 3]
        );
    }

    #[test]
    fn flatten() {
        let arr = REEArray::new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            None,
            10,
        );
        assert_eq!(
            flatten_primitive(&arr).unwrap().typed_data::<i32>(),
            vec![1, 1, 2, 2, 2, 3, 3, 3, 3, 3]
        );
    }
}
