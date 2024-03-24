use std::sync::{Arc, RwLock};

use croaring::{Bitmap, Native};

use compress::roaring_encode;
use vortex::array::{
    check_slice_bounds, Array, ArrayKind, ArrayRef, Encoding, EncodingId, EncodingRef,
};
use vortex::compress::EncodingCompression;
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::impl_array;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};
use vortex_schema::DType;
use vortex_schema::Nullability::NonNullable;

mod compress;
mod compute;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct RoaringBoolArray {
    bitmap: Bitmap,
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl RoaringBoolArray {
    pub fn new(bitmap: Bitmap, length: usize) -> Self {
        Self {
            bitmap,
            length,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    pub fn bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    pub fn encode(array: &dyn Array) -> VortexResult<Self> {
        match ArrayKind::from(array) {
            ArrayKind::Bool(p) => Ok(roaring_encode(p)),
            _ => Err(VortexError::InvalidEncoding(array.encoding().id())),
        }
    }
}

impl Array for RoaringBoolArray {
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
        &DType::Bool(NonNullable)
    }

    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        let slice_bitmap = Bitmap::from_range(start as u32..stop as u32);
        let bitmap = self.bitmap.and(&slice_bitmap).add_offset(-(start as i64));

        Ok(Self {
            bitmap,
            length: stop - start,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &RoaringBoolEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        // TODO(ngates): do we want Native serializer? Or portable? Or frozen?
        self.bitmap.get_serialized_size_in_bytes::<Native>()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl ArrayDisplay for RoaringBoolArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("bitmap", format!("{:?}", self.bitmap()))
    }
}

#[derive(Debug)]
pub struct RoaringBoolEncoding;

impl RoaringBoolEncoding {
    pub const ID: EncodingId = EncodingId::new("roaring.bool");
}

impl Encoding for RoaringBoolEncoding {
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
    use vortex::array::bool::BoolArray;
    use vortex::array::Array;
    use vortex::compute::scalar_at::scalar_at;
    use vortex::error::VortexResult;
    use vortex::scalar::Scalar;

    use crate::RoaringBoolArray;

    #[test]
    pub fn iter() -> VortexResult<()> {
        let bool: &dyn Array = &BoolArray::from(vec![true, false, true, true]);
        let array = RoaringBoolArray::encode(bool)?;

        let values = array.bitmap().to_vec();
        assert_eq!(values, vec![0, 2, 3]);

        Ok(())
    }

    #[test]
    pub fn test_scalar_at() -> VortexResult<()> {
        let bool: &dyn Array = &BoolArray::from(vec![true, false, true, true]);
        let array = RoaringBoolArray::encode(bool)?;

        let truthy: Scalar = true.into();
        let falsy: Scalar = false.into();

        assert_eq!(scalar_at(&array, 0)?, truthy);
        assert_eq!(scalar_at(&array, 1)?, falsy);
        assert_eq!(scalar_at(&array, 2)?, truthy);
        assert_eq!(scalar_at(&array, 3)?, truthy);

        Ok(())
    }
}
