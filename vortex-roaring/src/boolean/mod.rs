use std::sync::{Arc, RwLock};

use compress::roaring_encode;
use croaring::{Bitmap, Native};
use vortex::array::{Array, ArrayKind, ArrayRef};
use vortex::compress::EncodingCompression;
use vortex::compute::ArrayCompute;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{ArrayStatistics, OwnedStats, Statistics, StatsSet};
use vortex::validity::ArrayValidity;
use vortex::validity::Validity;
use vortex::{impl_array, ArrayWalker};
use vortex_error::{vortex_err, VortexResult};
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
            stats: Arc::new(RwLock::new(StatsSet::default())),
        }
    }

    pub fn bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    pub fn encode(array: &dyn Array) -> VortexResult<Self> {
        match ArrayKind::from(array) {
            ArrayKind::Bool(p) => Ok(roaring_encode(p)),
            _ => Err(vortex_err!("RoaringBool can only encode bool arrays")),
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

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &RoaringBoolEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        // TODO(ngates): do we want Native serializer? Or portable? Or frozen?
        self.bitmap.get_serialized_size_in_bytes::<Native>()
    }

    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, _walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        // TODO(ngates): should we store a buffer in memory? Or delay serialization?
        //  Or serialize into metadata? The only reason we support buffers is so we can write to
        //  the wire without copying into FlatBuffers. But if we need to allocate to serialize
        //  the bitmap anyway, then may as well shove it into metadata.
        todo!()
    }
}

impl ArrayValidity for RoaringBoolArray {
    fn logical_validity(&self) -> Validity {
        Validity::Valid(self.len())
    }

    fn is_valid(&self, _index: usize) -> bool {
        true
    }
}

impl ArrayDisplay for RoaringBoolArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("bitmap", format!("{:?}", self.bitmap()))
    }
}

impl OwnedStats for RoaringBoolArray {
    fn stats_set(&self) -> &RwLock<StatsSet> {
        &self.stats
    }
}

impl ArrayStatistics for RoaringBoolArray {
    fn statistics(&self) -> &dyn Statistics {
        self
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
    use vortex::scalar::Scalar;
    use vortex_error::VortexResult;

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
