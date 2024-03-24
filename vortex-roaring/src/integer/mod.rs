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
use vortex::ptype::PType;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};
use vortex_schema::DType;

mod compress;
mod compute;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct RoaringIntArray {
    bitmap: Bitmap,
    ptype: PType,
    stats: Arc<RwLock<StatsSet>>,
}

impl RoaringIntArray {
    pub fn new(bitmap: Bitmap, ptype: PType) -> Self {
        Self::try_new(bitmap, ptype).unwrap()
    }

    pub fn try_new(bitmap: Bitmap, ptype: PType) -> VortexResult<Self> {
        if !ptype.is_unsigned_int() {
            return Err(VortexError::InvalidPType(ptype));
        }

        Ok(Self {
            bitmap,
            ptype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub fn bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    pub fn ptype(&self) -> PType {
        self.ptype
    }

    pub fn encode(array: &dyn Array) -> VortexResult<Self> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => Ok(roaring_encode(p)),
            _ => Err(VortexError::InvalidEncoding(array.encoding().id())),
        }
    }
}

impl Array for RoaringIntArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.bitmap.cardinality() as usize
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.bitmap().is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.ptype.into()
    }

    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;
        todo!()
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &RoaringIntEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.bitmap.get_serialized_size_in_bytes::<Native>()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl ArrayDisplay for RoaringIntArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("bitmap", format!("{:?}", self.bitmap()))
    }
}

#[derive(Debug)]
pub struct RoaringIntEncoding;

impl RoaringIntEncoding {
    pub const ID: EncodingId = EncodingId::new("roaring.int");
}

impl Encoding for RoaringIntEncoding {
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
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::scalar_at::scalar_at;
    use vortex::error::VortexResult;

    use crate::RoaringIntArray;

    #[test]
    pub fn test_scalar_at() -> VortexResult<()> {
        let ints = PrimitiveArray::from(vec![2u32, 12, 22, 32]);
        let array = RoaringIntArray::encode(&ints)?;

        assert_eq!(scalar_at(&array, 0), Ok(2u32.into()));
        assert_eq!(scalar_at(&array, 1), Ok(12u32.into()));

        Ok(())
    }
}
