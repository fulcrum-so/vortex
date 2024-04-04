use std::sync::{Arc, RwLock};

use compress::roaring_encode;
use croaring::{Bitmap, Native};
use vortex::array::validity::Validity;
use vortex::array::{check_slice_bounds, Array, ArrayKind, ArrayRef};
use vortex::compress::EncodingCompression;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::ptype::PType;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};
use vortex::{impl_array, impl_array_compute, ArrayWalker};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
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
            vortex_bail!("RoaringInt expected unsigned int");
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
            _ => Err(vortex_err!("RoaringInt can only encode primitive arrays")),
        }
    }
}

impl Array for RoaringIntArray {
    impl_array!();
    impl_array_compute!();

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

    fn validity(&self) -> Option<Validity> {
        match self.dtype().is_nullable() {
            true => Some(Validity::Valid(self.len())),
            false => None,
        }
    }

    fn walk(&self, _walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        todo!()
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
    use vortex_error::VortexResult;

    use crate::RoaringIntArray;

    #[test]
    pub fn test_scalar_at() -> VortexResult<()> {
        let ints = PrimitiveArray::from(vec![2u32, 12, 22, 32]);
        let array = RoaringIntArray::encode(&ints)?;

        assert_eq!(scalar_at(&array, 0).unwrap(), 2u32.into());
        assert_eq!(scalar_at(&array, 1).unwrap(), 12u32.into());

        Ok(())
    }
}
