use std::sync::{RwLock};

use compress::roaring_encode;
use croaring::Bitmap;
use serde::{Deserialize, Serialize};
use vortex::encoding::{ArrayEncodingRef};
use vortex::stats::{ArrayStatistics, ArrayStatisticsCompute};
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::{impl_encoding, ArrayFlatten, ArrayDType, ToArrayData, OwnedArray};
// use vortex::array::bool::BoolArray;
use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex_error::{vortex_err, VortexResult};

mod compress;
mod compute;


impl_encoding!("vortex.roaring_bool", RoaringBool);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoaringBoolMetadata {
    bitmap: Bitmap,
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl RoaringBoolArray<'_> {
    pub fn new(bitmap: Bitmap, length: usize) -> Self {
        Self {
            bitmap,
            length,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    pub fn bitmap(&self) -> &Bitmap {
        &self.metadata().bitmap
    }

    pub fn encode(array: Array) -> VortexResult<OwnedArray> {
        if array.encoding().id() == Primitive::ID {
            Ok(roaring_encode(PrimitiveArray::try_from(array)?).into_array())
        } else {
            Err(vortex_err!("RoaringInt can only encode primitive arrays"))
        }
    }
}
impl AcceptArrayVisitor for RoaringBoolArray<'_> {
    fn accept(&self, _visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        todo!()
    }
}

impl ToArrayData for RoaringBoolArray<'_> {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}

impl ArrayTrait for RoaringBoolArray<'_> {
    fn len(&self) -> usize {
        todo!()
    }
}

impl ArrayStatisticsCompute for RoaringBoolArray<'_> {}

impl ArrayValidity for RoaringBoolArray<'_> {
    fn logical_validity(&self) -> LogicalValidity {
        LogicalValidity::AllValid(self.len())
    }

    fn is_valid(&self, _index: usize) -> bool {
        true
    }
}

// impl ArrayDisplay for RoaringBoolArray {
//     fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
//         f.property("bitmap", format!("{:?}", self.bitmap()))
//     }
// }

impl ArrayFlatten for RoaringBoolArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
        where
            Self: 'a,
    {
        todo!()
        // decompress(self).map(Flattened::Primitive)
    }
}

#[cfg(test)]
mod test {
    use vortex::Array;
    use vortex::array::bool::BoolArray;
    use vortex::compute::scalar_at::scalar_at;
    use vortex::scalar::Scalar;
    use vortex_error::VortexResult;

    use crate::RoaringBoolArray;

    #[test]
    pub fn iter() -> VortexResult<()> {
        let bool: Array = &BoolArray::from(vec![true, false, true, true]);
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
