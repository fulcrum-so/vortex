use std::sync::{RwLock};

use compress::roaring_encode;
use croaring::{Bitmap};
use serde::{Deserialize, Serialize};
use vortex::ptype::PType;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::{impl_encoding, OwnedArray};
use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::compute::ArrayCompute;
use vortex::compute::scalar_at::ScalarAtFn;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

mod compress;
mod compute;

impl_encoding!("vortex.roaring_int", RoaringInt);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoaringIntMetadata {
    bitmap: Bitmap,
    ptype: PType,
    stats: Arc<RwLock<StatsSet>>,
}

impl RoaringIntArray<'_> {
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
        &self.metadata().bitmap
    }

    pub fn ptype(&self) -> PType {
        self.metadata().ptype
    }

    pub fn encode(array: Array) -> VortexResult<OwnedArray> {
        if array.encoding().id() == Primitive::ID {
            Ok(roaring_encode(PrimitiveArray::try_from(array)?).into_array())
        } else {
            Err(vortex_err!("RoaringInt can only encode primitive arrays"))
        }
    }
}

impl ArrayValidity for RoaringIntArray<'_> {
    fn logical_validity(&self) -> LogicalValidity {
        LogicalValidity::AllValid(self.metadata().bitmap.iter().count())
    }

    fn is_valid(&self, _index: usize) -> bool {
        true
    }
}

impl ArrayCompute for RoaringIntArray<'_> {}

impl ScalarAtFn for RoaringIntArray<'_> {
    fn scalar_at(&self, _index: usize) -> VortexResult<Scalar> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::scalar_at::scalar_at;
    use vortex::IntoArray;
    use vortex_error::VortexResult;

    use crate::RoaringIntArray;

    #[test]
    pub fn test_scalar_at() -> VortexResult<()> {
        let ints = PrimitiveArray::from(vec![2u32, 12, 22, 32]).into_array();
        let array = RoaringIntArray::encode(ints)?;

        assert_eq!(scalar_at(&array, 0).unwrap(), 2u32.into());
        assert_eq!(scalar_at(&array, 1).unwrap(), 12u32.into());

        Ok(())
    }
}
