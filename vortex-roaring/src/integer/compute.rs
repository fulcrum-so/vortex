use vortex::compute::scalar_at::ScalarAtFn;
use vortex::compute::ArrayCompute;
use vortex::ptype::PType;
use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::RoaringIntArray;

impl ArrayCompute for RoaringIntArray<'_> {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for RoaringIntArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        // Unwrap since we know the index is valid
        let bitmap_value = self.bitmap.select(index as u32).unwrap();
        let scalar: Scalar = match self.ptype {
            PType::U8 => (bitmap_value as u8).into(),
            PType::U16 => (bitmap_value as u16).into(),
            PType::U32 => bitmap_value.into(),
            PType::U64 => (bitmap_value as u64).into(),
            _ => unreachable!("RoaringIntArray constructor should have disallowed this type"),
        };
        Ok(scalar)
    }
}
