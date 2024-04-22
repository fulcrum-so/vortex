use croaring::Bitmap;
use vortex::{IntoArray, OwnedArray};
use vortex::compute::scalar_at::ScalarAtFn;
use vortex::compute::slice::SliceFn;
use vortex::compute::ArrayCompute;
use vortex::scalar::{Scalar};
use vortex_error::{VortexResult};

use crate::RoaringBoolArray;

impl ArrayCompute for RoaringBoolArray<'_> {
    // fn flatten(&self) -> Option<&dyn FlattenFn> {
    //     Some(self)
    // }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }
}

// impl FlattenFn for RoaringBoolArray {
//     fn flatten(&self) -> VortexResult<FlattenedArray> {
//         // TODO(ngates): benchmark the fastest conversion from BitMap.
//         //  Via bitset requires two copies.
//         let bitset = self
//             .bitmap
//             .to_bitset()
//             .ok_or(vortex_err!("Failed to convert RoaringBitmap to Bitset"))?;
//
//         let bytes = &bitset.as_slice().as_bytes()[0..bitset.size_in_bytes()];
//         let buffer = Buffer::from_slice_ref(bytes);
//         Ok(FlattenedArray::Bool(BoolArray::new(
//             BooleanBuffer::new(buffer, 0, bitset.size_in_bits()),
//             match self.nullability() {
//                 Nullability::NonNullable => None,
//                 Nullability::Nullable => Some(Validity::Valid(self.len())),
//             },
//         )))
//     }
// }

impl ScalarAtFn for RoaringBoolArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.bitmap().contains(index as u32) {
            Ok(true.into())
        } else {
            Ok(false.into())
        }
    }
}

impl SliceFn for RoaringBoolArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        let slice_bitmap = Bitmap::from_range(start as u32..stop as u32);
        let bitmap = self.bitmap().and(&slice_bitmap).add_offset(-(start as i64));

        Ok(RoaringBoolArray::new(bitmap, stop - start).into_array())
    }
}
