use croaring::Bitmap;
use vortex::array::bool::BoolArray;
use vortex::ArrayTrait;
use vortex_error::VortexResult;

use crate::RoaringBoolArray;

pub fn roaring_bool_encode(bool_array: BoolArray) -> VortexResult<RoaringBoolArray> {
    let mut bitmap = Bitmap::new();
    bitmap.extend(
        bool_array
            .boolean_buffer()
            .iter()
            .enumerate()
            .filter(|(_, b)| *b)
            .map(|(i, _)| i as u32),
    );
    bitmap.run_optimize();
    bitmap.shrink_to_fit();

    RoaringBoolArray::try_new(bitmap, bool_array.len())
}
