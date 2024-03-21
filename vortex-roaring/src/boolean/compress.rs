use croaring::Bitmap;

use vortex::array::bool::{BoolArray, BoolEncoding};
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::error::VortexResult;
use vortex_schema::DType;
use vortex_schema::Nullability::NonNullable;

use crate::boolean::{RoaringBoolArray, RoaringBoolEncoding};

impl EncodingCompression for RoaringBoolEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support bool enc arrays
        if array.encoding().id() != &BoolEncoding::ID {
            return None;
        }

        // Only support non-nullable bool arrays
        if array.dtype() != &DType::Bool(NonNullable) {
            return None;
        }

        if array.len() > u32::MAX as usize {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        _like: Option<&dyn Array>,
        _ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        Ok(roaring_encode(array.as_bool()).boxed())
    }
}

pub fn roaring_encode(bool_array: &BoolArray) -> RoaringBoolArray {
    let mut bitmap = Bitmap::new();
    bitmap.extend(
        bool_array
            .buffer()
            .iter()
            .enumerate()
            .filter(|(_, b)| *b)
            .map(|(i, _)| i as u32),
    );
    bitmap.run_optimize();
    bitmap.shrink_to_fit();

    RoaringBoolArray::new(bitmap, bool_array.buffer().len())
}
