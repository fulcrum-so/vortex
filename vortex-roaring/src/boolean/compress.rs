use croaring::Bitmap;
use vortex::{Array, ArrayDef, ArrayDType, IntoArray};
// use vortex::array::bool::{BoolArray, BoolEncoding};
use vortex::array::primitive::PrimitiveArray;
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex_error::VortexResult;
use vortex_schema::DType;
use vortex_schema::Nullability::NonNullable;

use crate::boolean::{RoaringBoolArray};
use crate::{RoaringBool, RoaringBoolEncoding};

impl EncodingCompression for RoaringBoolEncoding {
    fn can_compress(
        &self,
        array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support bool enc arrays
        if array.encoding().id() != RoaringBool::ID {
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
        array: &Array,
        _like: Option<&Array>,
        _ctx: CompressCtx,
    ) -> VortexResult<Array<'static>> {
        Ok(roaring_encode(array.clone().flatten_primitive()?).into_array())
    }
}

pub fn roaring_encode(bool_array: PrimitiveArray) -> RoaringBoolArray {
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
