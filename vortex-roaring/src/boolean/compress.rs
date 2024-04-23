use croaring::Bitmap;
use vortex::array::bool::BoolArray;
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::{Array, ArrayDType, ArrayDef, ArrayTrait, IntoArray, OwnedArray};
use vortex_error::VortexResult;
use vortex_schema::DType;
use vortex_schema::Nullability::NonNullable;

use crate::boolean::RoaringBoolArray;
use crate::{OwnedRoaringBoolArray, RoaringBool, RoaringBoolEncoding};

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
    ) -> VortexResult<OwnedArray> {
        roaring_encode(array.clone().flatten_bool()?).map(move |a| a.into_array())
    }
}

pub fn roaring_encode(bool_array: BoolArray) -> VortexResult<OwnedRoaringBoolArray> {
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
