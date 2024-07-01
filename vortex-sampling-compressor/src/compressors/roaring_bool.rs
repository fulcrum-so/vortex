use vortex::{Array, ArrayDType, ArrayDef, IntoArray, IntoArrayVariant};
use vortex_dtype::DType;
use vortex_dtype::Nullability::NonNullable;
use vortex_error::VortexResult;
use vortex_roaring::{roaring_bool_encode, RoaringBool};

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct RoaringBoolCompressor;

impl EncodingCompressor for RoaringBoolCompressor {
    fn id(&self) -> &str {
        RoaringBool::ID.as_ref()
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
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

    fn compress<'a>(
        &'a self,
        array: &Array,
        _like: Option<CompressionTree<'a>>,
        _ctx: SamplingCompressor,
    ) -> VortexResult<CompressedArray<'a>> {
        Ok(CompressedArray::new(
            roaring_bool_encode(array.clone().into_bool()?)?.into_array(),
            Some(CompressionTree::flat(self)),
        ))
    }
}
