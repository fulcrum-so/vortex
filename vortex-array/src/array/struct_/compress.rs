use itertools::Itertools;

use vortex_error::VortexResult;

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::struct_::{StructArray, StructEncoding};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, EncodingCompression};

impl EncodingCompression for StructEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        (array.encoding().id() == Self::ID).then_some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let struct_array = array.as_struct();
        let struct_like = like.map(|like_array| like_array.as_struct());

        let fields = struct_array
            .fields()
            .iter()
            .enumerate()
            .map(|(i, chunk)| {
                let like_chunk = struct_like.and_then(|c_like| c_like.fields().get(i));
                ctx.auxiliary(&format!("[{}]", i))
                    .compress(chunk, like_chunk)
            })
            .try_collect()?;

        Ok(StructArray::new(struct_array.names().clone(), fields, array.len()).into_array())
    }
}
