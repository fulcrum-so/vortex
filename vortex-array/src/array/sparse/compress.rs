use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::sparse::{SparseArray, SparseEncoding};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, EncodingCompression};
use crate::error::VortexResult;

impl EncodingCompression for SparseEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        (array.encoding().id() == &Self::ID).then_some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let sparse_array = array.as_sparse();
        let sparse_like = like.map(|la| la.as_sparse());
        Ok(SparseArray::new(
            ctx.compress(sparse_array.indices(), sparse_like.map(|sa| sa.indices()))?,
            ctx.compress(sparse_array.values(), sparse_like.map(|sa| sa.values()))?,
            sparse_array.len(),
        )
        .boxed())
    }
}
