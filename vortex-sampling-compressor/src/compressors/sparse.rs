use vortex::array::sparse::{Sparse, SparseArray};
use vortex::{Array, ArrayDef, ArrayTrait, IntoArray};
use vortex_error::VortexResult;

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct SparseCompressor;

impl EncodingCompressor for SparseCompressor {
    fn id(&self) -> &str {
        Sparse::ID.as_ref()
    }

    fn cost(&self) -> u8 {
        0
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
        (array.encoding().id() == Sparse::ID).then_some(self)
    }

    fn compress<'a>(
        &'a self,
        array: &Array,
        like: Option<CompressionTree<'a>>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        let sparse_array = SparseArray::try_from(array)?;
        let indices = ctx.auxiliary("indices").compress(
            &sparse_array.indices(),
            like.as_ref().and_then(|l| l.child(0)),
        )?;
        let values = ctx.named("values").compress(
            &sparse_array.values(),
            like.as_ref().and_then(|l| l.child(0)),
        )?;
        Ok(CompressedArray::new(
            SparseArray::try_new(
                indices.array,
                values.array,
                sparse_array.len(),
                sparse_array.fill_value().clone(),
            )?
            .into_array(),
            Some(CompressionTree::new(self, vec![indices.path, values.path])),
        ))
    }
}
