use vortex::array::primitive::PrimitiveArray;
use vortex::stats::{trailing_zeros, ArrayStatistics};
use vortex::validity::ArrayValidity;
use vortex::{Array, ArrayDef, IntoArray};
use vortex_dtype::match_each_integer_ptype;
use vortex_error::VortexResult;
use vortex_fastlanes::{for_compress, FoR, FoRArray};

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct FoRCompressor;

impl EncodingCompressor for FoRCompressor {
    fn id(&self) -> &str {
        FoR::ID.as_ref()
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
        // Only support primitive arrays
        let parray = PrimitiveArray::try_from(array).ok()?;

        // Only supports integers
        if !parray.ptype().is_int() {
            return None;
        }

        // For all-null, cannot encode.
        if parray.logical_validity().all_invalid() {
            return None;
        }

        // Nothing for us to do if the min is already zero and tz == 0
        let shift = trailing_zeros(array);
        match_each_integer_ptype!(parray.ptype(), |$P| {
            let min: $P = parray.statistics().compute_min()?;
            if min == 0 && shift == 0 {
                return None;
            }
        });

        Some(self)
    }

    fn compress<'a>(
        &'a self,
        array: &Array,
        like: Option<CompressionTree<'a>>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        let (child, min, shift) = for_compress(&PrimitiveArray::try_from(array)?)?;

        let compressed_child = ctx
            .named("for")
            .excluding(self)
            .compress(&child, like.as_ref().and_then(|l| l.child(0)))?;
        Ok(CompressedArray::new(
            FoRArray::try_new(compressed_child.array, min, shift).map(|a| a.into_array())?,
            Some(CompressionTree::new(self, vec![compressed_child.path])),
        ))
    }
}
