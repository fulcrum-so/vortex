use vortex_error::VortexResult;

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::varbin::{VarBinArray, VarBinEncoding};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, EncodingCompression};
use crate::validity::ArrayValidity;

impl EncodingCompression for VarBinEncoding {
    fn cost(&self) -> u8 {
        0 // We simply destructure.
    }

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
        let vb = array.as_varbin();
        let vblike = like.map(|a| a.as_varbin());
        Ok(VarBinArray::new(
            ctx.auxiliary("offsets")
                .compress(vb.offsets(), vblike.map(|l| l.offsets()))?,
            vb.bytes().clone(),
            vb.dtype().clone(),
            ctx.compress_validity(vb.validity())?,
        )
        .into_array())
    }
}
