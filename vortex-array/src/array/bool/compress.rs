use crate::array::bool::BoolEncoding;
use crate::array::{Array, ArrayRef};
use crate::compress::{
    sampled_compression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};

impl EncodingCompression for BoolEncoding {
    fn compressor(
        &self,
        _array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        None
        // if array.encoding().id() == &BoolEncoding::ID {
        //     Some(&(bool_compressor as Compressor))
        // } else {
        //     None
        // }
    }
}

#[allow(dead_code)]
fn bool_compressor(array: &dyn Array, _like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    sampled_compression(array, ctx)
}
