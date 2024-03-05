use crate::array::primitive::PrimitiveEncoding;
use crate::array::{Array, ArrayRef};
use crate::compress::{
    sampled_compression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};

impl EncodingCompression for PrimitiveEncoding {
    fn compressor(
        &self,
        _array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        None
        // if array.encoding().id() == &Self::ID {
        //     Some(&(primitive_compressor as Compressor))
        // } else {
        //     None
        // }
    }
}

#[allow(dead_code)]
fn primitive_compressor(
    array: &dyn Array,
    _like: Option<&dyn Array>,
    ctx: CompressCtx,
) -> ArrayRef {
    sampled_compression(array, ctx)
}

#[cfg(test)]
mod test {
    use crate::array::constant::ConstantEncoding;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::Encoding;
    use crate::compress::CompressCtx;
    use crate::compute::scalar_at::scalar_at;

    #[test]
    pub fn compress_constant() {
        let arr = PrimitiveArray::from_vec(vec![1, 1, 1, 1]);
        let res = CompressCtx::default().compress(arr.as_ref(), None);
        assert_eq!(res.encoding().id(), ConstantEncoding.id());
        assert_eq!(scalar_at(res.as_ref(), 3).unwrap().try_into(), Ok(1));
    }
}
