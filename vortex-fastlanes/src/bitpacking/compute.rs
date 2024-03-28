use itertools::Itertools;

use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compute::as_contiguous::as_contiguous;
use vortex::compute::flatten::{flatten_primitive, FlattenFn, FlattenedArray};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::match_each_integer_ptype;
use vortex_error::VortexResult;

use crate::bitpacking::compress::bitunpack;
use crate::downcast::DowncastFastlanes;
use crate::BitPackedArray;

impl ArrayCompute for BitPackedArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl FlattenFn for BitPackedArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        bitunpack(self).map(FlattenedArray::Primitive)
    }
}

impl TakeFn for BitPackedArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let prim_indices = flatten_primitive(indices)?;
        // Group indices into 1024 chunks and relativise them to the beginning of each chunk
        let relative_indices: Vec<(usize, Vec<u16>)> = match_each_integer_ptype!(prim_indices.ptype(), |$P| {
            let groupped_indices = prim_indices
                .typed_data::<$P>()
                .iter()
                .group_by(|idx| (**idx / 1024) as usize);
            groupped_indices
                .into_iter()
                .map(|(k, g)| (k, g.map(|idx| (*idx % 1024) as u16).collect()))
                .collect()
        });

        let taken = relative_indices
            .into_iter()
            .map(|(chunk, offsets)| {
                let sliced = self.slice(chunk * 1024, (chunk + 1) * 1024)?;

                take(
                    &bitunpack(sliced.as_bitpacked())?,
                    &PrimitiveArray::from(offsets),
                )
            })
            .collect::<VortexResult<Vec<_>>>()?;
        as_contiguous(&taken)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding};
    use vortex::array::Array;
    use vortex::compress::{CompressConfig, CompressCtx};
    use vortex::compute::take::take;
    use vortex::encoding::EncodingRef;

    use crate::BitPackedEncoding;

    #[test]
    fn take_indices() {
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let indices = PrimitiveArray::from(vec![0, 125, 2047, 2049, 2151, 2790]);
        let unpacked = PrimitiveArray::from((0..4096).map(|i| (i % 63) as u8).collect::<Vec<_>>());
        let bitpacked = ctx.compress(&unpacked, None).unwrap();
        let result = take(&bitpacked, &indices).unwrap();
        assert_eq!(result.encoding().id(), PrimitiveEncoding::ID);
        let res_bytes = result.as_primitive().typed_data::<u8>();
        assert_eq!(res_bytes, &[0, 62, 31, 33, 9, 18]);
    }
}
