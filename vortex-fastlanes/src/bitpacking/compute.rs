use itertools::Itertools;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compute::as_contiguous::as_contiguous;
use vortex::compute::flatten::{flatten_primitive, FlattenFn, FlattenedArray};
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::match_each_integer_ptype;
use vortex::scalar::Scalar;
use vortex_error::{vortex_err, VortexResult};

use crate::bitpacking::compress::{unpack, unpack_single};
use crate::downcast::DowncastFastlanes;
use crate::BitPackedArray;

impl ArrayCompute for BitPackedArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl FlattenFn for BitPackedArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        unpack(self).map(FlattenedArray::Primitive)
    }
}

impl ScalarAtFn for BitPackedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if index >= self.len() {
            return Err(vortex_err!(OutOfBounds:index, 0, self.len()));
        }

        if let Some(patches) = self.patches() {
            // NB: All non-null values are considered patches
            if self.bit_width == 0 || patches.is_valid(index) {
                return scalar_at(patches, index)?.cast(self.dtype());
            }
        }
        unpack_single(self, index)?.cast(self.dtype())
    }
}

impl TakeFn for BitPackedArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let prim_indices = flatten_primitive(indices)?;
        // Group indices into 1024 chunks and relativise them to the beginning of each chunk
        let relative_indices: Vec<(usize, Vec<u16>)> = match_each_integer_ptype!(prim_indices.ptype(), |$P| {
            let grouped_indices = prim_indices
                .typed_data::<$P>()
                .iter()
                .group_by(|idx| (**idx / 1024) as usize);
            grouped_indices
                .into_iter()
                .map(|(k, g)| (k, g.map(|idx| (*idx % 1024) as u16).collect()))
                .collect()
        });

        let taken = relative_indices
            .into_iter()
            .map(|(chunk, offsets)| {
                let sliced = self.slice(chunk * 1024, (chunk + 1) * 1024)?;

                take(
                    &unpack(sliced.as_bitpacked())?,
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

    use itertools::Itertools;
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding};
    use vortex::array::Array;
    use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
    use vortex::compute::scalar_at::scalar_at;
    use vortex::compute::take::take;
    use vortex::encoding::EncodingRef;
    use vortex::scalar::Scalar;

    use crate::downcast::DowncastFastlanes;
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

    #[test]
    fn test_scalar_at() {
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let values = (0u32..257).collect_vec();
        let uncompressed = PrimitiveArray::from(values.clone()).into_array();
        let packed = BitPackedEncoding
            .compress(&uncompressed, None, ctx)
            .unwrap();
        let packed = packed.as_bitpacked();
        assert!(packed.patches().is_some());

        let patches = packed.patches().unwrap().as_sparse();
        assert_eq!(patches.resolved_indices(), vec![256]);

        values.iter().enumerate().for_each(|(i, v)| {
            assert_eq!(scalar_at(packed, i).unwrap(), Scalar::from(*v));
        });
    }
}
