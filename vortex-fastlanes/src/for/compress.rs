use itertools::Itertools;

use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::error::VortexResult;
use vortex::match_each_integer_ptype;
use vortex::stats::Stat;

use crate::{FoRArray, FoREncoding};

impl EncodingCompression for FoREncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let Some(parray) = array.maybe_primitive() else {
            return None;
        };

        // Only supports integers
        if !parray.ptype().is_int() {
            return None;
        }

        // Nothing for us to do if the min is already zero
        if parray.stats().get_or_compute_cast::<i64>(&Stat::Min)? != 0 {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: &CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let parray = array.as_primitive();

        let child = match_each_integer_ptype!(parray.ptype(), |$T| {
            let min = parray.stats().get_or_compute_as::<$T>(&Stat::Min).unwrap_or(<$T>::default());

            // TODO(ngates): check for overflow
            let values = parray.buffer().typed_data::<$T>().iter().map(|v| v - min)
                // TODO(ngates): cast to unsigned
                // .map(|v| v as parray.ptype().to_unsigned()::T)
                .collect_vec();

            PrimitiveArray::from(values)
        });

        // TODO(ngates): remove FoR as a potential encoding from the ctx
        // NOTE(ngates): we don't invoke next_level here since we know bit-packing is always
        //  worth trying.
        let compressed_child = ctx.excluding(&FoREncoding::ID).compress(
            child.as_ref(),
            like.map(|l| l.as_any().downcast_ref::<FoRArray>().unwrap().child()),
        )?;
        let reference = parray.stats().get(&Stat::Min).unwrap();
        Ok(FoRArray::try_new(compressed_child, reference)?.boxed())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use vortex::array::primitive::PrimitiveEncoding;
    use vortex::array::Encoding;

    use crate::BitPackedEncoding;

    use super::*;

    #[test]
    fn test_compress() {
        let cfg = CompressConfig::new(
            // We need some BitPacking else we will need choose FoR.
            HashSet::from([
                PrimitiveEncoding.id(),
                FoREncoding.id(),
                BitPackedEncoding.id(),
            ]),
            HashSet::default(),
        );
        let ctx = CompressCtx::new(&cfg);

        // Create a range offset by a million
        let array = PrimitiveArray::from((0u32..10_000).map(|v| v + 1_000_000).collect_vec());

        let compressed = ctx.compress(&array, None).unwrap();
        assert_eq!(compressed.encoding().id(), FoREncoding.id());
        let fa = compressed.as_any().downcast_ref::<FoRArray>().unwrap();
        assert_eq!(fa.reference().try_into(), Ok(1_000_000u32));
    }
}
