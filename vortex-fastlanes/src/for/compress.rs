use itertools::Itertools;
use num_traits::PrimInt;

use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::flatten::flatten_primitive;
use vortex::error::VortexResult;
use vortex::match_each_integer_ptype;
use vortex::ptype::{NativePType, PType};
use vortex::scalar::ListScalarVec;
use vortex::stats::Stat;

use crate::{FoRArray, FoREncoding};

impl EncodingCompression for FoREncoding {
    fn cost(&self) -> u8 {
        0
    }

    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let parray = array.maybe_primitive()?;

        // Only supports integers
        if !parray.ptype().is_int() {
            return None;
        }

        // Nothing for us to do if the min is already zero and tz == 0
        let shift = trailing_zeros(parray);
        let min = parray.stats().get_or_compute_cast::<i64>(&Stat::Min)?;
        if min == 0 && shift == 0 {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let parray = array.as_primitive();
        let shift = trailing_zeros(parray);
        let child = match_each_integer_ptype!(parray.ptype(), |$T| {
            compress_primitive::<$T>(parray, shift)
        });

        // TODO(ngates): remove FoR as a potential encoding from the ctx
        // NOTE(ngates): we don't invoke next_level here since we know bit-packing is always
        //  worth trying.
        let compressed_child = ctx.named("for").excluding(&FoREncoding).compress(
            &child,
            like.map(|l| l.as_any().downcast_ref::<FoRArray>().unwrap().encoded()),
        )?;
        let reference = parray.stats().get(&Stat::Min).unwrap();
        Ok(FoRArray::try_new(compressed_child, reference, shift)?.into_array())
    }
}

fn compress_primitive<T: NativePType + PrimInt>(
    parray: &PrimitiveArray,
    shift: u8,
) -> PrimitiveArray {
    let min = parray
        .stats()
        .get_or_compute_as::<T>(&Stat::Min)
        .unwrap_or_default();

    let values = if shift > 0 {
        let shifted_min = min >> shift as usize;
        parray
            .typed_data::<T>()
            .iter()
            .map(|&v| v >> shift as usize)
            .map(|v| v - shifted_min)
            .collect_vec()
    } else {
        parray
            .typed_data::<T>()
            .iter()
            .map(|&v| v - min)
            .collect_vec()
    };

    PrimitiveArray::from(values)
}

pub fn decompress(array: &FoRArray) -> VortexResult<PrimitiveArray> {
    let shift = array.shift();
    let ptype: PType = array.dtype().try_into()?;
    let encoded = flatten_primitive(array.encoded())?;
    Ok(match_each_integer_ptype!(ptype, |$T| {
        let reference: $T = array.reference().try_into()?;
        PrimitiveArray::from_nullable(
            decompress_primitive(encoded.typed_data::<$T>(), reference, shift),
            encoded.validity().cloned(),
        )
    }))
}

fn decompress_primitive<T: NativePType + PrimInt>(values: &[T], reference: T, shift: u8) -> Vec<T> {
    if shift > 0 {
        let shifted_reference = reference << shift as usize;
        values
            .iter()
            .map(|&v| v << shift as usize)
            .map(|v| v + shifted_reference)
            .collect_vec()
    } else {
        values.iter().map(|&v| v + reference).collect_vec()
    }
}

fn trailing_zeros(array: &dyn Array) -> u8 {
    let tz_freq = array
        .stats()
        .get_or_compute_as::<ListScalarVec<usize>>(&Stat::TrailingZeroFreq)
        .map(|v| v.0)
        .unwrap_or(vec![0]);
    tz_freq
        .iter()
        .enumerate()
        .find_or_first(|(_, &v)| v > 0)
        .map(|(i, _freq)| i)
        .unwrap_or(0) as u8
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex::array::{Encoding, EncodingRef};

    use crate::BitPackedEncoding;

    use super::*;

    fn compress_ctx() -> CompressCtx {
        let cfg = CompressConfig::new()
            // We need some BitPacking else we will need choose FoR.
            .with_enabled([&FoREncoding as EncodingRef, &BitPackedEncoding]);
        CompressCtx::new(Arc::new(cfg))
    }

    #[test]
    fn test_compress() {
        let ctx = compress_ctx();

        // Create a range offset by a million
        let array = PrimitiveArray::from((0u32..10_000).map(|v| v + 1_000_000).collect_vec());

        let compressed = ctx.compress(&array, None).unwrap();
        assert_eq!(compressed.encoding().id(), FoREncoding.id());
        let fa = compressed.as_any().downcast_ref::<FoRArray>().unwrap();
        assert_eq!(fa.reference().try_into(), Ok(1_000_000u32));
    }

    #[test]
    fn test_decompress() {
        let ctx = compress_ctx();

        // Create a range offset by a million
        let array = PrimitiveArray::from((0u32..10_000).map(|v| v + 1_000_000).collect_vec());
        let compressed = ctx.compress(&array, None).unwrap();
        assert_eq!(compressed.encoding().id(), FoREncoding.id());

        let decompressed = flatten_primitive(compressed.as_ref()).unwrap();
        assert_eq!(decompressed.typed_data::<u32>(), array.typed_data::<u32>());
    }
}
