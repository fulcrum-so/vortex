use itertools::Itertools;
use num_traits::{PrimInt, WrappingAdd, WrappingSub};
use vortex::array::constant::ConstantArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::stats::{ArrayStatistics, Stat};
use vortex::{Array, ArrayDType, ArrayTrait, IntoArray, OwnedArray};
use vortex_dtype::{match_each_integer_ptype, NativePType, PType};
use vortex_error::{vortex_err, VortexResult};

use crate::{FoRArray, FoREncoding};

impl EncodingCompression for FoREncoding {
    fn cost(&self) -> u8 {
        0
    }

    fn can_compress(
        &self,
        array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let parray = PrimitiveArray::try_from(array).ok()?;

        // Only supports integers
        if !parray.ptype().is_int() {
            return None;
        }

        // Nothing for us to do if the min is already zero and tz == 0
        let shift = trailing_zeros(array);
        let min = parray.statistics().compute_as_cast::<i64>(Stat::Min).ok()?;
        if min == 0 && shift == 0 {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &Array,
        like: Option<&Array>,
        ctx: CompressCtx,
    ) -> VortexResult<OwnedArray> {
        let parray = PrimitiveArray::try_from(array)?;
        let shift = trailing_zeros(array);
        let min = parray
            .statistics()
            .compute(Stat::Min)
            .ok_or_else(|| vortex_err!("Min stat not found"))?;

        let child = match_each_integer_ptype!(parray.ptype(), |$T| {
            if shift == <$T>::PTYPE.bit_width() as u8 {
                ConstantArray::new($T::default(), parray.len()).into_array()
            } else {
                compress_primitive::<$T>(parray, shift, $T::try_from(min.clone())?).into_array()
            }
        });
        let for_like = like.map(|like_arr| FoRArray::try_from(like_arr).unwrap());

        let compressed_child = ctx
            .named("for")
            .excluding(&FoREncoding)
            .compress(&child, for_like.as_ref().map(|l| l.encoded()).as_ref())?;
        FoRArray::try_new(compressed_child, min, shift).map(|a| a.into_array())
    }
}

fn compress_primitive<T: NativePType + WrappingSub + PrimInt>(
    parray: PrimitiveArray,
    shift: u8,
    min: T,
) -> PrimitiveArray {
    assert!(shift < T::PTYPE.bit_width() as u8);
    let values = if shift > 0 {
        let shifted_min = min >> shift as usize;
        parray
            .typed_data::<T>()
            .iter()
            .map(|&v| v >> shift as usize)
            .map(|v| v.wrapping_sub(&shifted_min))
            .collect_vec()
    } else {
        parray
            .typed_data::<T>()
            .iter()
            .map(|&v| v.wrapping_sub(&min))
            .collect_vec()
    };

    PrimitiveArray::from_vec(values, parray.validity())
}

pub fn decompress(array: FoRArray) -> VortexResult<PrimitiveArray> {
    let shift = array.shift();
    let ptype: PType = array.dtype().try_into()?;
    let encoded = array.encoded().flatten_primitive()?;
    Ok(match_each_integer_ptype!(ptype, |$T| {
        let reference: $T = array.reference().try_into()?;
        PrimitiveArray::from_vec(
            decompress_primitive(encoded.typed_data::<$T>(), reference, shift),
            encoded.validity(),
        )
    }))
}

fn decompress_primitive<T: NativePType + WrappingAdd + PrimInt>(
    values: &[T],
    reference: T,
    shift: u8,
) -> Vec<T> {
    if shift > 0 {
        let shifted_reference = reference << shift as usize;
        values
            .iter()
            .map(|&v| v << shift as usize)
            .map(|v| v.wrapping_add(&shifted_reference))
            .collect_vec()
    } else {
        values
            .iter()
            .map(|&v| v.wrapping_add(&reference))
            .collect_vec()
    }
}

fn trailing_zeros(array: &Array) -> u8 {
    let tz_freq = array
        .statistics()
        .compute_trailing_zero_freq()
        .unwrap_or(vec![0]);
    tz_freq
        .iter()
        .enumerate()
        .find_or_first(|(_, &v)| v > 0)
        .map(|(i, _)| i)
        .unwrap_or(0) as u8
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex::compute::scalar_at::ScalarAtFn;
    use vortex::encoding::{ArrayEncoding, EncodingRef};

    use super::*;
    use crate::BitPackedEncoding;

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

        let compressed = ctx.compress(array.array(), None).unwrap();
        assert_eq!(compressed.encoding().id(), FoREncoding.id());
        assert_eq!(
            u32::try_from(FoRArray::try_from(compressed).unwrap().reference()).unwrap(),
            1_000_000u32
        );
    }

    #[test]
    fn test_decompress() {
        let ctx = compress_ctx();

        // Create a range offset by a million
        let array = PrimitiveArray::from((0u32..10_000).map(|v| v + 1_000_000).collect_vec());
        let compressed = ctx.compress(array.array(), None).unwrap();
        assert_eq!(compressed.encoding().id(), FoREncoding.id());

        let decompressed = compressed.flatten_primitive().unwrap();
        assert_eq!(decompressed.typed_data::<u32>(), array.typed_data::<u32>());
    }

    #[test]
    fn test_overflow() {
        let ctx = compress_ctx();

        // Create a range offset by a million
        let array = PrimitiveArray::from((i8::MIN..i8::MAX).collect_vec());
        let compressed = FoREncoding {}.compress(array.array(), None, ctx).unwrap();
        let compressed = FoRArray::try_from(compressed).unwrap();
        assert_eq!(i8::MIN, compressed.reference().try_into().unwrap());

        let encoded = compressed.encoded().flatten_primitive().unwrap();
        let bitcast: &[u8] = unsafe { std::mem::transmute(encoded.typed_data::<i8>()) };
        let unsigned: Vec<u8> = (0..u8::MAX).collect_vec();
        assert_eq!(bitcast, unsigned.as_slice());

        let decompressed = compressed.array().clone().flatten_primitive().unwrap();
        assert_eq!(decompressed.typed_data::<i8>(), array.typed_data::<i8>());
        array
            .typed_data::<i8>()
            .iter()
            .enumerate()
            .for_each(|(i, v)| {
                assert_eq!(*v, compressed.scalar_at(i).unwrap().try_into().unwrap());
            });
    }
}
