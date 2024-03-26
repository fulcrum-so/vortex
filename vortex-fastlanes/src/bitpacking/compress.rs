use arrayref::array_ref;

use crate::{BitPackedArray, BitPackedEncoding};
use fastlanez_sys::TryBitPack;
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::array::IntoArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::cast::cast;
use vortex::compute::flatten::flatten_primitive;
use vortex::compute::patch::patch;
use vortex::match_each_integer_ptype;
use vortex::ptype::PType::{I16, I32, I64, I8, U16, U32, U64, U8};
use vortex::ptype::{NativePType, PType};
use vortex::scalar::ListScalarVec;
use vortex::stats::Stat;
use vortex::validity::ArrayValidity;
use vortex_error::VortexResult;

impl EncodingCompression for BitPackedEncoding {
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

        // Only supports ints
        if !parray.ptype().is_int() {
            return None;
        }

        let bytes_per_exception = bytes_per_exception(parray.ptype());
        let bit_width_freq = parray
            .stats()
            .get_or_compute_as::<ListScalarVec<usize>>(&Stat::BitWidthFreq)?
            .0;
        let bit_width = best_bit_width(&bit_width_freq, bytes_per_exception);

        // Check that the bit width is less than the type's bit width
        if bit_width == parray.ptype().bit_width() {
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
        let bit_width_freq = parray
            .stats()
            .get_or_compute_as::<ListScalarVec<usize>>(&Stat::BitWidthFreq)
            .unwrap()
            .0;

        let like_bp = like.map(|l| l.as_any().downcast_ref::<BitPackedArray>().unwrap());
        let bit_width = best_bit_width(&bit_width_freq, bytes_per_exception(parray.ptype()));
        let num_exceptions = count_exceptions(bit_width, &bit_width_freq);

        if bit_width == parray.ptype().bit_width() {
            // Nothing we can do
            return Ok(parray.clone().into_array());
        }

        let packed = bitpack(parray, bit_width);

        let validity = ctx.compress_validity(parray.validity())?;

        let patches = if num_exceptions > 0 {
            Some(ctx.auxiliary("patches").compress(
                &bitpack_patches(parray, bit_width, num_exceptions),
                like_bp.and_then(|bp| bp.patches()),
            )?)
        } else {
            None
        };

        Ok(BitPackedArray::try_new(
            packed,
            validity,
            patches,
            bit_width,
            parray.dtype().clone(),
            parray.len(),
        )
        .unwrap()
        .into_array())
    }
}

fn bitpack(parray: &PrimitiveArray, bit_width: usize) -> ArrayRef {
    // We know the min is > 0, so it's safe to re-interpret signed integers as unsigned.
    // TODO(ngates): we should implement this using a vortex cast to centralize this hack.
    use PType::*;
    let bytes = match parray.ptype() {
        I8 | U8 => bitpack_primitive(parray.buffer().typed_data::<u8>(), bit_width),
        I16 | U16 => bitpack_primitive(parray.buffer().typed_data::<u16>(), bit_width),
        I32 | U32 => bitpack_primitive(parray.buffer().typed_data::<u32>(), bit_width),
        I64 | U64 => bitpack_primitive(parray.buffer().typed_data::<u64>(), bit_width),
        _ => panic!("Unsupported ptype {:?}", parray.ptype()),
    };
    PrimitiveArray::from(bytes).into_array()
}

fn bitpack_primitive<T: NativePType + TryBitPack>(array: &[T], bit_width: usize) -> Vec<u8> {
    if bit_width == 0 {
        return Vec::new();
    }

    // How many fastlanes vectors we will process.
    let num_chunks = (array.len() + 1023) / 1024;

    // Allocate a result byte array.
    let mut output = Vec::with_capacity(num_chunks * bit_width * 128);

    // Loop over all but the last chunk.
    (0..num_chunks - 1).for_each(|i| {
        let start_elem = i * 1024;
        let chunk: &[T; 1024] = array_ref![array, start_elem, 1024];
        TryBitPack::try_bitpack_into(chunk, bit_width, &mut output).unwrap();
    });

    // Pad the last chunk with zeros to a full 1024 elements.
    let last_chunk_size = array.len() % 1024;
    if last_chunk_size > 0 {
        let mut last_chunk: [T; 1024] = [T::default(); 1024];
        last_chunk[..last_chunk_size].copy_from_slice(&array[array.len() - last_chunk_size..]);
        TryBitPack::try_bitpack_into(&last_chunk, bit_width, &mut output).unwrap();
    }

    output
}

fn bitpack_patches(
    parray: &PrimitiveArray,
    bit_width: usize,
    num_exceptions_hint: usize,
) -> ArrayRef {
    match_each_integer_ptype!(parray.ptype(), |$T| {
        let mut indices: Vec<u64> = Vec::with_capacity(num_exceptions_hint);
        let mut values: Vec<$T> = Vec::with_capacity(num_exceptions_hint);
        for (i, v) in parray.buffer().typed_data::<$T>().iter().enumerate() {
            if (v.leading_zeros() as usize) < parray.ptype().bit_width() - bit_width {
                indices.push(i as u64);
                values.push(*v);
            }
        }
        let len = indices.len();
        SparseArray::new(indices.into_array(), values.into_array(), len).into_array()
    })
}

pub fn bitunpack(array: &BitPackedArray) -> VortexResult<PrimitiveArray> {
    let bit_width = array.bit_width();
    let length = array.len();
    let encoded = flatten_primitive(cast(array.encoded(), PType::U8.into())?.as_ref())?;
    let ptype: PType = array.dtype().try_into()?;

    let mut unpacked = match ptype {
        I8 | U8 => PrimitiveArray::from_nullable(
            bitunpack_primitive::<u8>(encoded.typed_data::<u8>(), bit_width, length),
            array.validity(),
        ),
        I16 | U16 => PrimitiveArray::from_nullable(
            bitunpack_primitive::<u16>(encoded.typed_data::<u8>(), bit_width, length),
            array.validity(),
        ),
        I32 | U32 => PrimitiveArray::from_nullable(
            bitunpack_primitive::<u32>(encoded.typed_data::<u8>(), bit_width, length),
            array.validity(),
        ),
        I64 | U64 => PrimitiveArray::from_nullable(
            bitunpack_primitive::<u64>(encoded.typed_data::<u8>(), bit_width, length),
            array.validity(),
        ),
        _ => panic!("Unsupported ptype {:?}", ptype),
    }
    .into_array();

    // Cast to signed if necessary
    // TODO(ngates): do this more efficiently since we know it's a safe cast. unchecked_cast maybe?
    if ptype.is_signed_int() {
        unpacked = cast(&unpacked, &ptype.into())?
    }

    if let Some(patches) = array.patches() {
        unpacked = patch(unpacked.as_ref(), patches)?;
    }

    flatten_primitive(&unpacked)
}

fn bitunpack_primitive<T: NativePType + TryBitPack>(
    packed: &[u8],
    bit_width: usize,
    length: usize,
) -> Vec<T> {
    if bit_width == 0 {
        return vec![T::default(); length];
    }

    // How many fastlanes vectors we will process.
    let num_chunks = (length + 1023) / 1024;

    // Allocate a result vector.
    let mut output = Vec::with_capacity(length);

    // Loop over all but the last chunk.
    let bytes_per_chunk = 128 * bit_width;
    (0..num_chunks - 1).for_each(|i| {
        let chunk: &[u8] = &packed[i * bytes_per_chunk..][0..bytes_per_chunk];
        TryBitPack::try_bitunpack_into(chunk, bit_width, &mut output).unwrap();
    });

    // Handle the final chunk which may contain padding.
    let last_chunk_size = length % 1024;
    if last_chunk_size > 0 {
        let mut last_output = Vec::with_capacity(1024);
        TryBitPack::try_bitunpack_into(
            &packed[(num_chunks - 1) * bytes_per_chunk..],
            bit_width,
            &mut last_output,
        )
        .unwrap();
        output.extend_from_slice(&last_output[..last_chunk_size]);
    }

    output
}

/// Assuming exceptions cost 1 value + 1 u32 index, figure out the best bit-width to use.
/// We could try to be clever, but we can never really predict how the exceptions will compress.
fn best_bit_width(bit_width_freq: &[usize], bytes_per_exception: usize) -> usize {
    let len: usize = bit_width_freq.iter().sum();

    if bit_width_freq.len() > u8::MAX as usize {
        panic!("Too many bit widths");
    }

    let mut num_packed = 0;
    let mut best_cost = len * bytes_per_exception;
    let mut best_width = 0;
    for (bit_width, freq) in bit_width_freq.iter().enumerate() {
        num_packed += *freq;
        let packed_cost = ((bit_width * len) + 7) / 8;
        let exceptions_cost = (len - num_packed) * bytes_per_exception;
        let cost = exceptions_cost + packed_cost;
        if cost < best_cost {
            best_cost = cost;
            best_width = bit_width;
        }
    }

    best_width
}

fn bytes_per_exception(ptype: PType) -> usize {
    ptype.byte_width() + 4
}

fn count_exceptions(bit_width: usize, bit_width_freq: &[usize]) -> usize {
    bit_width_freq[bit_width + 1..].iter().sum()
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex::array::{Encoding, EncodingRef};

    use super::*;

    #[test]
    fn test_best_bit_width() {
        // 10 1-bit values, 20 2-bit, etc.
        let freq = vec![0, 10, 20, 15, 1, 0, 0, 0];
        // 3-bits => (46 * 3) + (8 * 1 * 5) => 178 bits => 23 bytes and zero exceptions
        assert_eq!(best_bit_width(&freq, bytes_per_exception(PType::U8)), 3);
    }

    #[test]
    fn test_compress() {
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let compressed = ctx
            .compress(
                &PrimitiveArray::from(Vec::from_iter((0..10_000).map(|i| (i % 63) as u8))),
                None,
            )
            .unwrap();
        assert_eq!(compressed.encoding().id(), BitPackedEncoding.id());
        let bp = compressed
            .as_any()
            .downcast_ref::<BitPackedArray>()
            .unwrap();
        assert_eq!(bp.bit_width(), 6);
    }

    #[test]
    fn test_decompress() {
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let values = PrimitiveArray::from(Vec::from_iter((0..10_000).map(|i| (i % 63) as u8)));
        let compressed = ctx.compress(&values, None).unwrap();
        assert_eq!(compressed.encoding().id(), BitPackedEncoding.id());

        let decompressed = flatten_primitive(compressed.as_ref()).unwrap();
        assert_eq!(decompressed.typed_data::<u8>(), values.typed_data::<u8>());
    }
}
