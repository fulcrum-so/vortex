use arrayref::array_ref;
use fastlanez::TryBitPack;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::{Sparse, SparseArray};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::cast::cast;
use vortex::stats::ArrayStatistics;
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, ArrayDef, ArrayTrait, IntoArray, OwnedArray, ToStatic};
use vortex_dtype::PType::U8;
use vortex_dtype::{match_each_integer_ptype, NativePType, PType};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::{match_integers_by_width, BitPackedArray, BitPackedEncoding};

impl EncodingCompression for BitPackedEncoding {
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

        // Only supports ints
        if !parray.ptype().is_int() {
            return None;
        }

        let bytes_per_exception = bytes_per_exception(parray.ptype());
        let bit_width_freq = parray.statistics().compute_bit_width_freq().ok()?;
        let bit_width = best_bit_width(&bit_width_freq, bytes_per_exception);

        // Check that the bit width is less than the type's bit width
        if bit_width == parray.ptype().bit_width() {
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
        let parray = array.as_primitive();
        let bit_width_freq = parray.statistics().compute_bit_width_freq()?;

        let like_bp = like.map(|l| BitPackedArray::try_from(l).unwrap());
        let bit_width = best_bit_width(&bit_width_freq, bytes_per_exception(parray.ptype()));
        let num_exceptions = count_exceptions(bit_width, &bit_width_freq);

        if bit_width == parray.ptype().bit_width() {
            // Nothing we can do
            return Ok(array.to_static());
        }

        let validity = ctx.compress_validity(parray.validity())?;
        let packed = bitpack(&parray, bit_width)?;
        let patches = if num_exceptions > 0 {
            Some(ctx.auxiliary("patches").compress(
                &bitpack_patches(&parray, bit_width, num_exceptions),
                like_bp.as_ref().and_then(|bp| bp.patches()).as_ref(),
            )?)
        } else {
            None
        };

        BitPackedArray::try_new(
            packed,
            validity,
            patches,
            bit_width,
            parray.dtype().clone(),
            parray.len(),
        )
        .map(|a| a.into_array())
    }
}

pub(crate) fn bitpack_encode(
    array: PrimitiveArray<'_>,
    bit_width: usize,
) -> VortexResult<BitPackedArray> {
    let bit_width_freq = array.statistics().compute_bit_width_freq()?;
    let num_exceptions = count_exceptions(bit_width, &bit_width_freq);

    if bit_width >= array.ptype().bit_width() {
        // Nothing we can do
        vortex_bail!(
            "Cannot pack -- specified bit width is greater than or equal to the type's bit width"
        )
    }

    let packed = bitpack(&array, bit_width)?;
    let patches = if num_exceptions > 0 {
        Some(bitpack_patches(&array, bit_width, num_exceptions))
    } else {
        None
    };

    BitPackedArray::try_new(
        packed,
        array.validity(),
        patches,
        bit_width,
        array.dtype().clone(),
        array.len(),
    )
}

pub(crate) fn bitpack(parray: &PrimitiveArray, bit_width: usize) -> VortexResult<OwnedArray> {
    // We know the min is > 0, so it's safe to re-interpret signed integers as unsigned.
    // TODO(ngates): we should implement this using a vortex cast to centralize this hack.
    let bytes = match_integers_by_width!(parray.ptype(), |$P| {
        bitpack_primitive(parray.buffer().typed_data::<$P>(), bit_width)
    });
    Ok(PrimitiveArray::from(bytes).into_array())
}

pub fn bitpack_primitive<T: NativePType + TryBitPack>(array: &[T], bit_width: usize) -> Vec<u8> {
    if bit_width == 0 {
        return Vec::new();
    }

    // How many fastlanes vectors we will process.
    let num_chunks = (array.len() + 1023) / 1024;
    let num_full_chunks = array.len() / 1024;

    // Allocate a result byte array.
    let mut output = Vec::with_capacity(num_chunks * bit_width * 128);

    // Loop over all but the last chunk.
    (0..num_full_chunks).for_each(|i| {
        let start_elem = i * 1024;
        let chunk: &[T; 1024] = array_ref![array, start_elem, 1024];
        TryBitPack::try_pack_into(chunk, bit_width, &mut output).unwrap();
    });

    // Pad the last chunk with zeros to a full 1024 elements.
    if num_chunks != num_full_chunks {
        let last_chunk_size = array.len() % 1024;
        let mut last_chunk: [T; 1024] = [T::zero(); 1024];
        last_chunk[..last_chunk_size].copy_from_slice(&array[array.len() - last_chunk_size..]);
        TryBitPack::try_pack_into(&last_chunk, bit_width, &mut output).unwrap();
    }

    output
}

fn bitpack_patches(
    parray: &PrimitiveArray,
    bit_width: usize,
    num_exceptions_hint: usize,
) -> OwnedArray {
    match_each_integer_ptype!(parray.ptype(), |$T| {
        let mut indices: Vec<u64> = Vec::with_capacity(num_exceptions_hint);
        let mut values: Vec<$T> = Vec::with_capacity(num_exceptions_hint);
        for (i, v) in parray.buffer().typed_data::<$T>().iter().enumerate() {
            if (v.leading_zeros() as usize) < parray.ptype().bit_width() - bit_width {
                indices.push(i as u64);
                values.push(*v);
            }
        }
        SparseArray::try_new(
            indices.into_array(),
            PrimitiveArray::from_vec(values, Validity::AllValid).into_array(),
            parray.len(),
            Scalar::null(&parray.dtype().as_nullable()),
        ).unwrap().into_array()
    })
}

pub fn unpack<'a>(array: BitPackedArray) -> VortexResult<PrimitiveArray<'a>> {
    let bit_width = array.bit_width();
    let length = array.len();
    let offset = array.offset();
    let encoded = cast(&array.packed(), U8.into())?.flatten_primitive()?;
    let ptype: PType = array.dtype().try_into()?;

    let mut unpacked = match_integers_by_width!(ptype, |$P| {
        PrimitiveArray::from_vec(
            unpack_primitive::<$P>(encoded.typed_data::<u8>(), bit_width, offset, length),
            array.validity(),
        )
    });

    // Cast to signed if necessary
    if ptype.is_signed_int() {
        unpacked = unpacked.reinterpret_cast(ptype);
    }

    if let Some(patches) = array.patches() {
        patch_unpacked(unpacked, &patches)
    } else {
        Ok(unpacked)
    }
}

fn patch_unpacked<'a>(
    array: PrimitiveArray<'a>,
    patches: &Array,
) -> VortexResult<PrimitiveArray<'a>> {
    match patches.encoding().id() {
        Sparse::ID => {
            match_each_integer_ptype!(array.ptype(), |$T| {
                let typed_patches = SparseArray::try_from(patches).unwrap();
                array.patch(
                    &typed_patches.resolved_indices(),
                    typed_patches.values().flatten_primitive()?.typed_data::<$T>())
            })
        }
        _ => panic!("can't patch bitpacked array with {}", patches),
    }
}

pub fn unpack_primitive<T: NativePType + TryBitPack>(
    packed: &[u8],
    bit_width: usize,
    offset: usize,
    length: usize,
) -> Vec<T> {
    if bit_width == 0 {
        return vec![T::zero(); length];
    }

    // How many fastlanes vectors we will process.
    // Packed array might not start at 0 when the array is sliced. Offset is guaranteed to be < 1024.
    let num_chunks = (offset + length + 1023) / 1024;
    let bytes_per_chunk = 128 * bit_width;
    assert_eq!(
        packed.len(),
        num_chunks * bytes_per_chunk,
        "Invalid packed length: got {}, expected {}",
        packed.len(),
        num_chunks * bytes_per_chunk
    );

    // Allocate a result vector.
    let mut output = Vec::with_capacity(num_chunks * 1024 - offset);
    // Handle first chunk if offset is non 0. We have to decode the chunk and skip first offset elements
    let first_full_chunk = if offset != 0 {
        let chunk: &[u8] = &packed[0..bytes_per_chunk];
        TryBitPack::try_unpack_into(chunk, bit_width, &mut output).unwrap();
        output.drain(0..offset);
        1
    } else {
        0
    };

    // Loop over all the chunks.
    (first_full_chunk..num_chunks).for_each(|i| {
        let chunk: &[u8] = &packed[i * bytes_per_chunk..][0..bytes_per_chunk];
        TryBitPack::try_unpack_into(chunk, bit_width, &mut output).unwrap();
    });

    // The final chunk may have had padding
    output.truncate(length);

    // For small vectors, the overhead of rounding up is more noticable.
    // Shrink to fit may or may not reallocate depending on the implementation.
    // But for very small vectors, the reallocation is cheap enough even if it does happen.
    if output.len() < 1024 {
        output.shrink_to_fit();
    }

    assert_eq!(
        output.len(),
        length,
        "Expected unpacked array to be of length {} but got {}",
        length,
        output.len()
    );
    output
}

pub(crate) fn unpack_single(array: &BitPackedArray, index: usize) -> VortexResult<Scalar> {
    let bit_width = array.bit_width();
    let encoded = cast(&array.packed(), U8.into())?.flatten_primitive()?;
    let ptype: PType = array.dtype().try_into()?;
    let index_in_encoded = index + array.offset();

    let scalar: Scalar = match_integers_by_width!(ptype, |$P| {
        unsafe {
            unpack_single_primitive::<$P>(encoded.typed_data::<u8>(), bit_width, index_in_encoded).map(|v| v.into())
        }
    })?;

    // Cast to fix signedness and nullability
    scalar.cast(array.dtype())
}

/// # Safety
///
/// The caller must ensure the following invariants hold:
/// * `packed.len() == (length + 1023) / 1024 * 128 * bit_width`
/// * `index_to_decode < length`
/// Where `length` is the length of the array/slice backed by `packed` (but is not provided to this function).
pub unsafe fn unpack_single_primitive<T: NativePType + TryBitPack>(
    packed: &[u8],
    bit_width: usize,
    index_to_decode: usize,
) -> VortexResult<T> {
    let bytes_per_chunk = 128 * bit_width;
    let chunk_index = index_to_decode / 1024;
    let chunk_bytes = &packed[chunk_index * bytes_per_chunk..][0..bytes_per_chunk];
    let index_in_chunk = index_to_decode % 1024;

    T::try_unpack_single(chunk_bytes, bit_width, index_in_chunk)
        .map_err(|_| vortex_err!("Unsupported bit width {}", bit_width))
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
    if (bit_width_freq.len()) <= bit_width {
        return 0;
    }
    bit_width_freq[bit_width + 1..].iter().sum()
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex::encoding::{ArrayEncoding, EncodingRef};
    use vortex::ToArray;

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
                PrimitiveArray::from(Vec::from_iter((0..10_000).map(|i| (i % 63) as u8))).array(),
                None,
            )
            .unwrap();
        assert_eq!(compressed.encoding().id(), BitPackedEncoding.id());
        assert_eq!(BitPackedArray::try_from(compressed).unwrap().bit_width(), 6);
    }

    #[test]
    fn test_compression_roundtrip() {
        compression_roundtrip(125);
        compression_roundtrip(1024);
        compression_roundtrip(10_000);
        compression_roundtrip(10_240);
    }

    fn compression_roundtrip(n: usize) {
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let values = PrimitiveArray::from(Vec::from_iter((0..n).map(|i| (i % 2047) as u16)));
        let compressed = ctx.compress(values.array(), None).unwrap();
        let compressed = BitPackedArray::try_from(compressed).unwrap();
        let decompressed = compressed.to_array().flatten_primitive().unwrap();
        assert_eq!(decompressed.typed_data::<u16>(), values.typed_data::<u16>());

        values
            .typed_data::<u16>()
            .iter()
            .enumerate()
            .for_each(|(i, v)| {
                let scalar_at: u16 =
                    if let Scalar::Primitive(pscalar) = unpack_single(&compressed, i).unwrap() {
                        pscalar.value().unwrap().try_into().unwrap()
                    } else {
                        panic!("expected u8 scalar")
                    };
                assert_eq!(scalar_at, *v);
            });
    }
}
