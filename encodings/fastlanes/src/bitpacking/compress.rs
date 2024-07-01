use std::mem::size_of;

use fastlanes::BitPacking;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::{Sparse, SparseArray};
use vortex::stats::ArrayStatistics;
use vortex::validity::Validity;
use vortex::IntoArrayVariant;
use vortex::{Array, ArrayDType, ArrayDef, ArrayTrait, IntoArray};
use vortex_dtype::{
    match_each_integer_ptype, match_each_unsigned_integer_ptype, NativePType, PType,
};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::BitPackedArray;

pub fn bitpack_encode(array: PrimitiveArray, bit_width: usize) -> VortexResult<BitPackedArray> {
    let bit_width_freq = array
        .statistics()
        .compute_bit_width_freq()
        .ok_or_else(|| vortex_err!(ComputeError: "missing bit width frequency"))?;
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

    BitPackedArray::try_new(packed, array.validity(), patches, bit_width, array.len())
}

pub fn bitpack(parray: &PrimitiveArray, bit_width: usize) -> VortexResult<Array> {
    // We know the min is > 0, so it's safe to re-interpret signed integers as unsigned.
    let parray = parray.reinterpret_cast(parray.ptype().to_unsigned());
    let packed = match_each_unsigned_integer_ptype!(parray.ptype(), |$P| {
        PrimitiveArray::from(bitpack_primitive(parray.maybe_null_slice::<$P>(), bit_width))
    });
    Ok(packed.into_array())
}

pub fn bitpack_primitive<T: NativePType + BitPacking>(array: &[T], bit_width: usize) -> Vec<T> {
    if bit_width == 0 {
        return Vec::new();
    }

    // How many fastlanes vectors we will process.
    let num_chunks = (array.len() + 1023) / 1024;
    let num_full_chunks = array.len() / 1024;
    let packed_len = 128 * bit_width / size_of::<T>();

    // Allocate a result byte array.
    let mut output = Vec::with_capacity(num_chunks * packed_len);

    // Loop over all but the last chunk.
    (0..num_full_chunks).for_each(|i| {
        let start_elem = i * 1024;

        output.reserve(packed_len);
        let output_len = output.len();
        unsafe {
            output.set_len(output_len + packed_len);
            BitPacking::unchecked_pack(
                bit_width,
                &array[start_elem..][..1024],
                &mut output[output_len..][..packed_len],
            );
        };
    });

    // Pad the last chunk with zeros to a full 1024 elements.
    if num_chunks != num_full_chunks {
        let last_chunk_size = array.len() % 1024;
        let mut last_chunk: [T; 1024] = [T::zero(); 1024];
        last_chunk[..last_chunk_size].copy_from_slice(&array[array.len() - last_chunk_size..]);

        output.reserve(packed_len);
        let output_len = output.len();
        unsafe {
            output.set_len(output_len + packed_len);
            BitPacking::unchecked_pack(
                bit_width,
                &last_chunk,
                &mut output[output_len..][..packed_len],
            );
        };
    }

    output
}

pub fn bitpack_patches(
    parray: &PrimitiveArray,
    bit_width: usize,
    num_exceptions_hint: usize,
) -> Array {
    match_each_integer_ptype!(parray.ptype(), |$T| {
        let mut indices: Vec<u64> = Vec::with_capacity(num_exceptions_hint);
        let mut values: Vec<$T> = Vec::with_capacity(num_exceptions_hint);
        for (i, v) in parray.maybe_null_slice::<$T>().iter().enumerate() {
            if (v.leading_zeros() as usize) < parray.ptype().bit_width() - bit_width {
                indices.push(i as u64);
                values.push(*v);
            }
        }
        SparseArray::try_new(
            indices.into_array(),
            PrimitiveArray::from_vec(values, Validity::AllValid).into_array(),
            parray.len(),
            Scalar::null(parray.dtype().as_nullable()),
        ).unwrap().into_array()
    })
}

pub fn unpack(array: BitPackedArray) -> VortexResult<PrimitiveArray> {
    let bit_width = array.bit_width();
    let length = array.len();
    let offset = array.offset();
    let packed = array.packed().into_primitive()?;
    let ptype = packed.ptype();

    let mut unpacked = match_each_unsigned_integer_ptype!(ptype, |$P| {
        PrimitiveArray::from_vec(
            unpack_primitive::<$P>(packed.maybe_null_slice::<$P>(), bit_width, offset, length),
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

fn patch_unpacked(array: PrimitiveArray, patches: &Array) -> VortexResult<PrimitiveArray> {
    match patches.encoding().id() {
        Sparse::ID => {
            match_each_integer_ptype!(array.ptype(), |$T| {
                let typed_patches = SparseArray::try_from(patches).unwrap();
                array.patch(
                    &typed_patches.resolved_indices(),
                    typed_patches.values().into_primitive()?.maybe_null_slice::<$T>())
            })
        }
        _ => panic!("can't patch bitpacked array with {}", patches),
    }
}

pub fn unpack_primitive<T: NativePType + BitPacking>(
    packed: &[T],
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
    let elems_per_chunk = 128 * bit_width / size_of::<T>();
    assert_eq!(
        packed.len(),
        num_chunks * elems_per_chunk,
        "Invalid packed length: got {}, expected {}",
        packed.len(),
        num_chunks * elems_per_chunk
    );

    // Allocate a result vector.
    let mut output = Vec::with_capacity(num_chunks * 1024 - offset);

    // Handle first chunk if offset is non 0. We have to decode the chunk and skip first offset elements
    let first_full_chunk = if offset != 0 {
        let chunk: &[T] = &packed[0..elems_per_chunk];
        let mut decoded = [T::zero(); 1024];
        unsafe { BitPacking::unchecked_unpack(bit_width, chunk, &mut decoded) };
        output.extend_from_slice(&decoded[offset..]);
        1
    } else {
        0
    };

    // Loop over all the chunks.
    (first_full_chunk..num_chunks).for_each(|i| {
        let chunk: &[T] = &packed[i * elems_per_chunk..][0..elems_per_chunk];
        unsafe {
            let output_len = output.len();
            output.set_len(output_len + 1024);
            BitPacking::unchecked_unpack(bit_width, chunk, &mut output[output_len..][0..1024]);
        }
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

pub fn unpack_single(array: &BitPackedArray, index: usize) -> VortexResult<Scalar> {
    let bit_width = array.bit_width();
    let packed = array.packed().into_primitive()?;
    let index_in_encoded = index + array.offset();
    let scalar: Scalar = match_each_unsigned_integer_ptype!(packed.ptype(), |$P| unsafe {
        unpack_single_primitive::<$P>(packed.maybe_null_slice::<$P>(), bit_width, index_in_encoded).map(|v| v.into())
    })?;
    // Cast to fix signedness and nullability
    scalar.cast(array.dtype())
}

/// # Safety
///
/// The caller must ensure the following invariants hold:
/// * `packed.len() == (length + 1023) / 1024 * 128 * bit_width`
/// * `index_to_decode < length`
///
/// Where `length` is the length of the array/slice backed by `packed`
/// (but is not provided to this function).
pub unsafe fn unpack_single_primitive<T: NativePType + BitPacking>(
    packed: &[T],
    bit_width: usize,
    index_to_decode: usize,
) -> VortexResult<T> {
    let chunk_index = index_to_decode / 1024;
    let index_in_chunk = index_to_decode % 1024;
    let elems_per_chunk: usize = 128 * bit_width / size_of::<T>();

    let packed_chunk = &packed[chunk_index * elems_per_chunk..][0..elems_per_chunk];
    Ok(unsafe { BitPacking::unchecked_unpack_single(bit_width, packed_chunk, index_in_chunk) })
}

pub fn find_best_bit_width(array: &PrimitiveArray) -> Option<usize> {
    let bit_width_freq = array.statistics().compute_bit_width_freq()?;

    Some(best_bit_width(
        &bit_width_freq,
        bytes_per_exception(array.ptype()),
    ))
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

pub fn count_exceptions(bit_width: usize, bit_width_freq: &[usize]) -> usize {
    if bit_width_freq.len() <= bit_width {
        return 0;
    }
    bit_width_freq[bit_width + 1..].iter().sum()
}

#[cfg(test)]
mod test {
    use vortex::IntoArrayVariant;
    use vortex::ToArray;
    use vortex_scalar::PrimitiveScalar;

    use super::*;

    #[test]
    fn test_best_bit_width() {
        // 10 1-bit values, 20 2-bit, etc.
        let freq = vec![0, 10, 20, 15, 1, 0, 0, 0];
        // 3-bits => (46 * 3) + (8 * 1 * 5) => 178 bits => 23 bytes and zero exceptions
        assert_eq!(best_bit_width(&freq, bytes_per_exception(PType::U8)), 3);
    }

    #[test]
    fn test_compression_roundtrip() {
        compression_roundtrip(125);
        compression_roundtrip(1024);
        compression_roundtrip(10_000);
        compression_roundtrip(10_240);
    }

    fn compression_roundtrip(n: usize) {
        let values = PrimitiveArray::from(Vec::from_iter((0..n).map(|i| (i % 2047) as u16)));
        let compressed = BitPackedArray::encode(values.array(), 11).unwrap();
        let decompressed = compressed.to_array().into_primitive().unwrap();
        assert_eq!(
            decompressed.maybe_null_slice::<u16>(),
            values.maybe_null_slice::<u16>()
        );

        values
            .maybe_null_slice::<u16>()
            .iter()
            .enumerate()
            .for_each(|(i, v)| {
                let scalar = unpack_single(&compressed, i).unwrap();
                let scalar = PrimitiveScalar::try_from(&scalar)
                    .unwrap()
                    .typed_value::<u16>()
                    .unwrap();
                assert_eq!(scalar, *v);
            });
    }
}
