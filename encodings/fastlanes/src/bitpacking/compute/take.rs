use std::cmp::min;

use fastlanes::BitPacking;
use itertools::Itertools;
use vortex::array::constant::ConstantArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::compute::slice::slice;
use vortex::compute::take::{take, TakeFn};
use vortex::{Array, ArrayDType, ArrayTrait, IntoArray, IntoArrayVariant};
use vortex_dtype::{
    match_each_integer_ptype, match_each_unsigned_integer_ptype, NativePType, PType,
};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::{unpack_single_primitive, BitPackedArray};

impl TakeFn for BitPackedArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let ptype: PType = self.dtype().try_into()?;
        let validity = self.validity();
        let taken_validity = validity.take(indices)?;
        if self.bit_width() == 0 {
            return if let Some(patches) = self.patches() {
                take(&patches, indices)
            } else {
                Ok(
                    ConstantArray::new(Scalar::null(self.dtype().as_nullable()), indices.len())
                        .into_array(),
                )
            };
        }

        let indices = indices.clone().into_primitive()?;
        let taken = match_each_unsigned_integer_ptype!(ptype, |$T| {
            PrimitiveArray::from_vec(take_primitive::<$T>(self, &indices)?, taken_validity)
        });
        Ok(taken.reinterpret_cast(ptype).into_array())
    }
}

fn take_primitive<T: NativePType + BitPacking>(
    array: &BitPackedArray,
    indices: &PrimitiveArray,
) -> VortexResult<Vec<T>> {
    // Group indices into 1024-element chunks and relativise them to the beginning of each chunk
    let relative_indices: Vec<(usize, Vec<u16>)> = match_each_integer_ptype!(indices.ptype(), |$P| {
        indices
            .maybe_null_slice::<$P>()
            .iter()
            .chunk_by(|idx| (**idx / 1024) as usize)
            .into_iter()
            .map(|(k, g)| (k, g.map(|idx| (*idx % 1024) as u16).collect()))
            .collect()
    });

    let bit_width = array.bit_width();

    let packed = array.packed().into_primitive()?;
    let packed = packed.maybe_null_slice::<T>();

    let patches = array.patches().map(SparseArray::try_from).transpose()?;

    // if we have a small number of relatively large batches, we gain by slicing and then patching inside the loop
    // if we have a large number of relatively small batches, the overhead isn't worth it, and we're better off with a bulk patch
    // roughly, if we have an average of less than 64 elements per batch, we prefer bulk patching
    let prefer_bulk_patch = relative_indices.len() * 64 > indices.len();

    // assuming the buffer is already allocated (which will happen at most once)
    // then unpacking all 1024 elements takes ~8.8x as long as unpacking a single element
    // see https://github.com/fulcrum-so/vortex/pull/190#issue-2223752833
    // however, the gap should be smaller with larger registers (e.g., AVX-512) vs the 128 bit
    // ones on M2 Macbook Air.
    let unpack_chunk_threshold = 8;

    let mut output = Vec::with_capacity(indices.len());
    let mut unpacked = [T::zero(); 1024];
    for (chunk, offsets) in relative_indices {
        let chunk_size = 128 * bit_width / size_of::<T>();
        let packed_chunk = &packed[chunk * chunk_size..][..chunk_size];
        if offsets.len() > unpack_chunk_threshold {
            unsafe {
                BitPacking::unchecked_unpack(bit_width, packed_chunk, &mut unpacked);
            }
            for index in &offsets {
                output.push(unpacked[*index as usize]);
            }
        } else {
            for index in &offsets {
                output.push(unsafe {
                    unpack_single_primitive::<T>(packed_chunk, bit_width, *index as usize)?
                });
            }
        }

        if !prefer_bulk_patch {
            if let Some(ref patches) = patches {
                let patches_slice = slice(
                    patches.array(),
                    chunk * 1024,
                    min((chunk + 1) * 1024, patches.len()),
                )?;
                let patches_slice = SparseArray::try_from(patches_slice)?;
                let offsets = PrimitiveArray::from(offsets);
                do_patch_for_take_primitive(&patches_slice, &offsets, &mut output)?;
            }
        }
    }

    if prefer_bulk_patch {
        if let Some(ref patches) = patches {
            do_patch_for_take_primitive(patches, indices, &mut output)?;
        }
    }

    Ok(output)
}

fn do_patch_for_take_primitive<T: NativePType>(
    patches: &SparseArray,
    indices: &PrimitiveArray,
    output: &mut [T],
) -> VortexResult<()> {
    let taken_patches = take(patches.array(), indices.array())?;
    let taken_patches = SparseArray::try_from(taken_patches)?;

    let base_index = output.len() - indices.len();
    let output_patches = taken_patches
        .values()
        .into_primitive()?
        .reinterpret_cast(T::PTYPE);
    taken_patches
        .resolved_indices()
        .iter()
        .map(|idx| base_index + *idx)
        .zip_eq(output_patches.maybe_null_slice::<T>())
        .for_each(|(idx, val)| {
            output[idx] = *val;
        });

    Ok(())
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use rand::distributions::Uniform;
    use rand::{thread_rng, Rng};
    use vortex::array::primitive::{Primitive, PrimitiveArray};
    use vortex::array::sparse::SparseArray;
    use vortex::compute::take::take;
    use vortex::compute::unary::scalar_at::scalar_at;
    use vortex::{ArrayDef, IntoArray, IntoArrayVariant};

    use crate::BitPackedArray;

    #[test]
    fn take_indices() {
        let indices = PrimitiveArray::from(vec![0, 125, 2047, 2049, 2151, 2790]).into_array();

        // Create a u8 array modulo 63.
        let unpacked = PrimitiveArray::from((0..4096).map(|i| (i % 63) as u8).collect::<Vec<_>>());

        let bitpacked = BitPackedArray::encode(unpacked.array(), 6).unwrap();

        let result = take(bitpacked.array(), &indices).unwrap();
        assert_eq!(result.encoding().id(), Primitive::ID);

        let primitive_result = result.into_primitive().unwrap();
        let res_bytes = primitive_result.maybe_null_slice::<u8>();
        assert_eq!(res_bytes, &[0, 62, 31, 33, 9, 18]);
    }

    #[test]
    fn take_random_indices() {
        let num_patches: usize = 128;
        let values = (0..u16::MAX as u32 + num_patches as u32).collect::<Vec<_>>();
        let uncompressed = PrimitiveArray::from(values.clone());
        let packed = BitPackedArray::encode(uncompressed.array(), 16).unwrap();
        assert!(packed.patches().is_some());

        let patches = SparseArray::try_from(packed.patches().unwrap()).unwrap();
        assert_eq!(
            patches.resolved_indices(),
            ((values.len() + 1 - num_patches)..values.len()).collect_vec()
        );

        let rng = thread_rng();
        let range = Uniform::new(0, values.len());
        let random_indices: PrimitiveArray = rng
            .sample_iter(range)
            .take(10_000)
            .map(|i| i as u32)
            .collect_vec()
            .into();
        let taken = take(packed.array(), random_indices.array()).unwrap();

        // sanity check
        random_indices
            .maybe_null_slice::<u32>()
            .iter()
            .enumerate()
            .for_each(|(ti, i)| {
                assert_eq!(
                    u32::try_from(scalar_at(packed.array(), *i as usize).unwrap().as_ref())
                        .unwrap(),
                    values[*i as usize]
                );
                assert_eq!(
                    u32::try_from(scalar_at(&taken, ti).unwrap().as_ref()).unwrap(),
                    values[*i as usize]
                );
            });
    }

    #[test]
    fn test_scalar_at() {
        let values = (0u32..257).collect_vec();
        let uncompressed = PrimitiveArray::from(values.clone()).into_array();
        let packed = BitPackedArray::encode(&uncompressed, 8).unwrap();
        assert!(packed.patches().is_some());

        let patches = SparseArray::try_from(packed.patches().unwrap()).unwrap();
        assert_eq!(patches.resolved_indices(), vec![256]);

        values.iter().enumerate().for_each(|(i, v)| {
            assert_eq!(
                u32::try_from(scalar_at(packed.array(), i).unwrap().as_ref()).unwrap(),
                *v
            );
        });
    }
}
