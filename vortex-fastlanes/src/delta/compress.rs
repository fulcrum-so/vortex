use std::mem::size_of;

use arrayref::array_ref;
use num_traits::{WrappingAdd, WrappingSub};

use fastlanez_sys::{transpose, untranspose, Delta};
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::fill::fill_forward;
use vortex::compute::flatten::flatten_primitive;
use vortex::match_each_integer_ptype;
use vortex::ptype::NativePType;
use vortex::validity::ArrayValidity;
use vortex_error::VortexResult;

use crate::{DeltaArray, DeltaEncoding};

impl EncodingCompression for DeltaEncoding {
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

        Some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let parray = array.as_primitive();
        let like_delta = like.map(|l| l.as_any().downcast_ref::<DeltaArray>().unwrap());

        let validity = ctx.compress_validity(parray.validity())?;

        // Fill forward nulls
        let filled = fill_forward(array)?;

        // Compress the filled array
        let (bases, deltas) = match_each_integer_ptype!(parray.ptype(), |$T| {
            let (bases, deltas) = compress_primitive(filled.as_primitive().typed_data::<$T>());
            (PrimitiveArray::from(bases), PrimitiveArray::from(deltas))
        });

        // Recursively compress the bases and deltas
        let bases = ctx
            .named("bases")
            .compress(&bases, like_delta.map(|d| d.bases()))?;
        let deltas = ctx
            .named("deltas")
            .compress(&deltas, like_delta.map(|d| d.deltas()))?;

        Ok(DeltaArray::try_new(array.len(), bases, deltas, validity)?.into_array())
    }
}

fn compress_primitive<T: NativePType + Delta + WrappingSub>(array: &[T]) -> (Vec<T>, Vec<T>)
where
    [(); 128 / size_of::<T>()]:,
{
    // How many fastlanes vectors we will process.
    let num_chunks = array.len() / 1024;

    // How long each base vector will be.
    let lanes = T::lanes();

    // Allocate result arrays.
    let mut bases = Vec::with_capacity(num_chunks * lanes + 1);
    let mut deltas = Vec::with_capacity(array.len());

    // Loop over all of the 1024-element chunks.
    if num_chunks > 0 {
        let mut transposed: [T; 1024] = [T::default(); 1024];
        let mut base = [T::default(); 128 / size_of::<T>()];
        assert_eq!(base.len(), lanes);

        for i in 0..num_chunks {
            let start_elem = i * 1024;
            let chunk: &[T; 1024] = array_ref![array, start_elem, 1024];
            transpose(chunk, &mut transposed);

            // Initialize and store the base vector for each chunk
            base.copy_from_slice(&transposed[0..lanes]);
            bases.extend(base);

            Delta::encode_transposed(&transposed, &mut base, &mut deltas);
        }
    }

    // To avoid padding, the remainder is encoded with scalar logic.
    let remainder_size = array.len() % 1024;
    if remainder_size > 0 {
        let chunk = &array[array.len() - remainder_size..];
        let mut base_scalar = chunk[0];
        bases.push(base_scalar);
        for next in chunk {
            let diff = next.wrapping_sub(&base_scalar);
            deltas.push(diff);
            base_scalar = *next;
        }
    }

    assert_eq!(
        bases.len(),
        num_chunks * lanes + (if remainder_size > 0 { 1 } else { 0 })
    );
    assert_eq!(deltas.len(), array.len());

    (bases, deltas)
}

pub fn decompress(array: &DeltaArray) -> VortexResult<PrimitiveArray> {
    let bases = flatten_primitive(array.bases())?;
    let deltas = flatten_primitive(array.deltas())?;
    let decoded = match_each_integer_ptype!(deltas.ptype(), |$T| {
        PrimitiveArray::from_nullable(
            decompress_primitive::<$T>(bases.typed_data(), deltas.typed_data()),
            array.validity()
        )
    });
    Ok(decoded)
}

fn decompress_primitive<T: NativePType + Delta + WrappingAdd>(bases: &[T], deltas: &[T]) -> Vec<T>
where
    [(); 128 / size_of::<T>()]:,
{
    // How many fastlanes vectors we will process.
    let num_chunks = deltas.len() / 1024;

    // How long each base vector will be.
    let lanes = T::lanes();

    // Allocate a result array.
    let mut output = Vec::with_capacity(deltas.len());

    // Loop over all the chunks
    if num_chunks > 0 {
        let mut transposed: [T; 1024] = [T::default(); 1024];
        let mut base = [T::default(); 128 / size_of::<T>()];
        assert_eq!(base.len(), lanes);

        for i in 0..num_chunks {
            let start_elem = i * 1024;
            let chunk: &[T; 1024] = array_ref![deltas, start_elem, 1024];

            // Initialize the base vector for this chunk
            base.copy_from_slice(&bases[i * lanes..(i + 1) * lanes]);
            Delta::decode_transposed(chunk, &mut base, &mut transposed);
            untranspose(&transposed, &mut output);
        }
    }
    assert_eq!(output.len() % 1024, 0);

    // The remainder was encoded with scalar logic, so we need to scalar decode it.
    let remainder_size = deltas.len() % 1024;
    if remainder_size > 0 {
        let chunk = &deltas[num_chunks * 1024..];
        assert_eq!(bases.len(), num_chunks * lanes + 1);
        let mut base_scalar = bases[num_chunks * lanes];
        for next_diff in chunk {
            let next = next_diff.wrapping_add(&base_scalar);
            output.push(next);
            base_scalar = next;
        }
    }

    output
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex::array::{Encoding, EncodingRef};

    use super::*;

    fn compress_ctx() -> CompressCtx {
        let cfg = CompressConfig::new().with_enabled([&DeltaEncoding as EncodingRef]);
        CompressCtx::new(Arc::new(cfg))
    }

    #[test]
    fn test_compress() {
        do_roundtrip_test(Vec::from_iter(0..10_000));
    }

    #[test]
    fn test_compress_overflow() {
        do_roundtrip_test(Vec::from_iter(
            (0..10_000).map(|i| (i % (u8::MAX as i32)) as u8),
        ));
    }

    fn do_roundtrip_test<T: NativePType>(input: Vec<T>) {
        let ctx = compress_ctx();
        let compressed = DeltaEncoding {}
            .compress(&PrimitiveArray::from(input.clone()), None, ctx)
            .unwrap();

        assert_eq!(compressed.encoding().id(), DeltaEncoding.id());
        let delta = compressed.as_any().downcast_ref::<DeltaArray>().unwrap();

        let decompressed = decompress(delta).unwrap();
        let decompressed_slice = decompressed.typed_data::<T>();
        assert_eq!(decompressed_slice.len(), input.len());
        for (actual, expected) in decompressed_slice.iter().zip(input) {
            assert_eq!(actual, &expected);
        }
    }
}
