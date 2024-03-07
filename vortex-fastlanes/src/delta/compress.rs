use arrayref::array_ref;
use log::debug;
use std::mem::size_of;

use fastlanez_sys::{transpose, Delta};
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::error::VortexResult;
use vortex::match_each_signed_integer_ptype;
use vortex::ptype::NativePType;

use crate::{DeltaArray, DeltaEncoding};

impl EncodingCompression for DeltaEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let Some(parray) = array.maybe_primitive() else {
            debug!("Skipping Delta: not primitive");
            return None;
        };

        // Only supports signed ints
        if !parray.ptype().is_signed_int() {
            debug!("Skipping Delta: not int");
            return None;
        }

        debug!("Compressing with Delta");
        Some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: &CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let parray = array.as_primitive();
        let like_delta = like.map(|l| l.as_any().downcast_ref::<DeltaArray>().unwrap());

        let validity = parray
            .validity()
            .map(|v| ctx.compress(v.as_ref(), like_delta.and_then(|d| d.validity())))
            .transpose()?;

        let delta_encoded = match_each_signed_integer_ptype!(parray.ptype(), |$T| {
            PrimitiveArray::from(delta_primitive(parray.buffer().typed_data::<$T>()))
        });

        let encoded = ctx.next_level().compress(
            delta_encoded.as_ref(),
            like_delta.map(|d| d.encoded().as_ref()),
        )?;

        return Ok(DeltaArray::try_new(array.len(), encoded, validity)
            .unwrap()
            .boxed());
    }
}

fn delta_primitive<T: NativePType + Delta>(array: &[T]) -> Vec<T>
where
    [(); 128 / size_of::<T>()]:,
{
    // How many fastlanes vectors we will process.
    let num_chunks = (array.len() + 1023) / 1024;

    // Allocate a result array.
    let mut output = Vec::with_capacity(array.len());

    // Start with a base vector of zeros.
    let mut base = [T::default(); 128 / size_of::<T>()];

    // Loop over all but the last chunk.
    (0..num_chunks - 1).for_each(|i| {
        let start_elem = i * 1024;
        let chunk: &[T; 1024] = array_ref![array, start_elem, 1024];
        let transposed = transpose(chunk);
        Delta::delta(&transposed, &mut base, &mut output);
    });

    // Pad the last chunk with zeros to a full 1024 elements.
    let last_chunk_size = array.len() % 1024;
    let mut last_chunk: [T; 1024] = [T::default(); 1024];
    last_chunk[..last_chunk_size].copy_from_slice(&array[array.len() - last_chunk_size..]);
    Delta::delta(&last_chunk, &mut base, &mut output);

    output
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use vortex::array::primitive::PrimitiveEncoding;
    use vortex::array::Encoding;

    use super::*;

    #[test]
    fn test_compress() {
        // FIXME(ngates): remove PrimitiveEncoding https://github.com/fulcrum-so/vortex/issues/35
        let cfg = CompressConfig::new(
            HashSet::from([PrimitiveEncoding.id(), DeltaEncoding.id()]),
            HashSet::default(),
        );
        let ctx = CompressCtx::new(&cfg);

        let compressed = ctx.compress(
            &PrimitiveArray::from_vec(Vec::from_iter((0..10_000).map(|i| (i % 63) as u8))),
            None,
        );
        assert_eq!(compressed.encoding().id(), DeltaEncoding.id());
        _ = compressed.as_any().downcast_ref::<DeltaArray>().unwrap();
    }
}
