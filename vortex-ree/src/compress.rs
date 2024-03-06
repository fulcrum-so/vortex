use itertools::Itertools;
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding};
use vortex::array::{Array, ArrayRef, Encoding};
use vortex::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use vortex::compute::cast::cast_primitive;
use vortex::error::VortexResult;
use vortex::ptype::{match_each_native_ptype, NativePType};
use vortex::stats::Stat;

use crate::downcast::DowncastREE;
use crate::{REEArray, REEEncoding};

impl EncodingCompression for REEEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.encoding().id() != PrimitiveEncoding.id() {
            return None;
        }

        let avg_run_length = array.len() as f32
            / array
                .stats()
                .get_or_compute_or::<usize>(array.len(), &Stat::RunCount) as f32;
        if avg_run_length < config.ree_average_run_threshold {
            return None;
        }

        Some(&(ree_compressor as Compressor))
    }
}

fn ree_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let ree_like = like.map(|like_arr| like_arr.as_ree());
    let primitive_array = array.as_primitive();

    let (ends, values) = ree_encode(primitive_array);
    let compressed_ends = ctx
        .next_level()
        .compress(ends.as_ref(), ree_like.map(|ree| ree.ends()));
    let compressed_values = ctx
        .next_level()
        .compress(values.as_ref(), ree_like.map(|ree| ree.values()));

    REEArray::new(
        compressed_ends,
        compressed_values,
        primitive_array
            .validity()
            .map(|v| ctx.compress(v, ree_like.and_then(|r| r.validity()))),
        array.len(),
    )
    .boxed()
}

pub fn ree_encode(array: &PrimitiveArray) -> (PrimitiveArray, PrimitiveArray) {
    match_each_native_ptype!(array.ptype(), |$P| {
        let (ends, values) = ree_encode_primitive(array.typed_data::<$P>());

        let compressed_values = PrimitiveArray::from(values);
        compressed_values.stats().set(Stat::IsConstant, false.into());
        compressed_values.stats().set(Stat::RunCount, compressed_values.len().into());
        compressed_values.stats().set_many(&array.stats(), vec![
            &Stat::Min, &Stat::Max, &Stat::IsSorted, &Stat::IsStrictSorted,
        ]);

        let compressed_ends = PrimitiveArray::from(ends);
        compressed_ends.stats().set(Stat::IsSorted, true.into());
        compressed_ends.stats().set(Stat::IsStrictSorted, true.into());
        compressed_ends.stats().set(Stat::IsConstant, false.into());
        compressed_ends.stats().set(Stat::Max, array.len().into());
        compressed_ends.stats().set(Stat::RunCount, compressed_ends.len().into());

        (compressed_ends, compressed_values)
    })
}

fn ree_encode_primitive<T: NativePType>(elements: &[T]) -> (Vec<u64>, Vec<T>) {
    let mut ends = Vec::new();
    let mut values = Vec::new();

    if elements.is_empty() {
        return (ends, values);
    }

    // Run-end encode the values
    let mut last = elements[0];
    let mut end = 1;
    for &e in elements.iter().skip(1) {
        if e != last {
            ends.push(end);
            values.push(last);
        }
        last = e;
        end += 1;
    }
    ends.push(end);
    values.push(last);

    (ends, values)
}

#[allow(dead_code)]
pub fn ree_decode(
    ends: &PrimitiveArray,
    values: &PrimitiveArray,
    validity: Option<ArrayRef>,
) -> VortexResult<PrimitiveArray> {
    // TODO(ngates): switch over ends without necessarily casting
    match_each_native_ptype!(values.ptype(), |$P| {
        Ok(PrimitiveArray::from_nullable(ree_decode_primitive(
            cast_primitive(ends, &PType::U64)?.typed_data(),
            values.typed_data::<$P>(),
        ), validity))
    })
}

pub fn ree_decode_primitive<T: NativePType>(run_ends: &[u64], values: &[T]) -> Vec<T> {
    let mut decoded = Vec::with_capacity(run_ends.last().map(|x| *x as usize).unwrap_or(0_usize));
    for (&end, &value) in run_ends.iter().zip_eq(values) {
        decoded.extend(std::iter::repeat(value).take(end as usize - decoded.len()));
    }
    decoded
}

#[cfg(test)]
mod test {
    use arrow::buffer::BooleanBuffer;

    use vortex::array::bool::BoolArray;
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::{Array, CloneOptionalArray};

    use crate::compress::{ree_decode, ree_encode};
    use crate::REEArray;

    #[test]
    fn encode() {
        let arr = PrimitiveArray::from(vec![1i32, 1, 2, 2, 2, 3, 3, 3, 3, 3]);
        let (ends, values) = ree_encode(&arr);

        assert_eq!(ends.typed_data::<u64>(), vec![2, 5, 10]);
        assert_eq!(values.typed_data::<i32>(), vec![1, 2, 3]);
    }

    #[test]
    fn decode() {
        let ends = PrimitiveArray::from(vec![2, 5, 10]);
        let values = PrimitiveArray::from(vec![1i32, 2, 3]);
        let decoded = ree_decode(&ends, &values, None).unwrap();

        assert_eq!(
            decoded.typed_data::<i32>(),
            vec![1i32, 1, 2, 2, 2, 3, 3, 3, 3, 3]
        );
    }

    #[test]
    fn decode_nullable() {
        let validity = {
            let mut validity = vec![true; 10];
            validity[2] = false;
            validity[7] = false;
            BoolArray::from(validity)
        };
        let arr = REEArray::new(
            vec![2u32, 5, 10].into(),
            vec![1i32, 2, 3].into(),
            Some(validity.boxed()),
            10,
        );

        let decoded = ree_decode(
            arr.ends().as_primitive(),
            arr.values().as_primitive(),
            arr.validity().clone_optional(),
        )
        .unwrap();

        assert_eq!(
            decoded.buffer().typed_data::<i32>(),
            vec![1i32, 1, 2, 2, 2, 3, 3, 3, 3, 3].as_slice()
        );
        assert_eq!(
            decoded.validity().unwrap().as_bool().buffer(),
            &BooleanBuffer::from(vec![
                true, true, false, true, true, true, true, false, true, true,
            ])
        );
    }
}
