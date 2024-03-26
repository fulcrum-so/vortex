use zigzag::ZigZag;

use crate::downcast::DowncastZigzag;
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayKind, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::ptype::{NativePType, PType};
use vortex::stats::Stat;
use vortex::validity::{ArrayValidity, Validity};
use vortex_alloc::{AlignedVec, ALIGNED_ALLOCATOR};
use vortex_error::VortexResult;

use crate::zigzag::{ZigZagArray, ZigZagEncoding};

impl EncodingCompression for ZigZagEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let parray = array.maybe_primitive()?;

        // Only supports signed integers
        if !parray.ptype().is_signed_int() {
            return None;
        }

        // Only compress if the array has negative values
        // TODO(ngates): also check that Stat::Max is less than half the max value of the type
        parray
            .stats()
            .get_or_compute_cast::<i64>(&Stat::Min)
            .filter(|&min| min < 0)
            .map(|_| self as &dyn EncodingCompression)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let zigzag_like = like.map(|like_arr| like_arr.as_zigzag());
        let encoded = match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => zigzag_encode(p),
            _ => unreachable!("This array kind should have been filtered out"),
        }
        .unwrap();

        Ok(
            ZigZagArray::new(ctx.compress(encoded.encoded(), zigzag_like.map(|z| z.encoded()))?)
                .into_array(),
        )
    }
}

pub fn zigzag_encode(parray: &PrimitiveArray) -> VortexResult<ZigZagArray> {
    let encoded = match parray.ptype() {
        PType::I8 => zigzag_encode_primitive::<i8>(parray.buffer().typed_data(), parray.validity()),
        PType::I16 => {
            zigzag_encode_primitive::<i16>(parray.buffer().typed_data(), parray.validity())
        }
        PType::I32 => {
            zigzag_encode_primitive::<i32>(parray.buffer().typed_data(), parray.validity())
        }
        PType::I64 => {
            zigzag_encode_primitive::<i64>(parray.buffer().typed_data(), parray.validity())
        }
        _ => panic!("Unsupported ptype"),
    };
    ZigZagArray::try_new(encoded.into_array())
}

fn zigzag_encode_primitive<T: ZigZag + NativePType>(
    values: &[T],
    validity: Option<Validity>,
) -> PrimitiveArray
where
    <T as ZigZag>::UInt: NativePType,
{
    let mut encoded = AlignedVec::with_capacity_in(values.len(), ALIGNED_ALLOCATOR);
    encoded.extend(values.iter().map(|v| T::encode(*v)));
    PrimitiveArray::from_nullable_in(encoded, validity)
}

#[allow(dead_code)]
pub fn zigzag_decode(parray: &PrimitiveArray) -> PrimitiveArray {
    match parray.ptype() {
        PType::U8 => zigzag_decode_primitive::<i8>(parray.buffer().typed_data(), parray.validity()),
        PType::U16 => {
            zigzag_decode_primitive::<i16>(parray.buffer().typed_data(), parray.validity())
        }
        PType::U32 => {
            zigzag_decode_primitive::<i32>(parray.buffer().typed_data(), parray.validity())
        }
        PType::U64 => {
            zigzag_decode_primitive::<i64>(parray.buffer().typed_data(), parray.validity())
        }
        _ => panic!("Unsupported ptype"),
    }
}

#[allow(dead_code)]
fn zigzag_decode_primitive<T: ZigZag + NativePType>(
    values: &[T::UInt],
    validity: Option<Validity>,
) -> PrimitiveArray
where
    <T as ZigZag>::UInt: NativePType,
{
    let mut encoded: AlignedVec<T> = AlignedVec::with_capacity_in(values.len(), ALIGNED_ALLOCATOR);
    encoded.extend(values.iter().map(|v| T::decode(*v)));
    PrimitiveArray::from_nullable_in(encoded, validity)
}
