use croaring::Bitmap;
use num_traits::NumCast;
use vortex::array::primitive::PrimitiveArray;
use vortex_dtype::{NativePType, PType};
use vortex_error::VortexResult;

use crate::RoaringIntArray;

pub fn roaring_int_encode(parray: PrimitiveArray) -> VortexResult<RoaringIntArray> {
    match parray.ptype() {
        PType::U8 => roaring_encode_primitive::<u8>(parray.maybe_null_slice()),
        PType::U16 => roaring_encode_primitive::<u16>(parray.maybe_null_slice()),
        PType::U32 => roaring_encode_primitive::<u32>(parray.maybe_null_slice()),
        PType::U64 => roaring_encode_primitive::<u64>(parray.maybe_null_slice()),
        _ => panic!("Unsupported ptype {}", parray.ptype()),
    }
}

fn roaring_encode_primitive<T: NumCast + NativePType>(
    values: &[T],
) -> VortexResult<RoaringIntArray> {
    let mut bitmap = Bitmap::new();
    bitmap.extend(values.iter().map(|i| i.to_u32().unwrap()));
    bitmap.run_optimize();
    bitmap.shrink_to_fit();
    RoaringIntArray::try_new(bitmap, T::PTYPE)
}
