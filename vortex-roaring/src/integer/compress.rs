use croaring::Bitmap;
use log::debug;
use num_traits::NumCast;
use vortex::{Array, ArrayDef, ArrayDType, IntoArray};
use vortex::array::primitive::{PrimitiveArray};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::ptype::{NativePType, PType};
use vortex::stats::{ArrayStatistics, Stat};
use vortex_error::VortexResult;
use vortex_schema::DType;
use vortex_schema::Nullability::NonNullable;
use vortex_schema::Signedness::Unsigned;

use crate::{RoaringInt, RoaringIntArray, RoaringIntEncoding, RoaringIntMetadata};

impl EncodingCompression for RoaringIntEncoding {
    fn can_compress(
        &self,
        array: & Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive enc arrays
        if array.encoding().id() != RoaringInt::ID {
            return None;
        }

        // Only support non-nullable uint arrays
        if !matches!(array.dtype(), DType::Int(_, Unsigned, NonNullable)) {
            debug!("Skipping roaring int, not non-nullable");
            return None;
        }

        // Only support sorted unique arrays
        if !array
            .statistics()
            .compute_as(Stat::IsStrictSorted)
            .map(|s| s.unwrap_or(false))
        {
            debug!("Skipping roaring int, not strict sorted");
            return None;
        }

        if array.statistics().compute_as(Stat::Max).map(|s| s > u32::MAX as usize).unwrap_or(false) {
            debug!("Skipping roaring int, max is larger than {}", u32::MAX);
            return None;
        }

        debug!("Using roaring int");
        Some(self)
    }

    fn compress(
        &self,
        array: & Array,
        _like: Option<& Array>,
        _ctx: CompressCtx,
    ) -> VortexResult<Array<'static>> {
        Ok(roaring_encode(array.clone().flatten_primitive()?).into_array())
    }
}

pub fn roaring_encode(primitive_array: PrimitiveArray) -> RoaringIntArray {
    match primitive_array.ptype() {
        PType::U8 => roaring_encode_primitive::<u8>(primitive_array.buffer().typed_data()),
        PType::U16 => roaring_encode_primitive::<u16>(primitive_array.buffer().typed_data()),
        PType::U32 => roaring_encode_primitive::<u32>(primitive_array.buffer().typed_data()),
        PType::U64 => roaring_encode_primitive::<u64>(primitive_array.buffer().typed_data()),
        _ => panic!("Unsupported ptype {}", primitive_array.ptype()),
    }
}

fn roaring_encode_primitive<T: NumCast + NativePType>(values: &[T]) -> RoaringIntArray {
    let mut bitmap = Bitmap::new();
    bitmap.extend(values.iter().map(|i| i.to_u32().unwrap()));
    bitmap.run_optimize();
    bitmap.shrink_to_fit();
    RoaringIntArray::new(bitmap, T::PTYPE)
}
