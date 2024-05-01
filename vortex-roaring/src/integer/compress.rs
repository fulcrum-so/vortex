use croaring::Bitmap;
use log::debug;
use num_traits::NumCast;
use vortex::array::primitive::PrimitiveArray;
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::stats::ArrayStatistics;
use vortex::{Array, ArrayDType, ArrayDef, IntoArray, OwnedArray, ToStatic};
use vortex_dtype::{NativePType, PType};
use vortex_error::VortexResult;

use crate::{OwnedRoaringIntArray, RoaringInt, RoaringIntArray, RoaringIntEncoding};

impl EncodingCompression for RoaringIntEncoding {
    fn can_compress(
        &self,
        array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive enc arrays
        if array.encoding().id() != RoaringInt::ID {
            return None;
        }

        // Only support non-nullable uint arrays
        if !array.dtype().is_unsigned_int() || array.dtype().is_nullable() {
            debug!("Skipping roaring int, not a uint");
            return None;
        }

        // Only support sorted unique arrays
        if !array
            .statistics()
            .compute_is_strict_sorted()
            .unwrap_or(false)
        {
            debug!("Skipping roaring int, not strict sorted");
            return None;
        }

        if array.statistics().compute_max().unwrap_or(0) > u32::MAX as usize {
            debug!("Skipping roaring int, max is larger than {}", u32::MAX);
            return None;
        }

        debug!("Using roaring int");
        Some(self)
    }

    fn compress(
        &self,
        array: &Array,
        _like: Option<&Array>,
        _ctx: CompressCtx,
    ) -> VortexResult<OwnedArray> {
        let parray = array.clone().flatten_primitive()?;
        Ok(roaring_encode(parray).into_array().to_static())
    }
}

pub fn roaring_encode(parray: PrimitiveArray) -> RoaringIntArray {
    match parray.ptype() {
        PType::U8 => roaring_encode_primitive::<u8>(parray.typed_data()),
        PType::U16 => roaring_encode_primitive::<u16>(parray.typed_data()),
        PType::U32 => roaring_encode_primitive::<u32>(parray.typed_data()),
        PType::U64 => roaring_encode_primitive::<u64>(parray.typed_data()),
        _ => panic!("Unsupported ptype {}", parray.ptype()),
    }
}

fn roaring_encode_primitive<T: NumCast + NativePType>(values: &[T]) -> OwnedRoaringIntArray {
    let mut bitmap = Bitmap::new();
    bitmap.extend(values.iter().map(|i| i.to_u32().unwrap()));
    bitmap.run_optimize();
    bitmap.shrink_to_fit();
    RoaringIntArray::new(bitmap, T::PTYPE)
}
