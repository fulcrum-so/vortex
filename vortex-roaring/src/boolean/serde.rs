use std::io;
use std::io::ErrorKind;

use croaring::{Bitmap, Portable};

use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use vortex_error::VortexResult;

use crate::{RoaringBoolArray, RoaringBoolEncoding};

impl ArraySerde for RoaringBoolArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_usize(self.len())?;
        let mut data = Vec::new();
        self.bitmap().serialize_into::<Portable>(&mut data);
        ctx.write_slice(data.as_slice())
    }
}

impl EncodingSerde for RoaringBoolEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let len = ctx.read_usize()?;
        let bitmap_data = ctx.read_slice()?;
        Ok(RoaringBoolArray::new(
            Bitmap::try_deserialize::<Portable>(bitmap_data.as_slice())
                .ok_or(io::Error::new(ErrorKind::InvalidData, "invalid bitmap"))?,
            len,
        )
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::downcast::DowncastRoaring;
    use croaring::Bitmap;

    use crate::serde_tests::test::roundtrip_array;
    use crate::RoaringBoolArray;

    #[test]
    fn roundtrip() {
        let arr = RoaringBoolArray::new(Bitmap::from_range(245..63000), 65536);
        let read_arr = roundtrip_array(&arr).unwrap();

        let read_roaring = read_arr.as_roaring_bool();
        assert_eq!(arr.bitmap(), read_roaring.bitmap());
    }
}
