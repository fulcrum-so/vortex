use std::io;
use std::io::ErrorKind;

use croaring::{Bitmap, Portable};

use vortex::array::{Array, ArrayRef};
use vortex::ptype::PType;
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use vortex_error::VortexResult;

use crate::{RoaringIntArray, RoaringIntEncoding};

impl ArraySerde for RoaringIntArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        let mut data = Vec::new();
        self.bitmap().serialize_into::<Portable>(&mut data);
        ctx.write_slice(data.as_slice())
    }
}

impl EncodingSerde for RoaringIntEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let bitmap_data = ctx.read_slice()?;
        let ptype: PType = ctx
            .schema()
            .try_into()
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        Ok(RoaringIntArray::new(
            Bitmap::try_deserialize::<Portable>(bitmap_data.as_slice())
                .ok_or(io::Error::new(ErrorKind::InvalidData, "invalid bitmap"))?,
            ptype,
        )
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use croaring::Bitmap;

    use vortex::ptype::PType;

    use crate::downcast::DowncastRoaring;
    use crate::serde_tests::test::roundtrip_array;
    use crate::RoaringIntArray;

    #[test]
    fn roundtrip() {
        let arr = RoaringIntArray::new(Bitmap::from_range(245..63000), PType::U32);
        let read_arr = roundtrip_array(&arr).unwrap();
        let read_roaring = read_arr.as_roaring_int();
        assert_eq!(arr.ptype(), read_roaring.ptype());
        assert_eq!(arr.bitmap(), read_roaring.bitmap());
    }
}
