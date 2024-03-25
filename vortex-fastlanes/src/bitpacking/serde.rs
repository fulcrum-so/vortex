use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use vortex_error::VortexResult;

use crate::{BitPackedArray, BitPackedEncoding};

impl ArraySerde for BitPackedArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write(self.encoded())?;
        ctx.write_optional_array(self.validity())?;
        ctx.write_optional_array(self.patches())?;
        ctx.write_usize(self.bit_width())?;
        ctx.write_usize(self.len())
    }
}

impl EncodingSerde for BitPackedEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let encoded = ctx.bytes().read()?;
        let validity = ctx.validity().read_optional_array()?;
        let patches = ctx.read_optional_array()?;
        let bit_width = ctx.read_usize()?;
        let len = ctx.read_usize()?;
        Ok(BitPackedArray::try_new(
            encoded,
            validity,
            patches,
            bit_width,
            ctx.schema().clone(),
            len,
        )
        .unwrap()
        .into_array())
    }
}
