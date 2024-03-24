use crate::{DateTimeArray, DateTimeEncoding};
use vortex::array::{Array, ArrayRef};
use vortex::error::VortexResult;
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for DateTimeArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write(self.days())?;
        ctx.write(self.seconds())?;
        ctx.write(self.subsecond())?;
        ctx.write_optional_array(self.validity())
    }
}

impl EncodingSerde for DateTimeEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        Ok(DateTimeArray::new(
            ctx.read()?,
            ctx.read()?,
            ctx.read()?,
            ctx.validity().read_optional_array()?,
            ctx.schema().clone(),
        )
        .into_array())
    }
}
