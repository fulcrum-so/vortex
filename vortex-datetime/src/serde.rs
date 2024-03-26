use crate::{DateTimeArray, DateTimeEncoding};
use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use vortex::validity::ArrayValidity;
use vortex_error::VortexResult;

impl ArraySerde for DateTimeArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write(self.days())?;
        ctx.write(self.seconds())?;
        ctx.write(self.subsecond())?;
        ctx.write_validity(self.validity())
    }
}

impl EncodingSerde for DateTimeEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        Ok(DateTimeArray::new(
            ctx.read()?,
            ctx.read()?,
            ctx.read()?,
            ctx.read_validity()?,
            ctx.schema().clone(),
        )
        .into_array())
    }
}
