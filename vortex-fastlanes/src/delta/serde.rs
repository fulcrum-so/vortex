use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use vortex::validity::ArrayValidity;
use vortex_error::VortexResult;

use crate::{DeltaArray, DeltaEncoding};

impl ArraySerde for DeltaArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_usize(self.len())?;
        ctx.write(self.bases())?;
        ctx.write(self.deltas())?;
        ctx.write_validity(self.validity())
    }
}

impl EncodingSerde for DeltaEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let len = ctx.read_usize()?;
        let bases = ctx.read()?;
        let deltas = ctx.read()?;
        let validity = ctx.read_validity()?;
        Ok(DeltaArray::try_new(len, bases, deltas, validity)
            .unwrap()
            .into_array())
    }
}
