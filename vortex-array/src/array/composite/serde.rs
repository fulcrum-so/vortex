use std::sync::Arc;

use vortex_schema::DType;

use crate::array::composite::{CompositeArray, CompositeEncoding};
use crate::array::{Array, ArrayRef};
use crate::error::VortexResult;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for CompositeArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_slice(self.metadata().as_slice())?;
        let underlying = self.underlying();
        ctx.dtype(underlying.dtype())?;
        ctx.write(self.underlying())
    }
}

impl EncodingSerde for CompositeEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let DType::Composite(id, _) = *ctx.schema() else {
            panic!("Expected composite schema")
        };
        let metadata = ctx.read_slice()?;
        let underling_dtype = ctx.dtype()?;
        let underlying = ctx.with_schema(&underling_dtype).read()?;

        Ok(CompositeArray::new(id, Arc::new(metadata), underlying).into_array())
    }
}
