use vortex_error::VortexResult;

use crate::array::constant::{ConstantArray, ConstantEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for ConstantArray {
    fn write(&self, ctx: &mut WriteCtx<'_>) -> VortexResult<()> {
        ctx.write_usize(self.len())?;
        ctx.scalar(self.scalar())
    }
}

impl EncodingSerde for ConstantEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let len = ctx.read_usize()?;
        let scalar = ctx.scalar()?;
        Ok(ConstantArray::new(scalar, len).into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::constant::ConstantArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::Array;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = ConstantArray::new(42i32, 100);
        let read_arr = roundtrip_array(&arr).unwrap();

        assert_eq!(arr.scalar(), read_arr.as_constant().scalar());
        assert_eq!(arr.len(), read_arr.len());
    }
}
