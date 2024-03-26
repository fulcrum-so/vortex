use arrow_buffer::buffer::BooleanBuffer;

use vortex_error::VortexResult;

use crate::array::bool::{BoolArray, BoolEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use crate::validity::ArrayValidity;

impl<'a> ArraySerde for BoolArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_validity(self.validity())?;
        ctx.write_buffer(self.len(), &self.buffer().sliced())
    }
}

impl EncodingSerde for BoolEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let validity = ctx.read_validity()?;
        let (logical_len, buf) = ctx.read_buffer(|len| (len + 7) / 8)?;
        Ok(BoolArray::new(BooleanBuffer::new(buf, 0, logical_len), validity).into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::bool::BoolArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::serde::test::roundtrip_array;
    use crate::validity::ArrayValidity;

    #[test]
    fn roundtrip() {
        let arr = BoolArray::from_iter(vec![Some(false), None, Some(true), Some(false)]);
        let read_arr = roundtrip_array(&arr).unwrap();

        assert_eq!(arr.buffer().values(), read_arr.as_bool().buffer().values());
        assert_eq!(arr.validity(), read_arr.validity());
    }
}
