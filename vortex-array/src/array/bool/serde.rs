use arrow_buffer::buffer::BooleanBuffer;

use crate::array::bool::{BoolArray, BoolEncoding};
use crate::array::{Array, ArrayRef};
use crate::error::VortexResult;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl<'a> ArraySerde for &'a BoolArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }
        ctx.write_buffer(self.len(), &self.buffer().sliced())
    }
}

impl EncodingSerde for BoolEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.validity().read()?)
        } else {
            None
        };

        let (logical_len, buf) = ctx.read_buffer(|len| (len + 7) / 8)?;
        Ok(BoolArray::new(BooleanBuffer::new(buf, 0, logical_len), validity).boxed())
    }
}

#[cfg(test)]
mod test {
    use crate::array::bool::BoolArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = BoolArray::from_iter(vec![Some(false), None, Some(true), Some(false)]);
        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        assert_eq!(arr.buffer().values(), read_arr.as_bool().buffer().values());
        assert_eq!(
            arr.validity().unwrap().as_bool().buffer().values(),
            read_arr
                .as_bool()
                .validity()
                .unwrap()
                .as_bool()
                .buffer()
                .values()
        );
    }
}
