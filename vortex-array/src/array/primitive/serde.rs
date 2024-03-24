use crate::array::primitive::{PrimitiveArray, PrimitiveEncoding};
use crate::array::{Array, ArrayRef};
use crate::error::VortexResult;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for PrimitiveArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.ptype(self.ptype())?;
        ctx.write_optional_array(self.validity())?;
        ctx.write_buffer(self.len(), self.buffer())
    }
}

impl EncodingSerde for PrimitiveEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let ptype = ctx.ptype()?;
        let validity = ctx.validity().read_optional_array()?;
        let (_, buf) = ctx.read_buffer(|len| len * ptype.byte_width())?;
        Ok(PrimitiveArray::new(ptype, buf, validity).into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = PrimitiveArray::from_iter(vec![Some(0), None, Some(2), Some(42)]);
        let read_arr = roundtrip_array(&arr).unwrap();
        assert_eq!(
            arr.buffer().typed_data::<i32>(),
            read_arr.as_primitive().buffer().typed_data::<i32>()
        );

        assert_eq!(
            arr.validity().unwrap().as_bool().buffer().values(),
            read_arr
                .as_primitive()
                .validity()
                .unwrap()
                .as_bool()
                .buffer()
                .values()
        );
    }
}
