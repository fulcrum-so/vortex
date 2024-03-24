use vortex::array::{Array, ArrayRef};
use vortex::error::VortexResult;
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{REEArray, REEEncoding};

impl ArraySerde for REEArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_usize(self.len())?;
        ctx.write_optional_array(self.validity())?;
        // TODO(robert): Stop writing this
        ctx.dtype(self.ends().dtype())?;
        ctx.write(self.ends())?;
        ctx.write(self.values())
    }
}

impl EncodingSerde for REEEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let len = ctx.read_usize()?;
        let validity = ctx.validity().read_optional_array()?;
        let ends_dtype = ctx.dtype()?;
        let ends = ctx.with_schema(&ends_dtype).read()?;
        let values = ctx.read()?;
        Ok(REEArray::new(ends, values, validity, len).into_array())
    }
}

#[cfg(test)]
mod test {

    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::IntoArray;
    use vortex::array::{Array, ArrayRef};
    use vortex::error::VortexResult;
    use vortex::serde::{ReadCtx, WriteCtx};

    use crate::downcast::DowncastREE;
    use crate::REEArray;

    fn roundtrip_array(array: &dyn Array) -> VortexResult<ArrayRef> {
        let mut buf = Vec::<u8>::new();
        let mut write_ctx = WriteCtx::new(&mut buf);
        write_ctx.write(array)?;
        let mut read = buf.as_slice();
        let mut read_ctx = ReadCtx::new(array.dtype(), &mut read);
        read_ctx.read()
    }

    #[test]
    fn roundtrip() {
        let arr = REEArray::new(
            vec![0u8, 9, 20, 32, 49].into_array(),
            vec![-7i64, -13, 17, 23].into_array(),
            None,
            49,
        );
        let read_arr = roundtrip_array(&arr).unwrap();
        let read_ree = read_arr.as_ree();

        assert_eq!(
            arr.ends().as_primitive().buffer().typed_data::<u8>(),
            read_ree.ends().as_primitive().buffer().typed_data::<u8>()
        );
        assert_eq!(
            arr.values().as_primitive().buffer().typed_data::<i64>(),
            read_ree
                .values()
                .as_primitive()
                .buffer()
                .typed_data::<i64>()
        );
    }
}
