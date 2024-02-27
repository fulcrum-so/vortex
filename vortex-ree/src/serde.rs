use std::io;

use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{REEArray, REEEncoding};

impl ArraySerde for REEArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write_usize(self.len())?;
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }
        // TODO(robert): Stop writing this
        ctx.dtype(self.ends().dtype())?;
        ctx.write(self.ends())?;
        ctx.write(self.values())
    }
}

impl EncodingSerde for REEEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let len = ctx.read_usize()?;
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.validity().read()?)
        } else {
            None
        };
        let ends_dtype = ctx.dtype()?;
        let ends = ctx.with_schema(&ends_dtype).read()?;
        let values = ctx.read()?;
        Ok(REEArray::new(ends, values, validity, len).boxed())
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::{Array, ArrayRef};
    use vortex::serde::{ReadCtx, WriteCtx};

    use crate::downcast::DowncastREE;
    use crate::REEArray;

    fn roundtrip_array(array: &dyn Array) -> io::Result<ArrayRef> {
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
            PrimitiveArray::from_vec(vec![0u8, 9, 20, 32, 49]).boxed(),
            PrimitiveArray::from_vec(vec![-7i64, -13, 17, 23]).boxed(),
            None,
            49,
        );
        let read_arr = roundtrip_array(arr.as_ref()).unwrap();
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
