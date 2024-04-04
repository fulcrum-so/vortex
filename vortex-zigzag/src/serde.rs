use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use vortex_error::VortexResult;
use vortex_schema::{DType, Signedness};

use crate::{ZigZagArray, ZigZagEncoding};

impl ArraySerde for ZigZagArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write(self.encoded())
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        Ok(None)
    }
}

impl EncodingSerde for ZigZagEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let encoded = ctx.with_schema(&encoded_dtype(ctx.schema())).read()?;
        Ok(ZigZagArray::new(encoded).into_array())
    }
}

fn encoded_dtype(schema: &DType) -> DType {
    match schema {
        DType::Int(w, Signedness::Signed, n) => DType::Int(*w, Signedness::Unsigned, *n),
        _ => panic!("Invalid schema"),
    }
}

#[cfg(test)]
mod test {
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::{PrimitiveArray, TypedPrimitiveTrait};
    use vortex::array::{Array, ArrayRef};
    use vortex::serde::{ReadCtx, WriteCtx};
    use vortex_error::VortexResult;

    use crate::compress::zigzag_encode;
    use crate::downcast::DowncastZigzag;

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
        let arr = zigzag_encode(&PrimitiveArray::from(vec![-7i64, -13, 17, 23])).unwrap();
        let read_arr = roundtrip_array(&arr).unwrap();

        let read_zigzag = read_arr.as_zigzag();
        assert_eq!(
            arr.encoded().as_primitive().typed_data::<u8>(),
            read_zigzag.encoded().as_primitive().typed_data::<u8>()
        );
    }
}
