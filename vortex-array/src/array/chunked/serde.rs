use crate::array::chunked::{ChunkedArray, ChunkedEncoding};
use crate::array::{Array, ArrayRef};
use crate::error::VortexResult;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for ChunkedArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_usize(self.chunks().len())?;
        for c in self.chunks() {
            ctx.write(c.as_ref())?;
        }
        Ok(())
    }
}

impl EncodingSerde for ChunkedEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let chunk_len = ctx.read_usize()?;
        let mut chunks = Vec::<ArrayRef>::with_capacity(chunk_len);
        // TODO(robert): Use read_vectored
        for _ in 0..chunk_len {
            chunks.push(ctx.read()?);
        }
        Ok(ChunkedArray::new(chunks, ctx.schema().clone()).boxed())
    }
}

#[cfg(test)]
mod test {
    use crate::array::chunked::ChunkedArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::Array;
    use crate::serde::test::roundtrip_array;
    use vortex_schema::{DType, IntWidth, Nullability, Signedness};

    #[test]
    fn roundtrip() {
        let arr = ChunkedArray::new(
            vec![
                PrimitiveArray::from_iter(vec![Some(0), None, Some(2), Some(42)]).boxed(),
                PrimitiveArray::from_iter(vec![Some(5), None, Some(7), Some(42)]).boxed(),
            ],
            DType::Int(IntWidth::_32, Signedness::Signed, Nullability::Nullable),
        );

        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        for (i, chunk) in arr.chunks().iter().enumerate() {
            assert_eq!(
                chunk.as_primitive().buffer().typed_data::<i32>(),
                read_arr.as_chunked().chunks()[i]
                    .as_primitive()
                    .buffer()
                    .typed_data::<i32>()
            );
        }
    }
}
