use flexbuffers::Builder;
use vortex_error::VortexResult;

use crate::array::chunked::{ChunkedArray, ChunkedEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for ChunkedArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_usize(self.chunks().len())?;
        for c in self.chunks() {
            ctx.write(c.as_ref())?;
        }
        Ok(())
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        // TODO(ngates) #163 - the chunk lengths should probably themselves be an array?
        let mut builder = Builder::default();
        let mut vec = builder.start_vector();
        for end in self.chunk_ends() {
            vec.push(*end);
        }
        vec.end_vector();
        Ok(Some(builder.take_buffer()))
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
        Ok(ChunkedArray::new(chunks, ctx.schema().clone()).into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex_schema::{DType, IntWidth, Nullability, Signedness};

    use crate::array::chunked::ChunkedArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::Array;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = ChunkedArray::new(
            vec![
                PrimitiveArray::from_iter(vec![Some(0), None, Some(2), Some(42)]).into_array(),
                PrimitiveArray::from_iter(vec![Some(5), None, Some(7), Some(42)]).into_array(),
            ],
            DType::Int(IntWidth::_32, Signedness::Signed, Nullability::Nullable),
        );

        let read_arr = roundtrip_array(&arr).unwrap();

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
