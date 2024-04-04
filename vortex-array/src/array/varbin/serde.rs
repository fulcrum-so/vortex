use vortex_error::VortexResult;

use crate::array::varbin::{VarBinArray, VarBinEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for VarBinArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_validity(self.validity())?;
        ctx.dtype(self.offsets().dtype())?;
        ctx.write(self.offsets())?;
        ctx.write(self.bytes())
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        Ok(None)
    }
}

impl EncodingSerde for VarBinEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let validity = ctx.read_validity()?;
        // TODO(robert): Stop writing this
        let offsets_dtype = ctx.dtype()?;
        let offsets = ctx.with_schema(&offsets_dtype).read()?;
        let bytes = ctx.bytes().read()?;
        Ok(VarBinArray::new(offsets, bytes, ctx.schema().clone(), validity).into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex_schema::{DType, Nullability};

    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::TypedPrimitiveTrait;
    use crate::array::varbin::VarBinArray;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = VarBinArray::from_vec(
            vec!["a", "def", "hello", "this", "is", "a", "test"],
            DType::Utf8(Nullability::NonNullable),
        );

        let read_arr = roundtrip_array(&arr).unwrap();

        assert_eq!(
            arr.offsets().as_primitive().typed_data::<u32>(),
            read_arr
                .as_varbin()
                .offsets()
                .as_primitive()
                .typed_data::<u32>()
        );

        assert_eq!(
            arr.bytes().as_primitive().typed_data::<u8>(),
            read_arr
                .as_varbin()
                .bytes()
                .as_primitive()
                .typed_data::<u8>()
        );
    }
}
