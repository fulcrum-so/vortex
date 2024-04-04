use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::DType;

use crate::array::struct_::{StructArray, StructEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for StructArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_usize(self.len())?;
        ctx.write_usize(self.fields().len())?;
        for f in self.fields() {
            ctx.write(f.as_ref())?;
        }
        Ok(())
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        let length = self.len() as u64;
        Ok(Some(length.to_le_bytes().to_vec()))
    }
}

impl EncodingSerde for StructEncoding {
    // fn to_array(&self, view: &ArrayView) -> ArrayRef {
    //     let DType::Struct(names, fields) = view.dtype() else {
    //         panic!("Incorrect DType {}", view.dtype())
    //     };
    //     assert_eq!(fields.len(), view.nchildren());
    //     StructArray::new(
    //         names.clone(),
    //         fields
    //             .iter()
    //             .enumerate()
    //             .map(|(i, field)| view.child(i, field).unwrap().into_array())
    //             .collect_vec(),
    //         self.len(view),
    //     )
    //     .into_array()
    // }

    // fn len(&self, view: &ArrayView) -> usize {
    //     let length = u64::from_le_bytes(view.metadata().unwrap().try_into().unwrap());
    //     length as usize
    // }

    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let len = ctx.read_usize()?;
        let num_fields = ctx.read_usize()?;
        let mut fields = Vec::<ArrayRef>::with_capacity(num_fields);
        // TODO(robert): use read_vectored
        for i in 0..num_fields {
            fields.push(ctx.subfield(i).read()?);
        }
        let DType::Struct(names, _) = ctx.schema() else {
            vortex_bail!(MismatchedTypes: "any struct", ctx.schema());
        };
        Ok(StructArray::new(names.clone(), fields, len).into_array())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::{PrimitiveArray, TypedPrimitiveTrait};
    use crate::array::struct_::StructArray;
    use crate::array::Array;
    use crate::array::IntoArray;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = StructArray::new(
            vec![
                Arc::new("primes".to_string()),
                Arc::new("nullable".to_string()),
            ],
            vec![
                vec![7u8, 37, 71, 97].into_array(),
                PrimitiveArray::from_iter(vec![Some(0), None, Some(2), Some(42)]).into_array(),
            ],
            4,
        );

        let read_arr = roundtrip_array(&arr).unwrap();

        assert_eq!(
            arr.fields()[0].as_primitive().typed_data::<u8>(),
            read_arr.as_struct().fields()[0]
                .as_primitive()
                .typed_data::<u8>()
        );

        assert_eq!(
            arr.fields()[1].as_primitive().typed_data::<i32>(),
            read_arr.as_struct().fields()[1]
                .as_primitive()
                .typed_data::<i32>()
        );
    }
}
