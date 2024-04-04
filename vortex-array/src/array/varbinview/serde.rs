use vortex_error::VortexResult;

use crate::array::varbinview::{VarBinViewArray, VarBinViewEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for VarBinViewArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_validity(self.validity())?;
        ctx.write(self.views())?;
        ctx.write_usize(self.data().len())?;
        for d in self.data() {
            ctx.write(d.as_ref())?;
        }
        Ok(())
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        Ok(None)
    }
}

impl EncodingSerde for VarBinViewEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let validity = ctx.read_validity()?;
        let views = ctx.bytes().read()?;
        let num_data = ctx.read_usize()?;
        let mut data_bufs = Vec::<ArrayRef>::with_capacity(num_data);
        for _ in 0..num_data {
            data_bufs.push(ctx.bytes().read()?);
        }
        Ok(VarBinViewArray::new(views, data_bufs, ctx.schema().clone(), validity).into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex_schema::{DType, Nullability};

    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::{PrimitiveArray, TypedPrimitiveTrait};
    use crate::array::varbinview::{BinaryView, Inlined, Ref, VarBinViewArray};
    use crate::array::Array;
    use crate::serde::test::roundtrip_array;

    fn binary_array() -> VarBinViewArray {
        let values = PrimitiveArray::from("hello world this is a long string".as_bytes().to_vec());
        let view1 = BinaryView {
            inlined: Inlined::new("hello world"),
        };
        let view2 = BinaryView {
            _ref: Ref {
                size: 33,
                prefix: "hell".as_bytes().try_into().unwrap(),
                buffer_index: 0,
                offset: 0,
            },
        };
        let view_arr = PrimitiveArray::from(
            vec![view1.to_le_bytes(), view2.to_le_bytes()]
                .into_iter()
                .flatten()
                .collect::<Vec<u8>>(),
        );

        VarBinViewArray::new(
            view_arr.into_array(),
            vec![values.into_array()],
            DType::Utf8(Nullability::NonNullable),
            None,
        )
    }

    #[test]
    fn roundtrip() {
        let arr = binary_array();
        let read_arr = roundtrip_array(&arr).unwrap();

        assert_eq!(
            arr.views().as_primitive().typed_data::<u8>(),
            read_arr
                .as_varbinview()
                .views()
                .as_primitive()
                .typed_data::<u8>()
        );

        assert_eq!(
            arr.data()[0].as_primitive().typed_data::<u8>(),
            read_arr.as_varbinview().data()[0]
                .as_primitive()
                .typed_data::<u8>()
        );
    }
}
