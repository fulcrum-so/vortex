use vortex_error::VortexResult;

use crate::array::primitive::{PrimitiveArray, PrimitiveEncoding, PrimitiveTrait};
use crate::array::{Array, ArrayRef};
use crate::compute::ArrayCompute;
use crate::match_each_native_ptype;
use crate::ptype::PType;
use crate::serde::{ArraySerde, ArrayView, EncodingSerde, ReadCtx, WriteCtx};
use crate::validity::ArrayValidity;

impl ArraySerde for PrimitiveArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.ptype(self.ptype())?;
        ctx.write_validity(self.validity())?;
        ctx.write_buffer(self.len(), self.buffer())
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        Ok(None)
    }
}

impl EncodingSerde for PrimitiveEncoding {
    fn view_compute<'view>(&self, view: &'view ArrayView) -> Option<&'view dyn ArrayCompute> {
        let ptype = PType::try_from(view.dtype()).unwrap();
        match_each_native_ptype!(ptype, |$T| {
            let primitive_view: &dyn PrimitiveTrait<$T> = view;
            Some(primitive_view.compute())
        })
    }

    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let ptype = ctx.ptype()?;
        let validity = ctx.read_validity()?;
        let (_, buf) = ctx.read_buffer(|len| len * ptype.byte_width())?;
        Ok(PrimitiveArray::new(ptype, buf, validity).into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::serde::test::roundtrip_array;
    use crate::validity::ArrayValidity;

    #[test]
    fn roundtrip() {
        let arr = PrimitiveArray::from_iter(vec![Some(0), None, Some(2), Some(42)]);
        let read_arr = roundtrip_array(&arr).unwrap();
        assert_eq!(
            arr.buffer().typed_data::<i32>(),
            read_arr.as_primitive().buffer().typed_data::<i32>()
        );
        assert_eq!(arr.validity(), read_arr.validity());
    }
}
