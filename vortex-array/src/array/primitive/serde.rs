use vortex_error::VortexResult;

use crate::array::primitive::{PrimitiveArray, PrimitiveEncoding, PrimitiveView};
use crate::array::validity::ArrayValidity;
use crate::array::{Array, ArrayRef};
use crate::compute::ArrayCompute;
use crate::match_each_native_ptype;
use crate::serde::{ArraySerde, ArrayView, EncodingSerde, ReadCtx, WriteCtx};

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
    fn with_view_compute<'view>(
        &self,
        view: &'view ArrayView,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        let view = PrimitiveView::try_new(view)?;
        match_each_native_ptype!(view.ptype(), |$T| {
            f(&view.as_trait::<$T>())
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
    use crate::array::validity::ArrayValidity;
    use crate::serde::test::roundtrip_array;

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
