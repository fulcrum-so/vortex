use crate::array::primitive::PrimitiveArray;
use crate::array::{ArrayRef, CloneOptionalArray};
use crate::compute::cast::CastPrimitiveFn;
use crate::compute::fill::FillForwardFn;
use crate::error::VortexResult;
use crate::ptype::NativePType;

impl FillForwardFn for PrimitiveArray {
    fn fill_forward(&self) -> VortexResult<ArrayRef> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::compute;

    #[test]
    fn cast_u32_u8() {
        let arr = PrimitiveArray::from_iter(vec![None, Some(0u8), None, Some(10), None]);
        let filled = compute::fill::fill_forward(&arr).unwrap().as_primitive();
        assert_eq!(filled.typed_data::<u8>(), vec![0, 8, 8, 10, 10]);
        assert_eq!(filled.validity(), None);
    }
}
