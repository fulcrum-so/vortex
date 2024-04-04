use num_traits::Zero;

use vortex_error::VortexResult;

use crate::array::{Array, ArrayRef, IntoArray};
use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::primitive::compute::TypedPrimitiveTrait;
use crate::array::primitive::PrimitiveArray;
use crate::compute::fill::FillForwardFn;
use crate::match_each_native_ptype;

impl FillForwardFn for &dyn PrimitiveTrait {
    fn fill_forward(&self) -> VortexResult<ArrayRef> {
        if self.validity_view().is_none() {
            return Ok(self.to_array());
        }

        let validity = self.validity().unwrap();
        if validity.as_view().all_valid() {
            return Ok(PrimitiveArray::new(self.ptype(), self.buffer().clone(), None).into_array());
        }

        match_each_native_ptype!(self.ptype(), |$T| {
            let mut last_value = $T::zero();
            let filled = self
                .typed_data()
                .iter()
                .zip(validity.to_bool_array().into_buffer().iter())
                .map(|(v, valid)| {
                    if valid {
                        last_value = *v;
                    }
                    last_value
                })
                .collect::<Vec<$T>>();
            Ok(filled.into_array())
        })
    }
}

#[cfg(test)]
mod test {
    use crate::array::Array;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::{PrimitiveArray, TypedPrimitiveTrait};
    use crate::compute;

    #[test]
    fn leading_none() {
        let arr = PrimitiveArray::from_iter(vec![None, Some(8u8), None, Some(10), None]);
        let filled = compute::fill::fill_forward(&arr).unwrap();
        let filled_primitive = filled.as_primitive();
        assert_eq!(filled_primitive.typed_data::<u8>(), vec![0, 8, 8, 10, 10]);
        assert!(filled_primitive.validity().is_none());
    }

    #[test]
    fn all_none() {
        let arr = PrimitiveArray::from_iter(vec![Option::<u8>::None, None, None, None, None]);
        let filled = compute::fill::fill_forward(&arr).unwrap();
        let filled_primitive = filled.as_primitive();
        assert_eq!(filled_primitive.typed_data::<u8>(), vec![0, 0, 0, 0, 0]);
        assert!(filled_primitive.validity().is_none());
    }

    #[test]
    fn nullable_non_null() {
        let arr = PrimitiveArray::from_nullable(
            vec![8u8, 10u8, 12u8, 14u8, 16u8],
            Some(vec![true, true, true, true, true].into()),
        );
        let filled = compute::fill::fill_forward(&arr).unwrap();
        let filled_primitive = filled.as_primitive();
        assert_eq!(filled_primitive.typed_data::<u8>(), vec![8, 10, 12, 14, 16]);
        assert!(filled_primitive.validity().is_none());
    }
}
