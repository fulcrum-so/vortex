use num_traits::Zero;

use crate::array::primitive::PrimitiveArray;
use crate::array::IntoArray;
use crate::array::{Array, ArrayRef};
use crate::compute::fill::FillForwardFn;
use crate::compute::flatten::flatten_bool;
use crate::error::VortexResult;
use crate::match_each_native_ptype;
use crate::stats::Stat;

impl FillForwardFn for PrimitiveArray {
    fn fill_forward(&self) -> VortexResult<ArrayRef> {
        if self.validity().is_none() {
            Ok(self.clone().into_array())
        } else if self
            .stats()
            .get_or_compute_as::<usize>(&Stat::NullCount)
            .unwrap()
            == 0usize
        {
            return Ok(PrimitiveArray::new(self.ptype(), self.buffer().clone(), None).into_array());
        } else {
            match_each_native_ptype!(self.ptype(), |$P| {
                let validity = flatten_bool(self.validity().unwrap())?;
                let typed_data = self.typed_data::<$P>();
                let mut last_value = $P::zero();
                let filled = typed_data
                    .iter()
                    .zip(validity.buffer().iter())
                    .map(|(v, valid)| {
                        if valid {
                            last_value = *v;
                        }
                        last_value
                    })
                    .collect::<Vec<_>>();
                Ok(filled.into_array())
            })
        }
    }
}

#[cfg(test)]
mod test {
    use crate::array::bool::BoolArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::Array;
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
            Some(BoolArray::from(vec![true, true, true, true, true]).into_array()),
        );
        let filled = compute::fill::fill_forward(&arr).unwrap();
        let filled_primitive = filled.as_primitive();
        assert_eq!(filled_primitive.typed_data::<u8>(), vec![8, 10, 12, 14, 16]);
        assert!(filled_primitive.validity().is_none());
    }
}
