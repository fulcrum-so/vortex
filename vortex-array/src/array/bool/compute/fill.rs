use vortex_error::VortexResult;
use vortex_schema::Nullability;

use crate::array::bool::BoolArray;
use crate::compute::fill::FillForwardFn;
use crate::validity::ArrayValidity;
use crate::{ArrayDType, IntoArray, OwnedArray, ToArrayData};

impl FillForwardFn for BoolArray<'_> {
    fn fill_forward(&self) -> VortexResult<OwnedArray> {
        if self.dtype().nullability() == Nullability::NonNullable {
            return Ok(self.to_array_data().into_array());
        }

        let validity = self.logical_validity().to_null_buffer()?.unwrap();
        let bools = self.boolean_buffer();
        let mut last_value = false;
        let filled = bools
            .iter()
            .zip(validity.inner().iter())
            .map(|(v, valid)| {
                if valid {
                    last_value = v;
                }
                last_value
            })
            .collect::<Vec<_>>();
        Ok(BoolArray::from(filled).into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::bool::BoolArray;
    use crate::validity::Validity;
    use crate::{compute, IntoArray};

    #[test]
    fn fill_forward() {
        let barr =
            BoolArray::from_iter(vec![None, Some(false), None, Some(true), None]).into_array();
        let filled_bool = BoolArray::try_from(compute::fill::fill_forward(&barr).unwrap()).unwrap();
        assert_eq!(
            filled_bool.boolean_buffer().iter().collect::<Vec<bool>>(),
            vec![false, false, false, true, true]
        );
        assert_eq!(filled_bool.validity(), Validity::NonNullable);
    }
}
