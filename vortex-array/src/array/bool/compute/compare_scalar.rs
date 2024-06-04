use std::ops::BitAnd;

use arrow_buffer::BooleanBufferBuilder;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_expr::operators::Operator;
use vortex_scalar::Scalar;

use crate::array::bool::{apply_comparison_op, BoolArray};
use crate::compute::compare_scalar::CompareScalarFn;
use crate::{Array, ArrayTrait, IntoArray};

impl CompareScalarFn for BoolArray {
    fn compare_scalar(&self, op: Operator, scalar: &Scalar) -> VortexResult<Array> {
        if let DType::Bool(_) = scalar.dtype() {
        } else {
            vortex_bail!("Invalid dtype for boolean scalar comparison")
        }

        let lhs = self.boolean_buffer();
        let scalar_val = scalar
            .value()
            .as_bool()?
            .ok_or_else(|| vortex_err!("Invalid scalar for comparison"))?;

        let mut rhs = BooleanBufferBuilder::new(self.len());
        rhs.append_n(self.len(), scalar_val);
        let rhs = rhs.finish();
        let comparison_result = apply_comparison_op(lhs, rhs, op);

        let present = self.validity().to_logical(self.len()).to_null_buffer()?;
        let with_validity_applied = present
            .map(|p| comparison_result.bitand(&p.into_inner()))
            .unwrap_or(comparison_result);

        Ok(BoolArray::from(with_validity_applied).into_array())
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use crate::compute::compare_scalar::compare_scalar;
    use crate::validity::Validity;

    fn to_int_indices(indices_bits: BoolArray) -> Vec<u64> {
        let filtered = indices_bits
            .boolean_buffer()
            .iter()
            .enumerate()
            .flat_map(|(idx, v)| if v { Some(idx as u64) } else { None })
            .collect_vec();
        filtered
    }

    #[test]
    fn test_basic_comparisons() -> VortexResult<()> {
        let arr = BoolArray::from_vec(
            vec![true, true, false, true, false],
            Validity::Array(BoolArray::from(vec![false, true, true, true, false]).into_array()),
        )
        .into_array();

        let matches = compare_scalar(&arr, Operator::EqualTo, &false.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [2u64]);

        let matches = compare_scalar(&arr, Operator::NotEqualTo, &false.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [1u64, 3]);

        let matches = compare_scalar(&arr, Operator::GreaterThan, &false.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [1u64, 3]);

        let matches =
            compare_scalar(&arr, Operator::GreaterThanOrEqualTo, &false.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [1u64, 2, 3]);

        let matches = compare_scalar(&arr, Operator::LessThan, &false.into())?.flatten_bool()?;
        let empty: [u64; 0] = [];
        assert_eq!(to_int_indices(matches), empty);

        let matches =
            compare_scalar(&arr, Operator::LessThanOrEqualTo, &false.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [2u64]);
        Ok(())
    }
}
