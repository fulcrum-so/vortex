use std::ops::BitAnd;

use arrow_buffer::BooleanBuffer;
use vortex_dtype::{match_each_native_ptype, DType, NativePType};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_expr::operators::Operator;
use vortex_scalar::Scalar;

use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::compute::compare_scalar::CompareScalarFn;
use crate::{Array, ArrayTrait, IntoArray};

impl CompareScalarFn for PrimitiveArray {
    // @TODO(@jcasale) take stats into account here, which may allow us to elide some comparison
    // work based on sortedness/min/max/etc.
    fn compare_scalar(&self, op: Operator, scalar: &Scalar) -> VortexResult<Array> {
        if let DType::Primitive(..) = scalar.dtype() {
        } else {
            vortex_bail!("Invalid scalar dtype for boolean scalar comparison")
        }

        let p_val = scalar
            .value()
            .as_pvalue()?
            .ok_or_else(|| vortex_err!("Invalid scalar for comparison"))?;
        let matching_idxs = match_each_native_ptype!(self.ptype(), |$T| {
            let predicate_fn = &op.to_predicate::<$T>();
            let rhs = p_val.try_into()?;
            apply_predicate(self.typed_data::<$T>(), &rhs, predicate_fn)
        });

        let present = self.validity().to_logical(self.len()).to_null_buffer()?;
        let with_validity_applied = present
            .map(|p| matching_idxs.bitand(&p.into_inner()))
            .unwrap_or(matching_idxs);

        Ok(BoolArray::from(with_validity_applied).into_array())
    }
}

fn apply_predicate<T: NativePType, F: Fn(&T, &T) -> bool>(
    lhs: &[T],
    rhs: &T,
    f: F,
) -> BooleanBuffer {
    let matches = lhs.iter().map(|lhs| f(lhs, rhs));
    BooleanBuffer::from_iter(matches)
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use crate::compute::compare_scalar::compare_scalar;

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
        let arr = PrimitiveArray::from_nullable_vec(vec![
            Some(1i32),
            Some(2),
            Some(3),
            Some(4),
            None,
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            None,
            Some(9),
            None,
        ])
        .into_array();

        let matches = compare_scalar(&arr, Operator::EqualTo, &5.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [5u64]);

        let matches = compare_scalar(&arr, Operator::NotEqualTo, &5.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 6, 7, 8, 10]);

        let matches = compare_scalar(&arr, Operator::EqualTo, &11.into())?.flatten_bool()?;
        let empty: [u64; 0] = [];
        assert_eq!(to_int_indices(matches), empty);

        let matches = compare_scalar(&arr, Operator::LessThan, &8.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 5, 6, 7]);

        let matches =
            compare_scalar(&arr, Operator::LessThanOrEqualTo, &8.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 5, 6, 7, 8]);

        let matches = compare_scalar(&arr, Operator::GreaterThan, &8.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [10u64]);

        let matches =
            compare_scalar(&arr, Operator::GreaterThanOrEqualTo, &8.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [8u64, 10]);
        Ok(())
    }
}
