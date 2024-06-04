use std::ops::BitAnd;

use arrow_buffer::BooleanBuffer;
use vortex_dtype::{match_each_native_ptype, NativePType};
use vortex_error::{vortex_err, VortexResult};
use vortex_expr::operators::Operator;

use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::compute::compare::CompareFn;
use crate::{Array, ArrayTrait, IntoArray};

impl CompareFn for PrimitiveArray {
    // @TODO(@jcasale) take stats into account here, which may allow us to elide some comparison
    // work based on sortedness/min/max/etc.
    fn compare(&self, other: &Array, predicate: Operator) -> VortexResult<Array> {
        let flattened = other
            .clone()
            .flatten_primitive()
            .map_err(|_| vortex_err!("Cannot compare primitive array with non-primitive array"))?;

        let matching_idxs = match_each_native_ptype!(self.ptype(), |$T| {
            let predicate_fn = &predicate.to_predicate::<$T>();
            apply_predicate(self.typed_data::<$T>(), flattened.typed_data::<$T>(), predicate_fn)
        });

        let present = self.validity().to_logical(self.len()).to_null_buffer()?;
        let with_validity_applied = present
            .map(|p| matching_idxs.bitand(&p.into_inner()))
            .unwrap_or(matching_idxs);

        let present_other = flattened
            .validity()
            .to_logical(self.len())
            .to_null_buffer()?;

        let with_other_validity_applied = present_other
            .map(|p| with_validity_applied.bitand(&p.into_inner()))
            .unwrap_or(with_validity_applied);

        Ok(BoolArray::from(with_other_validity_applied).into_array())
    }
}

fn apply_predicate<T: NativePType, F: Fn(&T, &T) -> bool>(
    lhs: &[T],
    rhs: &[T],
    f: F,
) -> BooleanBuffer {
    let matches = lhs.iter().zip(rhs.iter()).map(|(lhs, rhs)| f(lhs, rhs));
    BooleanBuffer::from_iter(matches)
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use crate::compute::compare::compare;

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

        let matches = compare(&arr, &arr, Operator::EqualTo)?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 5, 6, 7, 8, 10]);

        let matches = compare(&arr, &arr, Operator::NotEqualTo)?.flatten_bool()?;
        let empty: [u64; 0] = [];
        assert_eq!(to_int_indices(matches), empty);

        let other = PrimitiveArray::from_nullable_vec(vec![
            Some(1i32),
            Some(2),
            Some(3),
            Some(4),
            None,
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            None,
            Some(10),
            None,
        ])
        .into_array();

        let matches = compare(&arr, &other, Operator::LessThanOrEqualTo)?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 5, 6, 7, 8, 10]);

        let matches = compare(&arr, &other, Operator::LessThan)?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [5u64, 6, 7, 8, 10]);

        let matches = compare(&other, &arr, Operator::GreaterThanOrEqualTo)?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 5, 6, 7, 8, 10]);

        let matches = compare(&other, &arr, Operator::GreaterThan)?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [5u64, 6, 7, 8, 10]);
        Ok(())
    }
}
