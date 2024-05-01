use itertools::Itertools;
use num_traits::ops::overflowing::OverflowingSub;
use num_traits::SaturatingSub;
use vortex_dtype::{match_each_float_ptype, match_each_integer_ptype, NativePType};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};
use vortex_scalar::{PScalarType, Scalar};

use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::compute::scalar_subtract::SubtractScalarFn;
use crate::stats::{ArrayStatistics, Stat};
use crate::validity::ArrayValidity;
use crate::{ArrayDType, ArrayTrait, IntoArray, OwnedArray, ToStatic};

impl SubtractScalarFn for PrimitiveArray<'_> {
    fn subtract_scalar(&self, to_subtract: &Scalar) -> VortexResult<OwnedArray> {
        if self.dtype() != to_subtract.dtype() {
            vortex_bail!(MismatchedTypes: self.dtype(), to_subtract.dtype())
        }

        let validity = self.validity().to_logical(self.len());
        if validity.all_invalid() {
            return Ok(ConstantArray::new(Scalar::null(self.dtype()), self.len()).into_array());
        }

        let to_subtract = match to_subtract {
            Scalar::Primitive(prim_scalar) => prim_scalar,
            _ => vortex_bail!("Expected primitive scalar"),
        };

        let result = if to_subtract.dtype().is_int() {
            match_each_integer_ptype!(self.ptype(), |$T| {
                let to_subtract: $T = to_subtract
                    .typed_value()
                    .ok_or_else(|| vortex_err!("expected primitive"))?;
                subtract_scalar_integer::<$T>(self, to_subtract)?
            })
        } else {
            match_each_float_ptype!(self.ptype(), |$T| {
                let to_subtract: $T = to_subtract.typed_value()
                    .ok_or_else(|| vortex_err!("expected primitive"))?;
                let sub_vec : Vec<$T> = self.typed_data::<$T>()
                .iter()
                .map(|&v| v - to_subtract).collect_vec();
                PrimitiveArray::from(sub_vec)
            })
        };
        Ok(result.into_array().to_static())
    }
}

fn subtract_scalar_integer<
    'a,
    T: NativePType
        + OverflowingSub
        + SaturatingSub
        + PScalarType
        + TryFrom<Scalar, Error = VortexError>,
>(
    subtract_from: &PrimitiveArray<'a>,
    to_subtract: T,
) -> VortexResult<PrimitiveArray<'a>> {
    if to_subtract.is_zero() {
        // if to_subtract is zero, skip operation
        return Ok(subtract_from.clone());
    }

    if let Ok(min) = subtract_from.statistics().compute_as_cast::<T>(Stat::Min) {
        if let (_, true) = min.overflowing_sub(&to_subtract) {
            vortex_bail!(
                "Integer subtraction over/underflow: {}, {}",
                min,
                to_subtract
            )
        }
    }
    if let Ok(max) = subtract_from.statistics().compute_as_cast::<T>(Stat::Max) {
        if let (_, true) = max.overflowing_sub(&to_subtract) {
            vortex_bail!(
                "Integer subtraction over/underflow: {}, {}",
                max,
                to_subtract
            )
        }
    }

    let contains_nulls = !subtract_from.logical_validity().all_valid();
    let subtraction_result = if contains_nulls {
        let sub_vec = subtract_from
            .typed_data()
            .iter()
            .map(|&v: &T| v.saturating_sub(&to_subtract))
            .collect_vec();
        PrimitiveArray::from_vec(sub_vec, subtract_from.validity())
    } else {
        PrimitiveArray::from(
            subtract_from
                .typed_data::<T>()
                .iter()
                .map(|&v| v - to_subtract)
                .collect_vec(),
        )
    };
    Ok(subtraction_result)
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use vortex_scalar::Scalar;

    use crate::array::primitive::PrimitiveArray;
    use crate::compute::scalar_subtract::subtract_scalar;
    use crate::{ArrayTrait, IntoArray};

    #[test]
    fn test_scalar_subtract_unsigned() {
        let values = vec![1u16, 2, 3].into_array();
        let results = subtract_scalar(&values, &1u16.into())
            .unwrap()
            .flatten_primitive()
            .unwrap()
            .typed_data::<u16>()
            .to_vec();
        assert_eq!(results, &[0u16, 1, 2]);
    }

    #[test]
    fn test_scalar_subtract_signed() {
        let values = vec![1i64, 2, 3].into_array();
        let results = subtract_scalar(&values, &(-1i64).into())
            .unwrap()
            .flatten_primitive()
            .unwrap()
            .typed_data::<i64>()
            .to_vec();
        assert_eq!(results, &[2i64, 3, 4]);
    }

    #[test]
    fn test_scalar_subtract_nullable() {
        let values = PrimitiveArray::from_nullable_vec(vec![Some(1u16), Some(2), None, Some(3)])
            .into_array();
        let flattened = subtract_scalar(&values, &Some(1u16).into())
            .unwrap()
            .flatten_primitive()
            .unwrap();

        let results = flattened.typed_data::<u16>().to_vec();
        assert_eq!(results, &[0u16, 1, 0, 2]);
        let valid_indices = flattened
            .validity()
            .to_logical(flattened.len())
            .to_null_buffer()
            .unwrap()
            .unwrap()
            .valid_indices()
            .collect_vec();
        assert_eq!(valid_indices, &[0, 1, 3]);
    }

    #[test]
    fn test_scalar_subtract_float() {
        let values = vec![1.0f64, 2.0, 3.0].into_array();
        let to_subtract = -1f64;
        let results = subtract_scalar(&values, &to_subtract.into())
            .unwrap()
            .flatten_primitive()
            .unwrap()
            .typed_data::<f64>()
            .to_vec();
        assert_eq!(results, &[2.0f64, 3.0, 4.0]);
    }

    #[test]
    fn test_scalar_subtract_unsigned_underflow() {
        let values = vec![u8::MIN, 2, 3].into_array();
        let _results =
            subtract_scalar(&values, &1u8.into()).expect_err("should fail with underflow");
        let values = vec![u16::MIN, 2, 3].into_array();
        let _results =
            subtract_scalar(&values, &1u16.into()).expect_err("should fail with underflow");
        let values = vec![u32::MIN, 2, 3].into_array();
        let _results =
            subtract_scalar(&values, &1u32.into()).expect_err("should fail with underflow");
        let values = vec![u64::MIN, 2, 3].into_array();
        let _results =
            subtract_scalar(&values, &1u64.into()).expect_err("should fail with underflow");
    }

    #[test]
    fn test_scalar_subtract_signed_overflow() {
        let values = vec![i8::MAX, 2, 3].into_array();
        let to_subtract: Scalar = (-1i8).into();
        let _results =
            subtract_scalar(&values, &to_subtract).expect_err("should fail with overflow");
        let values = vec![i16::MAX, 2, 3].into_array();
        let _results =
            subtract_scalar(&values, &to_subtract).expect_err("should fail with overflow");
        let values = vec![i32::MAX, 2, 3].into_array();
        let _results =
            subtract_scalar(&values, &to_subtract).expect_err("should fail with overflow");
        let values = vec![i64::MAX, 2, 3].into_array();
        let _results =
            subtract_scalar(&values, &to_subtract).expect_err("should fail with overflow");
    }

    #[test]
    fn test_scalar_subtract_signed_underflow() {
        let values = vec![i8::MIN, 2, 3].into_array();
        let _results =
            subtract_scalar(&values, &1i8.into()).expect_err("should fail with underflow");
        let values = vec![i16::MIN, 2, 3].into_array();
        let _results =
            subtract_scalar(&values, &1i16.into()).expect_err("should fail with underflow");
        let values = vec![i32::MIN, 2, 3].into_array();
        let _results =
            subtract_scalar(&values, &1i32.into()).expect_err("should fail with underflow");
        let values = vec![i64::MIN, 2, 3].into_array();
        let _results =
            subtract_scalar(&values, &1i64.into()).expect_err("should fail with underflow");
    }

    #[test]
    fn test_scalar_subtract_float_underflow_is_ok() {
        let values = vec![f32::MIN, 2.0, 3.0].into_array();
        let _results = subtract_scalar(&values, &1.0f32.into()).unwrap();
        let _results = subtract_scalar(&values, &f32::MAX.into()).unwrap();
    }

    #[test]
    fn test_scalar_subtract_type_mismatch_fails() {
        let values = vec![1u64, 2, 3].into_array();
        // Subtracting incompatible dtypes should fail
        let _results =
            subtract_scalar(&values, &1.5f64.into()).expect_err("Expected type mismatch error");
    }
}
