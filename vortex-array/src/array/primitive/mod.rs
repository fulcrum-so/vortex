use arrow_buffer::{ArrowNativeType, ScalarBuffer};
use itertools::Itertools;
use num_traits::ops::overflowing::OverflowingSub;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use vortex_dtype::{
    match_each_float_ptype, match_each_integer_ptype, match_each_native_ptype, NativePType, PType,
};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_scalar::{PScalarType, PrimitiveScalar};

use crate::array::constant::ConstantArray;
use crate::buffer::Buffer;
use crate::compute::scalar_subtract::SubtractScalarFn;
use crate::stats::ArrayStatistics;
use crate::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::ArrayFlatten;
use crate::{impl_encoding, ArrayDType, OwnedArray, ToStatic};

mod accessor;
mod compute;
mod stats;

impl_encoding!("vortex.primitive", Primitive);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrimitiveMetadata {
    validity: ValidityMetadata,
}

impl PrimitiveArray<'_> {
    // TODO(ngates): remove the Arrow types from this API.
    pub fn try_new<T: NativePType + ArrowNativeType>(
        buffer: ScalarBuffer<T>,
        validity: Validity,
    ) -> VortexResult<Self> {
        Ok(Self {
            typed: TypedArray::try_from_parts(
                DType::from(T::PTYPE).with_nullability(validity.nullability()),
                PrimitiveMetadata {
                    validity: validity.to_metadata(buffer.len())?,
                },
                Some(Buffer::Owned(buffer.into_inner())),
                validity.into_array_data().into_iter().collect_vec().into(),
                HashMap::default(),
            )?,
        })
    }

    pub fn from_vec<T: NativePType>(values: Vec<T>, validity: Validity) -> Self {
        match_each_native_ptype!(T::PTYPE, |$P| {
            Self::try_new(ScalarBuffer::<$P>::from(
                unsafe { std::mem::transmute::<Vec<T>, Vec<$P>>(values) }
            ), validity).unwrap()
        })
    }

    pub fn from_nullable_vec<T: NativePType>(values: Vec<Option<T>>) -> Self {
        let elems: Vec<T> = values.iter().map(|v| v.unwrap_or_default()).collect();
        let validity = Validity::from(values.iter().map(|v| v.is_some()).collect::<Vec<_>>());
        Self::from_vec(elems, validity)
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(0, &Validity::DTYPE))
    }

    pub fn ptype(&self) -> PType {
        // TODO(ngates): we can't really cache this anywhere?
        self.dtype().try_into().unwrap()
    }

    pub fn buffer(&self) -> &Buffer {
        self.array().buffer().expect("missing buffer")
    }

    // TODO(ngates): deprecated, remove this.
    pub fn scalar_buffer<T: NativePType + ArrowNativeType>(&self) -> ScalarBuffer<T> {
        assert_eq!(
            T::PTYPE,
            self.ptype(),
            "Attempted to get scalar buffer of type {} from array of type {}",
            T::PTYPE,
            self.ptype(),
        );
        ScalarBuffer::new(self.buffer().clone().into(), 0, self.len())
    }

    pub fn typed_data<T: NativePType>(&self) -> &[T] {
        assert_eq!(
            T::PTYPE,
            self.ptype(),
            "Attempted to get typed_data of type {} from array of type {}",
            T::PTYPE,
            self.ptype(),
        );
        self.buffer().typed_data::<T>()
    }

    pub fn reinterpret_cast(&self, ptype: PType) -> Self {
        if self.ptype() == ptype {
            return self.clone();
        }

        assert_eq!(
            self.ptype().byte_width(),
            ptype.byte_width(),
            "can't reinterpret cast between integers of two different widths"
        );

        match_each_native_ptype!(ptype, |$P| {
            PrimitiveArray::try_new(
                ScalarBuffer::<$P>::new(self.buffer().clone().into(), 0, self.len()),
                self.validity(),
            )
            .unwrap()
        })
    }

    pub fn patch<P: AsPrimitive<usize>, T: NativePType>(
        self,
        positions: &[P],
        values: &[T],
    ) -> VortexResult<Self> {
        if self.ptype() != T::PTYPE {
            vortex_bail!(MismatchedTypes: self.dtype(), T::PTYPE)
        }

        let validity = self.validity().to_static();

        let mut own_values = self
            .into_buffer()
            .into_vec::<T>()
            .unwrap_or_else(|b| Vec::from(b.typed_data::<T>()));
        // TODO(robert): Also patch validity
        for (idx, value) in positions.iter().zip_eq(values.iter()) {
            own_values[(*idx).as_()] = *value;
        }
        Ok(Self::from_vec(own_values, validity))
    }
}

impl<'a> PrimitiveArray<'a> {
    pub fn into_buffer(self) -> Buffer<'a> {
        self.into_array().into_buffer().unwrap()
    }
}

impl<T: NativePType> From<Vec<T>> for PrimitiveArray<'_> {
    fn from(values: Vec<T>) -> Self {
        PrimitiveArray::from_vec(values, Validity::NonNullable)
    }
}

impl<T: NativePType> IntoArray<'static> for Vec<T> {
    fn into_array(self) -> OwnedArray {
        PrimitiveArray::from(self).into_array()
    }
}

impl ArrayFlatten for PrimitiveArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        Ok(Flattened::Primitive(self))
    }
}

impl ArrayTrait for PrimitiveArray<'_> {
    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }
}

impl ArrayValidity for PrimitiveArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for PrimitiveArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer())?;
        visitor.visit_validity(&self.validity())
    }
}

impl<'a> Array<'a> {
    pub fn into_primitive(self) -> PrimitiveArray<'a> {
        PrimitiveArray::try_from(self).expect("expected primitive array")
    }

    pub fn as_primitive(&self) -> PrimitiveArray {
        PrimitiveArray::try_from(self).expect("expected primitive array")
    }
}

impl EncodingCompression for PrimitiveEncoding {}

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
                subtract_scalar_integer::<$T>(self, to_subtract.clone())?
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

fn subtract_scalar_integer<'a, T: NativePType + OverflowingSub + PScalarType + TryFrom<Scalar>>(
    subtract_from: &PrimitiveArray<'a>,
    to_subtract: PrimitiveScalar,
) -> VortexResult<PrimitiveArray<'a>> {
    let to_subtract: T = to_subtract
        .typed_value()
        .ok_or_else(|| vortex_err!("expected primitive"))?;

    if to_subtract.is_zero() {
        // if to_subtract is zero, skip operation
        return Ok(subtract_from.clone());
    } else {
        if let Some(min) = subtract_from.statistics().compute_as_cast(Stat::Min) {
            let min: T = min;
            if let (_, true) = min.overflowing_sub(&to_subtract) {
                vortex_bail!(
                    "Integer subtraction over/underflow: {}, {}",
                    min,
                    to_subtract
                )
            }
        }
        if let Some(max) = subtract_from.statistics().compute_as_cast(Stat::Max) {
            let max: T = max;
            if let (_, true) = max.overflowing_sub(&to_subtract) {
                vortex_bail!(
                    "Integer subtraction over/underflow: {}, {}",
                    max,
                    to_subtract
                )
            }
        }
    }

    let contains_nulls = !subtract_from.logical_validity().all_valid();
    let subtraction_result = if contains_nulls {
        let validity = subtract_from
            .logical_validity()
            .to_null_buffer()?
            .expect("should_wrap only true if there are nulls");
        let sub_vec = subtract_from
            .typed_data()
            .iter()
            .zip(validity.iter())
            .map(|(&v, is_valid): (&T, bool)| {
                if is_valid {
                    Some(v - to_subtract)
                } else {
                    None
                }
            })
            .collect_vec();
        PrimitiveArray::from_nullable_vec(sub_vec)
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
