use crate::array::primitive::PrimitiveArray;
use crate::array::CloneOptionalArray;
use crate::array::{Array, ArrayRef};
use crate::compute::cast::CastFn;
use crate::error::{VortexError, VortexResult};
use crate::match_each_native_ptype;
use crate::ptype::{NativePType, PType};
use vortex_schema::DType;

impl CastFn for PrimitiveArray {
    fn cast(&self, dtype: &DType) -> VortexResult<ArrayRef> {
        // TODO(ngates): check validity
        let ptype = PType::try_from(dtype)?;
        if ptype == self.ptype {
            Ok(self.clone().boxed())
        } else {
            match_each_native_ptype!(ptype, |$T| {
                Ok(PrimitiveArray::from_nullable(
                    cast::<$T>(self)?,
                    self.validity().clone_optional(),
                ).boxed())
            })
        }
    }
}

fn cast<T: NativePType>(array: &PrimitiveArray) -> VortexResult<Vec<T>> {
    match_each_native_ptype!(array.ptype(), |$E| {
        array
            .typed_data::<$E>()
            .iter()
            // TODO(ngates): allow configurable checked/unchecked casting
            .map(|&v| {
                T::from(v).ok_or_else(|| {
                    VortexError::ComputeError(format!("Failed to cast {} to {:?}", v, T::PTYPE).into())
                })
            })
            .collect()
    })
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::compute;
    use crate::error::VortexError;
    use crate::ptype::PType;

    #[test]
    fn cast_u32_u8() {
        let arr = PrimitiveArray::from(vec![0u32, 10, 200]);
        let u8arr = compute::cast::cast(&arr, &PType::U8.into()).unwrap();
        assert_eq!(u8arr.as_primitive().typed_data::<u8>(), vec![0u8, 10, 200]);
    }

    #[test]
    fn cast_u32_f32() {
        let arr = PrimitiveArray::from(vec![0u32, 10, 200]);
        let u8arr = compute::cast::cast(&arr, &PType::F32.into()).unwrap();
        assert_eq!(
            u8arr.as_primitive().typed_data::<f32>(),
            vec![0.0f32, 10., 200.]
        );
    }

    #[test]
    fn cast_i32_u32() {
        let arr = PrimitiveArray::from(vec![-1i32]);
        assert_eq!(
            compute::cast::cast(&arr, &PType::U32.into()).err().unwrap(),
            VortexError::ComputeError("Failed to cast -1 to U32".into())
        )
    }
}
