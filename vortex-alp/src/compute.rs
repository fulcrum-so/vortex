use crate::alp::ALPFloat;
use crate::ALPArray;
use std::f32;
use vortex::array::Array;
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::ArrayCompute;
use vortex::dtype::{DType, FloatWidth};
use vortex::error::{VortexError, VortexResult};
use vortex::scalar::Scalar;

impl ArrayCompute for ALPArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for ALPArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if let Some(patch) = self.patches().and_then(|p| scalar_at(p, index).ok()) {
            return Ok(patch);
        }

        let encoded_val = scalar_at(self.encoded(), index)?;

        match self.dtype() {
            DType::Float(FloatWidth::_32, _) => {
                let encoded_val: i32 = encoded_val.try_into().unwrap();
                Ok(Scalar::from(<f32 as ALPFloat>::decode_single(
                    encoded_val,
                    self.exponents(),
                )))
            }
            DType::Float(FloatWidth::_64, _) => {
                let encoded_val: i64 = encoded_val.try_into().unwrap();
                Ok(Scalar::from(<f64 as ALPFloat>::decode_single(
                    encoded_val,
                    self.exponents(),
                )))
            }
            _ => Err(VortexError::InvalidDType(self.dtype().clone())),
        }
    }
}
