use zigzag::ZigZag;

use vortex::array::Array;
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::ArrayCompute;
use vortex::scalar::{PScalar, Scalar};
use vortex_error::{VortexError, VortexResult};

use crate::ZigZagArray;

impl ArrayCompute for ZigZagArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for ZigZagArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let scalar = scalar_at(self.encoded(), index)?;
        match scalar {
            Scalar::Primitive(p) => match p.value() {
                None => Ok(Scalar::null(self.dtype())),
                Some(p) => match p {
                    PScalar::U8(u) => Ok(i8::decode(u).into()),
                    PScalar::U16(u) => Ok(i16::decode(u).into()),
                    PScalar::U32(u) => Ok(i32::decode(u).into()),
                    PScalar::U64(u) => Ok(i64::decode(u).into()),
                    _ => Err(VortexError::InvalidDType(self.dtype().clone())),
                },
            },
            _ => Err(VortexError::InvalidDType(self.dtype().clone())),
        }
    }
}
