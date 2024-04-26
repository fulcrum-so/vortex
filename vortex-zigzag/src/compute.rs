use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::ArrayCompute;
use vortex::scalar::{PScalar, Scalar};
use vortex::{ArrayDType, IntoArray, OwnedArray};
use vortex_error::{vortex_err, VortexResult};
use zigzag::ZigZag as ExternalZigZag;

use crate::ZigZagArray;

impl ArrayCompute for ZigZagArray<'_> {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }
}

impl ScalarAtFn for ZigZagArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let scalar = scalar_at(&self.encoded(), index)?;
        match scalar {
            Scalar::Primitive(p) => match p.value() {
                None => Ok(Scalar::null(self.dtype())),
                Some(p) => match p {
                    PScalar::U8(u) => Ok(i8::decode(u).into()),
                    PScalar::U16(u) => Ok(i16::decode(u).into()),
                    PScalar::U32(u) => Ok(i32::decode(u).into()),
                    PScalar::U64(u) => Ok(i64::decode(u).into()),
                    _ => Err(vortex_err!(MismatchedTypes: "unsigned int", self.dtype())),
                },
            },
            _ => Err(vortex_err!(MismatchedTypes: "primitive scalar", self.dtype())),
        }
    }
}

impl SliceFn for ZigZagArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        Ok(ZigZagArray::try_new(slice(&self.encoded(), start, stop)?)?.into_array())
    }
}
