use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::flatten::{FlattenFn, FlattenedArray};

impl FlattenFn for PrimitiveArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Primitive(self.clone()))
    }
}
