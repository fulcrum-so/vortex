use vortex_error::VortexResult;

use crate::array::primitive::compute::PrimitiveTrait;
use crate::compute::flatten::{FlattenFn, FlattenedArray};

impl FlattenFn for &dyn PrimitiveTrait {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Primitive(self.to_primitive()))
    }
}
