use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::ArrayCompute;
use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::DictArray;

impl ArrayCompute for DictArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for DictArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let dict_index: usize = scalar_at(self.codes(), index)?.try_into()?;
        scalar_at(self.dict(), dict_index)
    }
}
