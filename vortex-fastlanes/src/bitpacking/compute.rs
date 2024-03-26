use crate::bitpacking::compress::bitunpack;
use crate::BitPackedArray;
use vortex::compute::flatten::{FlattenFn, FlattenedArray};
use vortex::compute::ArrayCompute;
use vortex_error::VortexResult;

impl ArrayCompute for BitPackedArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }
}

impl FlattenFn for BitPackedArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        bitunpack(self).map(FlattenedArray::Primitive)
    }
}
