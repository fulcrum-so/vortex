use crate::r#for::compress::decompress;
use crate::FoRArray;
use vortex::compute::flatten::{FlattenFn, FlattenedArray};
use vortex::compute::ArrayCompute;
use vortex_error::VortexResult;

impl ArrayCompute for FoRArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }
}

impl FlattenFn for FoRArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        decompress(self).map(FlattenedArray::Primitive)
    }
}
