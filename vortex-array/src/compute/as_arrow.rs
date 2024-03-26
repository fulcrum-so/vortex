use arrow_array::ArrayRef as ArrowArrayRef;
use itertools::Itertools;

use vortex_error::{VortexError, VortexResult};

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::Array;
use crate::compute::flatten::flatten;
use crate::compute::ArrayCompute;

pub trait AsArrowArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef>;
}

pub fn as_arrow(array: &dyn Array) -> VortexResult<ArrowArrayRef> {
    // If as_arrow is implemented, then invoke that.
    if let Some(a) = array.as_arrow() {
        return a.as_arrow();
    }

    // Otherwise, flatten and try again.
    let array = flatten(array)?.into_array();
    array.as_arrow().map(|a| a.as_arrow()).unwrap_or_else(|| {
        Err(VortexError::NotImplemented(
            "as_arrow",
            array.encoding().id().name(),
        ))
    })
}

// TODO(ngates): return a RecordBatchReader instead?
pub fn as_arrow_chunks(array: &dyn Array) -> VortexResult<Vec<ArrowArrayRef>> {
    if let Some(chunked) = array.maybe_chunked() {
        chunked
            .chunks()
            .iter()
            .map(|a| as_arrow(a.as_ref()))
            .try_collect()
    } else {
        as_arrow(array).map(|a| vec![a])
    }
}
