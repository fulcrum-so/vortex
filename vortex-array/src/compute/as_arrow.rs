use arrow_array::ArrayRef as ArrowArrayRef;
use vortex_error::{vortex_err, VortexResult};

use crate::array::chunked::ChunkedArray;
use crate::{Array, IntoArray};

pub trait AsArrowArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef>;
}

pub fn as_arrow(array: &Array) -> VortexResult<ArrowArrayRef> {
    array.with_dyn(|a| {
        // If as_arrow is implemented, then invoke that.
        if let Some(a) = a.as_arrow() {
            return a.as_arrow();
        }

        // Otherwise, flatten and try again.
        let array = array.clone().flatten()?.into_array();
        a.as_arrow()
            .map(|a| a.as_arrow())
            .unwrap_or_else(|| Err(vortex_err!(NotImplemented: "as_arrow", array.encoding().id())))
    })
}

// TODO(ngates): return a RecordBatchReader instead?
pub fn as_arrow_chunks(array: &Array) -> VortexResult<Vec<ArrowArrayRef>> {
    if let Ok(chunked) = ChunkedArray::try_from(array) {
        chunked
            .chunks()
            .map(|a| as_arrow(&a))
            .collect::<VortexResult<Vec<_>>>()
    } else {
        as_arrow(array).map(|a| vec![a])
    }
}
