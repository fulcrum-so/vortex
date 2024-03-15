use crate::array::bool::BoolArray;
use crate::arrow::as_arrow::AsArrowArray;
use crate::compute::flatten::flatten_bool;
use crate::error::VortexResult;
use arrow_array::{ArrayRef as ArrowArrayRef, BooleanArray as ArrowBoolArray};
use arrow_buffer::NullBuffer;
use std::sync::Arc;

impl AsArrowArray for BoolArray {
    fn as_arrow_array(&self) -> VortexResult<ArrowArrayRef> {
        let validity = self
            .validity()
            .map(|v| flatten_bool(v))
            .transpose()?
            .map(|b| NullBuffer::new(b.buffer));
        Ok(Arc::new(ArrowBoolArray::new(
            self.buffer().clone(),
            validity,
        )))
    }
}
