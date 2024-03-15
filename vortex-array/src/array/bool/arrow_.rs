// Implement conversion to/from Arrow.

use crate::array::bool::BoolArray;
use crate::array::{Array, ArrayRef};
use crate::arrow::TryIntoArrow;
use crate::compute::cast::cast_bool;
use crate::error::{VortexError, VortexResult};
use arrow_array::builder::BooleanBuilder;
use arrow_array::{ArrayRef as ArrowArrayRef, BooleanArray as ArrowBoolArray, BooleanArray};
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use std::sync::Arc;

impl TryIntoArrow for BoolArray {
    fn arrow_array(&self) -> VortexResult<ArrowArrayRef> {
        let validity = self
            .validity()
            .map(|validity| {
                // First we convert the validity buffer into a BoolArray.
                let validity = cast_bool(validity.as_ref())?;
                if validity.validity().is_some() {
                    return Err(VortexError::InvalidArgument(
                        "Validity buffer cannot have nested validity".into(),
                    ));
                }
                Ok(NullBuffer::new(validity.buffer().clone()))
            })
            .transpose()?;
        Ok(Arc::new(BooleanArray::new(self.buffer.clone(), validity)))
    }

    fn arrow_dtype(&self) -> VortexResult<DataType> {
        Ok(DataType::Boolean)
    }
}

impl From<ArrowBoolArray> for BoolArray {
    fn from(_value: ArrowBoolArray) -> Self {
        todo!()
    }
}

impl From<ArrowBoolArray> for ArrayRef {
    fn from(value: ArrowBoolArray) -> Self {
        BoolArray::from(value).boxed()
    }
}

impl TryFrom<BoolArray> for ArrowBoolArray {
    type Error = VortexError;

    fn try_from(_value: BoolArray) -> Result<Self, Self::Error> {
        Ok(BooleanBuilder::new().finish())
    }
}

impl TryFrom<BoolArray> for ArrowArrayRef {
    type Error = VortexError;

    fn try_from(value: BoolArray) -> Result<Self, Self::Error> {
        Ok(Arc::new(ArrowBoolArray::try_from(value)?))
    }
}
