use crate::array::struct_::StructArray;
use crate::array::{Array, ArrayRef, IntoArray};
use crate::arrow::FromArrowType;
use crate::compute::cast::cast;
use crate::encode::FromArrowArray;
use arrow_array::RecordBatch;
use std::sync::Arc;
use vortex_schema::DType;

impl IntoArray for &RecordBatch {
    fn into_array(self) -> ArrayRef {
        StructArray::new(
            self.schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .map(|s| s.to_owned())
                .map(Arc::new)
                .collect(),
            self.columns()
                .iter()
                .zip(self.schema().fields())
                .map(|(array, field)| {
                    // The dtype of the child arrays infer their nullability from the array itself.
                    // In case the schema says something different, we cast into the schema's dtype.
                    let vortex_array = ArrayRef::from_arrow(array.clone(), field.is_nullable());
                    cast(&vortex_array, &DType::from_arrow(field.as_ref())).unwrap()
                })
                .collect(),
        )
        .into_array()
    }
}
