use itertools::Itertools;

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::struct_::StructArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{flatten, FlattenStructFn};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::{Scalar, StructScalar};

impl ArrayCompute for StructArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn flatten_struct(&self) -> Option<&dyn FlattenStructFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl AsContiguousFn for StructArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        let mut fields = vec![Vec::new(); self.fields().len()];
        for array in arrays {
            for f in 0..self.fields().len() {
                fields[f].push(array.as_struct().fields()[f].clone())
            }
        }

        Ok(StructArray::new(
            self.names().clone(),
            fields
                .iter()
                .map(|field_arrays| as_contiguous(field_arrays.clone()))
                .try_collect()?,
        )
        .boxed())
    }
}

impl FlattenStructFn for StructArray {
    fn flatten_struct(&self) -> VortexResult<StructArray> {
        Ok(StructArray::new(
            self.names().clone(),
            self.fields()
                .iter()
                .map(|field| flatten(field.as_ref()).map(|a| a.as_array()))
                .try_collect()?,
        ))
    }
}

impl ScalarAtFn for StructArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(StructScalar::new(
            self.dtype.clone(),
            self.fields
                .iter()
                .map(|field| scalar_at(field.as_ref(), index))
                .try_collect()?,
        )
        .into())
    }
}
