use arrow_buffer::buffer::BooleanBuffer;
use itertools::Itertools;
use std::sync::Arc;

use crate::array::bool::BoolArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::fill::FillForwardFn;
use crate::compute::flatten::{flatten_bool, FlattenFn, FlattenedArray};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::{BoolScalar, Scalar};

impl ArrayCompute for BoolArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl AsContiguousFn for BoolArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        // TODO(ngates): implement a HasValidity trait to avoid this duplicate code.
        let validity = if arrays.iter().all(|a| a.as_bool().validity().is_none()) {
            None
        } else {
            Some(as_contiguous(
                arrays
                    .iter()
                    .map(|a| {
                        a.as_bool()
                            .validity()
                            .cloned()
                            .unwrap_or_else(|| BoolArray::from(vec![true; a.len()]).into_array())
                    })
                    .collect_vec(),
            )?)
        };

        Ok(BoolArray::new(
            BooleanBuffer::from(
                arrays
                    .iter()
                    .flat_map(|a| a.as_bool().buffer().iter())
                    .collect::<Vec<bool>>(),
            ),
            validity,
        )
        .into_array())
    }
}

impl FlattenFn for BoolArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Bool(self.clone()))
    }
}

impl ScalarAtFn for BoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            Ok(self.buffer.value(index).into())
        } else {
            Ok(BoolScalar::new(None).into())
        }
    }
}

impl FillForwardFn for BoolArray {
    fn fill_forward(&self) -> VortexResult<ArrayRef> {
        if self.validity().is_none() {
            Ok(Arc::new(self.clone()))
        } else {
            let validity = flatten_bool(self.validity().unwrap())?;
            let bools = self.buffer();
            let mut last_value = false;
            let filled = bools
                .iter()
                .zip(validity.buffer().iter())
                .map(|(v, valid)| {
                    if valid {
                        last_value = v;
                    }
                    last_value
                })
                .collect::<Vec<_>>();
            Ok(BoolArray::from(filled).into_array())
        }
    }
}

#[cfg(test)]
mod test {
    use crate::array::bool::BoolArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::compute;

    #[test]
    fn fill_forward() {
        let barr = BoolArray::from_iter(vec![None, Some(false), None, Some(true), None]);
        let filled = compute::fill::fill_forward(&barr).unwrap();
        let filled_bool = filled.as_bool();
        assert_eq!(
            filled_bool.buffer().iter().collect::<Vec<bool>>(),
            vec![false, false, false, true, true]
        );
        assert!(filled_bool.validity().is_none());
    }
}
