use std::sync::Arc;

use arrow_buffer::buffer::BooleanBuffer;

use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::fill::FillForwardFn;
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::scalar::{BoolScalar, Scalar};
use crate::validity::{ArrayValidity, Validity};

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
    fn as_contiguous(&self, arrays: &[ArrayRef]) -> VortexResult<ArrayRef> {
        let validity: Option<Validity> = if self.dtype().is_nullable() {
            Some(Validity::from_iter(arrays.iter().map(|a| {
                a.validity().unwrap_or_else(|| Validity::Valid(a.len()))
            })))
        } else {
            None
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
        Ok(BoolScalar::new(
            self.is_valid(index).then(|| self.buffer.value(index)),
            self.nullability(),
        )
        .unwrap()
        .into())
    }
}

impl FillForwardFn for BoolArray {
    fn fill_forward(&self) -> VortexResult<ArrayRef> {
        if self.validity().is_none() {
            return Ok(Arc::new(self.clone()));
        }

        let validity = self.validity().unwrap().to_bool_array();
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

#[cfg(test)]
mod test {
    use crate::array::bool::BoolArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::compute;
    use crate::validity::ArrayValidity;

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
