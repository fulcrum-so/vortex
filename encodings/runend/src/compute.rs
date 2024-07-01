use vortex::array::primitive::PrimitiveArray;
use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::unary::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::ArrayCompute;
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_dtype::match_each_integer_ptype;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::RunEndArray;

impl ArrayCompute for RunEndArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for RunEndArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        scalar_at(&self.values(), self.find_physical_index(index)?)
    }
}

impl TakeFn for RunEndArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let primitive_indices = indices.clone().into_primitive()?;
        let physical_indices = match_each_integer_ptype!(primitive_indices.ptype(), |$P| {
            primitive_indices
                .maybe_null_slice::<$P>()
                .iter()
                .map(|idx| {
                    self.find_physical_index(*idx as usize)
                        .map(|loc| loc as u64)
                })
                .collect::<VortexResult<Vec<_>>>()?
        });
        take(
            &self.values(),
            &PrimitiveArray::from(physical_indices).into_array(),
        )
    }
}

impl SliceFn for RunEndArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let slice_begin = self.find_physical_index(start)?;
        let slice_end = self.find_physical_index(stop)?;
        Ok(Self::with_offset_and_size(
            slice(&self.ends(), slice_begin, slice_end + 1)?,
            slice(&self.values(), slice_begin, slice_end + 1)?,
            self.validity().slice(slice_begin, slice_end + 1)?,
            stop - start,
            start,
        )?
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::take::take;
    use vortex::{IntoCanonical, ToArray};

    use crate::RunEndArray;

    #[test]
    fn ree_take() {
        let ree = RunEndArray::encode(
            PrimitiveArray::from(vec![1, 1, 1, 4, 4, 4, 2, 2, 5, 5, 5, 5]).to_array(),
        )
        .unwrap();
        let taken = take(ree.array(), PrimitiveArray::from(vec![9, 8, 1, 3]).array()).unwrap();
        assert_eq!(
            taken
                .into_canonical()
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &[5, 5, 1, 4]
        );
    }
}
