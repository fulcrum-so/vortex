use vortex::array::primitive::{PrimitiveArray, PrimitiveTrait, TypedPrimitiveTrait};
use vortex::array::{Array, ArrayRef};
use vortex::compute::flatten::{flatten, flatten_primitive, FlattenFn, FlattenedArray};
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::match_each_integer_ptype;
use vortex::scalar::Scalar;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::compress::ree_decode;
use crate::REEArray;

impl ArrayCompute for REEArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl FlattenFn for REEArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        let ends = flatten(self.ends())?;
        let FlattenedArray::Primitive(pends) = ends else {
            vortex_bail!("REE Ends array didn't flatten to primitive",);
        };

        let values = flatten(self.values())?;
        if let FlattenedArray::Primitive(pvalues) = values {
            ree_decode(&pends, &pvalues, self.validity(), self.offset(), self.len())
                .map(FlattenedArray::Primitive)
        } else {
            Err(vortex_err!("Cannot yet flatten non-primitive REE array",))
        }
    }
}

impl ScalarAtFn for REEArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        scalar_at(self.values(), self.find_physical_index(index)?)
    }
}

impl TakeFn for REEArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let primitive_indices = flatten_primitive(indices)?;
        let physical_indices = match_each_integer_ptype!(primitive_indices.ptype(), |$P| {
            primitive_indices
                .typed_data::<$P>()
                .iter()
                .map(|idx| {
                    self.find_physical_index(*idx as usize)
                        .map(|loc| loc as u64)
                })
                .collect::<VortexResult<Vec<_>>>()?
        });
        take(self.values(), &PrimitiveArray::from(physical_indices))
    }
}

#[cfg(test)]
mod test {
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::{PrimitiveArray, TypedPrimitiveTrait};
    use vortex::compute::take::take;

    use crate::REEArray;

    #[test]
    fn ree_take() {
        let ree = REEArray::encode(&PrimitiveArray::from(vec![
            1, 1, 1, 4, 4, 4, 2, 2, 5, 5, 5, 5,
        ]))
        .unwrap();
        let taken = take(&ree, &PrimitiveArray::from(vec![9, 8, 1, 3])).unwrap();
        assert_eq!(taken.as_primitive().typed_data::<i32>(), &[5, 5, 1, 4]);
    }
}
