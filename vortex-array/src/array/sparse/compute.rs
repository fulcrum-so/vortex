use arrow_buffer::BooleanBufferBuilder;
use itertools::Itertools;
use vortex_error::{vortex_err, VortexResult};

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::{PrimitiveArray, PrimitiveTrait, TypedPrimitiveTrait};
use crate::array::sparse::SparseArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{flatten, FlattenFn, FlattenedArray};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::search_sorted::{search_sorted, SearchSortedSide};
use crate::compute::ArrayCompute;
use crate::match_each_native_ptype;
use crate::scalar::Scalar;

impl ArrayCompute for SparseArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl AsContiguousFn for SparseArray {
    fn as_contiguous(&self, arrays: &[ArrayRef]) -> VortexResult<ArrayRef> {
        Ok(SparseArray::new(
            as_contiguous(
                &arrays
                    .iter()
                    .map(|a| a.as_sparse().indices())
                    .cloned()
                    .collect_vec(),
            )?,
            as_contiguous(
                &arrays
                    .iter()
                    .map(|a| a.as_sparse().values())
                    .cloned()
                    .collect_vec(),
            )?,
            arrays.iter().map(|a| a.len()).sum(),
        )
        .into_array())
    }
}

impl FlattenFn for SparseArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        // Resolve our indices into a vector of usize applying the offset
        let indices = self.resolved_indices();

        let mut validity = BooleanBufferBuilder::new(self.len());
        validity.append_n(self.len(), false);

        let values = flatten(self.values())?;
        if let FlattenedArray::Primitive(parray) = values {
            match_each_native_ptype!(parray.ptype(), |$P| {
                let mut values = vec![$P::default(); self.len()];
                let mut offset = 0;

                for v in parray.typed_data::<$P>() {
                    let idx = indices[offset];
                    values[idx] = *v;
                    validity.set_bit(idx, true);
                    offset += 1;
                }

                let validity = validity.finish();

                Ok(FlattenedArray::Primitive(PrimitiveArray::from_nullable(
                    values,
                    Some(validity.into()),
                )))
            })
        } else {
            Err(vortex_err!(
                "Cannot flatten SparseArray with non-primitive values"
            ))
        }
    }
}

impl ScalarAtFn for SparseArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        // Check whether `true_patch_index` exists in the patch index array
        // First, get the index of the patch index array that is the first index
        // greater than or equal to the true index
        let true_patch_index = index + self.indices_offset;
        search_sorted(self.indices(), true_patch_index, SearchSortedSide::Left).and_then(|idx| {
            // If the value at this index is equal to the true index, then it exists in the patch index array,
            // and we should return the value at the corresponding index in the patch values array
            scalar_at(self.indices(), idx)
                .or_else(|_| Ok(Scalar::null(self.values().dtype())))
                .and_then(usize::try_from)
                .and_then(|patch_index| {
                    if patch_index == true_patch_index {
                        scalar_at(self.values(), idx)
                    } else {
                        Ok(Scalar::null(self.values().dtype()))
                    }
                })
        })
    }
}
