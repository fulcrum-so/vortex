use crate::array::bool::BoolArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::error::{VortexError, VortexResult};
use crate::match_each_native_ptype;
use crate::ptype::NativePType;
use itertools::Itertools;
use vortex_alloc::{AlignedVec, ALIGNED_ALLOCATOR};

impl AsContiguousFn for PrimitiveArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        if !arrays
            .iter()
            .map(|chunk| chunk.as_primitive().ptype())
            .all_equal()
        {
            return Err(VortexError::ComputeError(
                "Chunks have differing ptypes".into(),
            ));
        }
        let ptype = arrays[0].as_primitive().ptype();

        let validity = if arrays.iter().all(|a| a.as_primitive().validity().is_none()) {
            None
        } else {
            Some(as_contiguous(
                arrays
                    .iter()
                    .map(|a| {
                        a.as_primitive()
                            .validity()
                            .cloned()
                            .unwrap_or_else(|| BoolArray::from(vec![true; a.len()]).into_array())
                    })
                    .collect_vec(),
            )?)
        };

        Ok(match_each_native_ptype!(ptype, |$P| {
            PrimitiveArray::from_nullable_in(
                native_primitive_as_contiguous(arrays.iter().map(|a| a.as_primitive().typed_data::<$P>()).collect()),
                validity,
            ).into_array()
        }))
    }
}

fn native_primitive_as_contiguous<P: NativePType>(arrays: Vec<&[P]>) -> AlignedVec<P> {
    let len = arrays.iter().map(|a| a.len()).sum();
    let mut result = AlignedVec::with_capacity_in(len, ALIGNED_ALLOCATOR);
    arrays.iter().for_each(|arr| result.extend_from_slice(arr));
    result
}
