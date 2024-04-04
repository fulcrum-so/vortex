use itertools::Itertools;
use vortex_alloc::{AlignedVec, ALIGNED_ALLOCATOR};
use vortex_error::{vortex_bail, VortexResult};

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::primitive::compute::TypedPrimitiveTrait;
use crate::array::primitive::PrimitiveArray;
use crate::array::validity::Validity;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::AsContiguousFn;
use crate::match_each_native_ptype;
use crate::ptype::NativePType;

impl AsContiguousFn for &dyn PrimitiveTrait {
    fn as_contiguous(&self, arrays: &[ArrayRef]) -> VortexResult<ArrayRef> {
        if !arrays
            .iter()
            .map(|chunk| chunk.as_primitive().ptype())
            .all_equal()
        {
            vortex_bail!(ComputeError: "Chunks have differing ptypes");
        }
        let ptype = arrays[0].as_primitive().ptype();

        let validity = if self.dtype().is_nullable() {
            Some(Validity::from_iter(arrays.iter().map(|v| {
                v.validity().unwrap_or_else(|| Validity::Valid(v.len()))
            })))
        } else {
            None
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
