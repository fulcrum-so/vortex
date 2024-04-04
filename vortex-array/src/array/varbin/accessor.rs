use num_traits::AsPrimitive;

use crate::accessor::ArrayAccessor;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::{PrimitiveTrait, TypedPrimitiveTrait};
use crate::array::varbin::VarBinArray;
use crate::array::Array;
use crate::compute::flatten::flatten_primitive;
use crate::compute::scalar_at::scalar_at;
use crate::match_each_native_ptype;

fn offset_at(array: &dyn Array, index: usize) -> usize {
    if let Some(parray) = array.maybe_primitive() {
        match_each_native_ptype!(parray.ptype(), |$P| {
            parray.typed_data::<$P>()[index].as_()
        })
    } else {
        scalar_at(array, index).and_then(|s| s.try_into()).unwrap()
    }
}

impl<'a> ArrayAccessor<'a, &'a [u8]> for VarBinArray {
    fn value(&'a self, index: usize) -> Option<&'a [u8]> {
        if self.is_valid(index) {
            let start = offset_at(self.offsets(), index);
            let end = offset_at(self.offsets(), index + 1);
            Some(&self.bytes().as_primitive().buffer()[start..end])
        } else {
            None
        }
    }
}

impl ArrayAccessor<'_, Vec<u8>> for VarBinArray {
    fn value(&self, index: usize) -> Option<Vec<u8>> {
        if self.is_valid(index) {
            let start = offset_at(self.offsets(), index);
            let end = offset_at(self.offsets(), index + 1);

            let slice_bytes = self.bytes().slice(start, end).unwrap();
            Some(
                flatten_primitive(&slice_bytes)
                    .unwrap()
                    .typed_data::<u8>()
                    .to_vec(),
            )
        } else {
            None
        }
    }
}
