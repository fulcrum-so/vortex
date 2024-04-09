use num_traits::PrimInt;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::varbin::builder::VarBinBuilder;
use crate::array::varbin::VarBinArray;
use crate::array::{Array, ArrayRef};
use crate::compute::flatten::flatten_primitive;
use crate::compute::take::TakeFn;
use crate::match_each_integer_ptype;
use crate::ptype::NativePType;
use crate::validity::OwnedValidity;
use crate::validity::ValidityView;

impl TakeFn for VarBinArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        // TODO(ngates): support i64 indices.
        assert!(
            indices.len() < i32::MAX as usize,
            "indices.len() must be less than i32::MAX"
        );

        let offsets = flatten_primitive(self.offsets())?;
        let data = flatten_primitive(self.bytes())?;
        let indices = flatten_primitive(indices)?;
        match_each_integer_ptype!(offsets.ptype(), |$O| {
            match_each_integer_ptype!(indices.ptype(), |$I| {
                Ok(take(
                    self.dtype().clone(),
                    offsets.typed_data::<$O>(),
                    data.typed_data::<u8>(),
                    indices.typed_data::<$I>(),
                    self.validity(),
                ).to_array_data())
            })
        })
    }
}

fn take<I: NativePType + PrimInt, O: NativePType + PrimInt>(
    dtype: DType,
    offsets: &[O],
    data: &[u8],
    indices: &[I],
    validity: Option<ValidityView>,
) -> VarBinArray {
    if let Some(v) = validity {
        return take_nullable(dtype, offsets, data, indices, v);
    }

    let mut builder = VarBinBuilder::<I>::with_capacity(indices.len());
    for &idx in indices {
        let idx = idx.to_usize().unwrap();
        let start = offsets[idx].to_usize().unwrap();
        let stop = offsets[idx + 1].to_usize().unwrap();
        builder.push(Some(&data[start..stop]));
    }
    builder.finish(dtype)
}

fn take_nullable<I: NativePType + PrimInt, O: NativePType + PrimInt>(
    dtype: DType,
    offsets: &[O],
    data: &[u8],
    indices: &[I],
    validity: ValidityView,
) -> VarBinArray {
    let mut builder = VarBinBuilder::<I>::with_capacity(indices.len());
    for &idx in indices {
        let idx = idx.to_usize().unwrap();
        if validity.is_valid(idx) {
            let start = offsets[idx].to_usize().unwrap();
            let stop = offsets[idx + 1].to_usize().unwrap();
            builder.push(Some(&data[start..stop]));
        } else {
            builder.push(None);
        }
    }
    builder.finish(dtype)
}
