use crate::array::bool::BoolArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::VarBinArray;
use crate::array::{Array, ArrayRef, CloneOptionalArray};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::cast::cast_primitive;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::ptype::PType;
use crate::scalar::{NullableScalar, Scalar, ScalarRef};
use itertools::Itertools;
use std::ops::Deref;

impl ArrayCompute for VarBinArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl AsContiguousFn for VarBinArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        let bytes = as_contiguous(
            arrays
                .iter()
                .map(|a| a.as_varbin().sliced_bytes())
                .try_collect()?,
        )?;

        let validity = as_contiguous(
            arrays
                .iter()
                .map(|a| {
                    a.as_varbin()
                        .validity()
                        .clone_optional()
                        .unwrap_or_else(|| BoolArray::from(vec![true; a.len()]).boxed())
                })
                .collect_vec(),
        )?;

        let mut offsets = Vec::new();
        arrays
            .iter()
            .map(|a| a.as_varbin())
            .map(|a| a.sliced_offsets().unwrap())
            .for_each(|o| {
                let offsets_array = cast_primitive(o.deref(), &PType::U64).expect("TODO");
                let shift = offsets.last().copied().unwrap_or(0);
                offsets.extend(offsets_array.typed_data::<u64>().iter().map(|o| o + shift));
            });

        let offsets_array = PrimitiveArray::from_vec(offsets).boxed();

        Ok(VarBinArray::new(offsets_array, bytes, self.dtype.clone(), Some(validity)).boxed())
    }
}

impl ScalarAtFn for VarBinArray {
    fn scalar_at(&self, index: usize) -> VortexResult<ScalarRef> {
        if self.is_valid(index) {
            self.bytes_at(index).map(|bytes| {
                if matches!(self.dtype, DType::Utf8(_)) {
                    unsafe { String::from_utf8_unchecked(bytes) }.into()
                } else {
                    bytes.into()
                }
            })
        } else {
            Ok(NullableScalar::none(self.dtype.clone()).boxed())
        }
    }
}
