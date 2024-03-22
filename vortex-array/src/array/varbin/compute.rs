use std::sync::Arc;

use arrow_array::{
    ArrayRef as ArrowArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
};
use itertools::Itertools;
use vortex_schema::DType;

use crate::array::bool::BoolArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::VarBinArray;
use crate::array::{Array, ArrayRef};
use crate::arrow::wrappers::{as_nulls, as_offset_buffer};
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::cast::cast;
use crate::compute::flatten::{flatten, flatten_primitive, FlattenFn, FlattenedArray};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::error::{VortexError, VortexResult};
use crate::ptype::PType;
use crate::scalar::{BinaryScalar, Scalar, Utf8Scalar};

impl ArrayCompute for VarBinArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

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
                        .cloned()
                        .unwrap_or_else(|| BoolArray::from(vec![true; a.len()]).into_array())
                })
                .collect_vec(),
        )?;

        let mut offsets = Vec::new();
        offsets.push(0);
        for a in arrays.iter().map(|a| a.as_varbin()) {
            let first_offset: u64 = a.first_offset()?;
            let offsets_array = flatten_primitive(cast(a.offsets(), &PType::U64.into())?.as_ref())?;
            let shift = offsets.last().copied().unwrap_or(0);
            offsets.extend(
                offsets_array
                    .typed_data::<u64>()
                    .iter()
                    .skip(1) // Ignore the zero offset for each array
                    .map(|o| o + shift - first_offset),
            );
        }

        let offsets_array = PrimitiveArray::from(offsets).into_array();

        Ok(VarBinArray::new(offsets_array, bytes, self.dtype.clone(), Some(validity)).into_array())
    }
}

impl AsArrowArray for VarBinArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // Ensure the offsets are either i32 or i64
        let offsets = flatten_primitive(self.offsets())?;
        let offsets = match offsets.ptype() {
            &PType::I32 | &PType::I64 => offsets,
            // Unless it's u64, everything else can be converted into an i32.
            // FIXME(ngates): do not copy offsets again
            &PType::U64 => {
                flatten_primitive(cast(&offsets.to_array(), &PType::I64.into())?.as_ref())?
            }
            _ => flatten_primitive(cast(&offsets.to_array(), &PType::I32.into())?.as_ref())?,
        };
        let nulls = as_nulls(offsets.validity())?;

        let data = flatten_primitive(self.bytes())?;
        assert_eq!(data.ptype(), &PType::U8);
        let data = data.buffer().clone();

        // Switch on Arrow DType.
        Ok(match self.dtype() {
            DType::Binary(_) => match offsets.ptype() {
                PType::I32 => Arc::new(BinaryArray::new(
                    as_offset_buffer::<i32>(offsets),
                    data,
                    nulls,
                )),
                PType::I64 => Arc::new(LargeBinaryArray::new(
                    as_offset_buffer::<i64>(offsets),
                    data,
                    nulls,
                )),
                _ => panic!("Invalid offsets type"),
            },
            DType::Utf8(_) => match offsets.ptype() {
                PType::I32 => Arc::new(StringArray::new(
                    as_offset_buffer::<i32>(offsets),
                    data,
                    nulls,
                )),
                PType::I64 => Arc::new(LargeStringArray::new(
                    as_offset_buffer::<i64>(offsets),
                    data,
                    nulls,
                )),
                _ => panic!("Invalid offsets type"),
            },
            _ => return Err(VortexError::InvalidDType(self.dtype().clone())),
        })
    }
}

impl FlattenFn for VarBinArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        let bytes = flatten(self.bytes())?.into_array();
        let offsets = flatten(self.offsets())?.into_array();
        let validity = self
            .validity()
            .map(|v| flatten(v).map(FlattenedArray::into_array))
            .transpose()?;
        Ok(FlattenedArray::VarBin(VarBinArray::new(
            offsets,
            bytes,
            self.dtype.clone(),
            validity,
        )))
    }
}

impl ScalarAtFn for VarBinArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            self.bytes_at(index).map(|bytes| {
                if matches!(self.dtype, DType::Utf8(_)) {
                    unsafe { String::from_utf8_unchecked(bytes) }.into()
                } else {
                    bytes.into()
                }
            })
        } else if matches!(self.dtype, DType::Utf8(_)) {
            Ok(Utf8Scalar::new(None).into())
        } else {
            Ok(BinaryScalar::new(None).into())
        }
    }
}
