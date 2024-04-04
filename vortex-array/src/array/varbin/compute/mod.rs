use std::sync::Arc;

use arrow_array::{
    ArrayRef as ArrowArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
};
use itertools::Itertools;
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::DType;

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::{PrimitiveArray, PrimitiveTrait, TypedPrimitiveTrait};
use crate::array::validity::Validity;
use crate::array::varbin::VarBinArray;
use crate::array::{Array, ArrayRef};
use crate::arrow::wrappers::{as_nulls, as_offset_buffer};
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::cast::cast;
use crate::compute::flatten::{flatten, flatten_primitive, FlattenFn, FlattenedArray};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::ptype::PType;
use crate::scalar::{BinaryScalar, Scalar, Utf8Scalar};

mod take;

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

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl AsContiguousFn for VarBinArray {
    fn as_contiguous(&self, arrays: &[ArrayRef]) -> VortexResult<ArrayRef> {
        let bytes_chunks: Vec<ArrayRef> = arrays
            .iter()
            .map(|a| a.as_varbin().sliced_bytes())
            .try_collect()?;
        let bytes = as_contiguous(&bytes_chunks)?;

        let validity = if self.dtype().is_nullable() {
            Some(Validity::from_iter(arrays.iter().map(|a| {
                a.validity().unwrap_or_else(|| Validity::Valid(a.len()))
            })))
        } else {
            None
        };

        let mut offsets = Vec::new();
        offsets.push(0);
        for a in arrays.iter().map(|a| a.as_varbin()) {
            let first_offset: u64 = a.first_offset()?;
            let offsets_array = flatten_primitive(cast(a.offsets(), PType::U64.into())?.as_ref())?;
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

        Ok(VarBinArray::new(offsets_array, bytes, self.dtype.clone(), validity).into_array())
    }
}

impl AsArrowArray for VarBinArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // Ensure the offsets are either i32 or i64
        let offsets = flatten_primitive(self.offsets())?;
        let offsets = match offsets.ptype() {
            PType::I32 | PType::I64 => offsets,
            // Unless it's u64, everything else can be converted into an i32.
            // FIXME(ngates): do not copy offsets again
            PType::U64 => {
                flatten_primitive(cast(&offsets.to_array(), PType::I64.into())?.as_ref())?
            }
            _ => flatten_primitive(cast(&offsets.to_array(), PType::I32.into())?.as_ref())?,
        };
        let nulls = as_nulls(self.validity())?;

        let data = flatten_primitive(self.bytes())?;
        assert_eq!(data.ptype(), PType::U8);
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
            _ => vortex_bail!(MismatchedTypes: "utf8 or binary", self.dtype()),
        })
    }
}

impl FlattenFn for VarBinArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        let bytes = flatten(self.bytes())?.into_array();
        let offsets = flatten(self.offsets())?.into_array();
        Ok(FlattenedArray::VarBin(VarBinArray::new(
            offsets,
            bytes,
            self.dtype.clone(),
            self.validity(),
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
            // FIXME(ngates): there's something weird about this.
        } else if matches!(self.dtype, DType::Utf8(_)) {
            Ok(Utf8Scalar::none().into())
        } else {
            Ok(BinaryScalar::none().into())
        }
    }
}
