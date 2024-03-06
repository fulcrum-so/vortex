use arrow::buffer::BooleanBuffer;
use itertools::Itertools;
use vortex_alloc::{AlignedVec, ALIGNED_ALLOCATOR};

use crate::array::bool::{BoolArray, BoolEncoding};
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::{PrimitiveArray, PrimitiveEncoding};
use crate::array::{Array, ArrayRef, CloneOptionalArray};
use crate::error::{VortexError, VortexResult};
use crate::ptype::{match_each_native_ptype, NativePType};

pub fn as_contiguous(arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
    if arrays.is_empty() {
        return Err(VortexError::ComputeError("No arrays to concatenate".into()));
    }
    if !arrays.iter().map(|chunk| chunk.encoding().id()).all_equal() {
        return Err(VortexError::ComputeError(
            "Chunks have differing encodings".into(),
        ));
    }

    match *arrays[0].encoding().id() {
        BoolEncoding::ID => {
            Ok(bool_as_contiguous(arrays.iter().map(|a| a.as_bool()).collect())?.boxed())
        }
        PrimitiveEncoding::ID => {
            Ok(primitive_as_contiguous(arrays.iter().map(|a| a.as_primitive()).collect())?.boxed())
        }
        _ => Err(VortexError::ComputeError(
            format!("as_contiguous not supported for {:?}", arrays[0].encoding()).into(),
        ))?,
    }
}

fn bool_as_contiguous(arrays: Vec<&BoolArray>) -> VortexResult<BoolArray> {
    // TODO(ngates): implement a HasValidity trait to avoid this duplicate code.
    let validity = if arrays.iter().all(|a| a.validity().is_none()) {
        None
    } else {
        Some(as_contiguous(
            arrays
                .iter()
                .map(|a| {
                    a.validity()
                        .clone_optional()
                        .unwrap_or_else(|| vec![true; a.len()].into())
                })
                .collect(),
        )?)
    };

    Ok(BoolArray::new(
        BooleanBuffer::from(
            arrays
                .iter()
                .flat_map(|a| a.buffer().iter())
                .collect::<Vec<bool>>(),
        ),
        validity,
    ))
}

fn primitive_as_contiguous(arrays: Vec<&PrimitiveArray>) -> VortexResult<PrimitiveArray> {
    if !arrays.iter().map(|chunk| (*chunk).ptype()).all_equal() {
        return Err(VortexError::ComputeError(
            "Chunks have differing ptypes".into(),
        ));
    }
    let ptype = arrays[0].ptype();

    let validity = if arrays.iter().all(|a| a.validity().is_none()) {
        None
    } else {
        Some(as_contiguous(
            arrays
                .iter()
                .map(|a| {
                    a.validity()
                        .clone_optional()
                        .unwrap_or_else(|| vec![true; a.len()].into())
                })
                .collect(),
        )?)
    };

    Ok(match_each_native_ptype!(ptype, |$P| {
        PrimitiveArray::from_nullable_in(
            native_primitive_as_contiguous(arrays.iter().map(|a| a.buffer().typed_data::<$P>()).collect()),
            validity,
        )
    }))
}

fn native_primitive_as_contiguous<P: NativePType>(arrays: Vec<&[P]>) -> AlignedVec<P> {
    let len = arrays.iter().map(|a| a.len()).sum();
    let mut result = AlignedVec::with_capacity_in(len, ALIGNED_ALLOCATOR);
    arrays.iter().for_each(|arr| result.extend_from_slice(arr));
    result
}
