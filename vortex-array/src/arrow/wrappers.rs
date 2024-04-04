use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use vortex_error::VortexResult;

use crate::array::primitive::{PrimitiveArray, PrimitiveTrait};
use crate::array::validity::{Validity, ValidityView};
use crate::ptype::NativePType;

pub fn as_scalar_buffer<T: NativePType>(array: PrimitiveArray) -> ScalarBuffer<T> {
    assert_eq!(array.ptype(), T::PTYPE);
    ScalarBuffer::from(array.buffer().clone())
}

pub fn as_offset_buffer<T: NativePType>(array: PrimitiveArray) -> OffsetBuffer<T> {
    OffsetBuffer::new(as_scalar_buffer(array))
}

pub fn as_nulls_view(validity: Option<ValidityView>) -> VortexResult<Option<NullBuffer>> {
    if validity.is_none() {
        return Ok(None);
    }

    // Short-circuit if the validity is constant
    let validity = validity.unwrap();
    if validity.all_valid() {
        return Ok(None);
    }

    if validity.all_invalid() {
        return Ok(Some(NullBuffer::new_null(validity.len())));
    }

    Ok(Some(NullBuffer::new(
        validity.to_bool_array().buffer().clone(),
    )))
}

pub fn as_nulls(validity: Option<Validity>) -> VortexResult<Option<NullBuffer>> {
    match validity {
        None => Ok(None),
        Some(v) => as_nulls_view(Some(v.as_view())),
    }
}
