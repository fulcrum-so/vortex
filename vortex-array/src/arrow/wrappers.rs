use arrow_buffer::{ArrowNativeType, NullBuffer, OffsetBuffer, ScalarBuffer};

use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::ptype::NativePType;
use crate::validity::Validity;

pub fn as_scalar_buffer<T: NativePType + ArrowNativeType>(
    array: PrimitiveArray,
) -> ScalarBuffer<T> {
    assert_eq!(array.ptype(), T::PTYPE);
    ScalarBuffer::from(array.buffer().clone())
}

pub fn as_offset_buffer<T: NativePType + ArrowNativeType>(
    array: PrimitiveArray,
) -> OffsetBuffer<T> {
    OffsetBuffer::new(as_scalar_buffer(array))
}

pub fn as_nulls(validity: Option<Validity>) -> VortexResult<Option<NullBuffer>> {
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
