use std::mem::ManuallyDrop;

use arrow_buffer::BooleanBuffer;
use serde::{Deserialize, Serialize};
use vortex::array::bool::BoolArray;
use vortex::{
    impl_encoding,
    validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata},
    visitor::{AcceptArrayVisitor, ArrayVisitor},
};
use vortex::{Canonical, IntoCanonical};
use vortex_buffer::Buffer;

mod compute;
mod stats;

impl_encoding!("vortex.byte_bool", ByteBool);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ByteBoolMetadata {
    validity: ValidityMetadata,
}

impl ByteBoolArray {
    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(0, &Validity::DTYPE))
    }

    pub fn try_new(buffer: Buffer, validity: Validity) -> VortexResult<Self> {
        let length = buffer.len();

        let typed = TypedArray::try_from_parts(
            DType::Bool(validity.nullability()),
            ByteBoolMetadata {
                validity: validity.to_metadata(length)?,
            },
            Some(buffer),
            validity.into_array().into_iter().collect::<Vec<_>>().into(),
            StatsSet::new(),
        )?;

        Ok(typed.into())
    }

    pub fn try_from_vec<V: Into<Validity>>(data: Vec<bool>, validity: V) -> VortexResult<Self> {
        let validity = validity.into();
        let mut vec = ManuallyDrop::new(data);
        vec.shrink_to_fit();

        let ptr = vec.as_mut_ptr() as *mut u8;
        let length = vec.len();
        let capacity = vec.capacity();

        let bytes = unsafe { Vec::from_raw_parts(ptr, length, capacity) };

        let buffer = Buffer::from(bytes);

        Self::try_new(buffer, validity)
    }

    pub fn buffer(&self) -> &Buffer {
        self.array().buffer().expect("missing mandatory buffer")
    }

    fn maybe_null_slice(&self) -> &[bool] {
        // Safety: The internal buffer contains byte-sized bools
        unsafe { std::mem::transmute(self.buffer().as_slice()) }
    }
}

impl From<Vec<bool>> for ByteBoolArray {
    fn from(value: Vec<bool>) -> Self {
        Self::try_from_vec(value, Validity::AllValid).unwrap()
    }
}

impl From<Vec<Option<bool>>> for ByteBoolArray {
    fn from(value: Vec<Option<bool>>) -> Self {
        let validity = Validity::from_iter(value.iter());

        // This doesn't reallocate, and the compiler even vectorizes it
        let data = value.into_iter().map(|b| b.unwrap_or_default()).collect();

        Self::try_from_vec(data, validity).unwrap()
    }
}

impl ArrayTrait for ByteBoolArray {
    fn len(&self) -> usize {
        self.buffer().len()
    }
}

impl IntoCanonical for ByteBoolArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let boolean_buffer = BooleanBuffer::from(self.maybe_null_slice());
        let validity = self.validity();

        BoolArray::try_new(boolean_buffer, validity).map(Canonical::Bool)
    }
}

impl ArrayValidity for ByteBoolArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for ByteBoolArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer())?;
        visitor.visit_validity(&self.validity())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validity_construction() {
        let v = vec![true, false];
        let v_len = v.len();

        let arr = ByteBoolArray::from(v);
        assert_eq!(v_len, arr.len());

        for idx in 0..arr.len() {
            assert!(arr.is_valid(idx));
        }

        let v = vec![Some(true), None, Some(false)];
        let arr = ByteBoolArray::from(v);
        assert!(arr.is_valid(0));
        assert!(!arr.is_valid(1));
        assert!(arr.is_valid(2));
        assert_eq!(arr.len(), 3);

        let v: Vec<Option<bool>> = vec![None, None];
        let v_len = v.len();

        let arr = ByteBoolArray::from(v);
        assert_eq!(v_len, arr.len());

        for idx in 0..arr.len() {
            assert!(!arr.is_valid(idx));
        }
        assert_eq!(arr.len(), 2);
    }
}
