use std::mem::{size_of, ManuallyDrop};

use arrow_buffer::{ArrowNativeType, ScalarBuffer};
use itertools::Itertools;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use vortex_buffer::Buffer;
use vortex_dtype::{match_each_native_ptype, NativePType, PType};
use vortex_error::vortex_bail;

use crate::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::ArrayFlatten;
use crate::{impl_encoding, ArrayDType};

mod accessor;
mod compute;
mod stats;

impl_encoding!("vortex.primitive", Primitive);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrimitiveMetadata {
    validity: ValidityMetadata,
}

impl PrimitiveArray {
    // TODO(ngates): remove the Arrow types from this API.
    pub fn try_new<T: NativePType + ArrowNativeType>(
        buffer: ScalarBuffer<T>,
        validity: Validity,
    ) -> VortexResult<Self> {
        Ok(Self {
            typed: TypedArray::try_from_parts(
                DType::from(T::PTYPE).with_nullability(validity.nullability()),
                PrimitiveMetadata {
                    validity: validity.to_metadata(buffer.len())?,
                },
                Some(Buffer::from(buffer.into_inner())),
                validity.into_array_data().into_iter().collect_vec().into(),
                StatsSet::new(),
            )?,
        })
    }

    pub fn from_vec<T: NativePType>(values: Vec<T>, validity: Validity) -> Self {
        match_each_native_ptype!(T::PTYPE, |$P| {
            Self::try_new(ScalarBuffer::<$P>::from(
                unsafe { std::mem::transmute::<Vec<T>, Vec<$P>>(values) }
            ), validity).unwrap()
        })
    }

    pub fn from_nullable_vec<T: NativePType>(values: Vec<Option<T>>) -> Self {
        let elems: Vec<T> = values.iter().map(|v| v.unwrap_or_default()).collect();
        let validity = Validity::from(values.iter().map(|v| v.is_some()).collect::<Vec<_>>());
        Self::from_vec(elems, validity)
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(0, &Validity::DTYPE))
    }

    pub fn ptype(&self) -> PType {
        // TODO(ngates): we can't really cache this anywhere?
        self.dtype().try_into().unwrap()
    }

    pub fn buffer(&self) -> &Buffer {
        self.array().buffer().expect("missing buffer")
    }

    // TODO(ngates): deprecated, remove this.
    pub fn scalar_buffer<T: NativePType + ArrowNativeType>(&self) -> ScalarBuffer<T> {
        assert_eq!(
            T::PTYPE,
            self.ptype(),
            "Attempted to get scalar buffer of type {} from array of type {}",
            T::PTYPE,
            self.ptype(),
        );
        ScalarBuffer::new(self.buffer().clone().into(), 0, self.len())
    }

    pub fn typed_data<T: NativePType>(&self) -> &[T] {
        assert_eq!(
            T::PTYPE,
            self.ptype(),
            "Attempted to get typed_data of type {} from array of type {}",
            T::PTYPE,
            self.ptype(),
        );

        let (prefix, offsets, suffix) = unsafe { self.buffer().as_ref().align_to::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        offsets
    }

    /// Convert the array into a mutable vec of the given type.
    /// If possible, this will be zero-copy.
    pub fn into_typed_data<T: NativePType>(self) -> Vec<T> {
        assert_eq!(
            T::PTYPE,
            self.ptype(),
            "Attempted to get typed_data of type {} from array of type {}",
            T::PTYPE,
            self.ptype(),
        );
        let bytes = self
            .into_buffer()
            .into_vec()
            .unwrap_or_else(|b| Vec::from(b.as_ref()));

        let mut manual_bytes = ManuallyDrop::new(bytes);
        let (prefix, aligned_data, suffix) = unsafe { manual_bytes.align_to_mut::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());

        unsafe {
            Vec::from_raw_parts(
                aligned_data.as_mut_ptr(),
                manual_bytes.len() / size_of::<T>(),
                manual_bytes.capacity() / size_of::<T>(),
            )
        }
    }

    pub fn get_as_cast<T: NativePType>(&self, idx: usize) -> T {
        match_each_native_ptype!(self.ptype(), |$P| {
            T::from(self.typed_data::<$P>()[idx]).expect("failed to cast")
        })
    }

    pub fn reinterpret_cast(&self, ptype: PType) -> Self {
        if self.ptype() == ptype {
            return self.clone();
        }

        assert_eq!(
            self.ptype().byte_width(),
            ptype.byte_width(),
            "can't reinterpret cast between integers of two different widths"
        );

        match_each_native_ptype!(ptype, |$P| {
            PrimitiveArray::try_new(
                ScalarBuffer::<$P>::new(self.buffer().clone().into(), 0, self.len()),
                self.validity(),
            )
            .unwrap()
        })
    }

    pub fn patch<P: AsPrimitive<usize>, T: NativePType>(
        self,
        positions: &[P],
        values: &[T],
    ) -> VortexResult<Self> {
        if self.ptype() != T::PTYPE {
            vortex_bail!(MismatchedTypes: self.dtype(), T::PTYPE)
        }

        let validity = self.validity();

        let mut own_values = self.into_typed_data();
        // TODO(robert): Also patch validity
        for (idx, value) in positions.iter().zip_eq(values.iter()) {
            own_values[(*idx).as_()] = *value;
        }
        Ok(Self::from_vec(own_values, validity))
    }

    pub fn into_buffer(self) -> Buffer {
        self.into_array()
            .into_buffer()
            .expect("PrimitiveArray must have a buffer")
    }
}

impl<T: NativePType> From<Vec<T>> for PrimitiveArray {
    fn from(values: Vec<T>) -> Self {
        Self::from_vec(values, Validity::NonNullable)
    }
}

impl<T: NativePType> IntoArray for Vec<T> {
    fn into_array(self) -> Array {
        PrimitiveArray::from(self).into_array()
    }
}

impl ArrayFlatten for PrimitiveArray {
    fn flatten(self) -> VortexResult<Flattened> {
        Ok(Flattened::Primitive(self))
    }
}

impl ArrayTrait for PrimitiveArray {
    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }
}

impl ArrayValidity for PrimitiveArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for PrimitiveArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer())?;
        visitor.visit_validity(&self.validity())
    }
}

impl Array {
    pub fn into_primitive(self) -> PrimitiveArray {
        PrimitiveArray::try_from(self).expect("expected primitive array")
    }

    pub fn as_primitive(&self) -> PrimitiveArray {
        PrimitiveArray::try_from(self).expect("expected primitive array")
    }
}

impl EncodingCompression for PrimitiveEncoding {}
