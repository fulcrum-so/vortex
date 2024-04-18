mod accessor;
mod compute;
mod stats;

use arrow_buffer::{ArrowNativeType, ScalarBuffer};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use vortex_error::VortexResult;

use crate::buffer::Buffer;
use crate::ptype::{NativePType, PType};
use crate::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::ArrayFlatten;
use crate::{impl_encoding, ArrayDType};

impl_encoding!("vortex.primitive", Primitive);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrimitiveMetadata {
    validity: ValidityMetadata,
}

impl PrimitiveArray<'_> {
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
        self.array().buffer(0).expect("missing buffer")
    }

    pub fn scalar_buffer<T: NativePType>(&self) -> ScalarBuffer<T> {
        ScalarBuffer::new(self.buffer().clone().into(), 0, self.len())
    }

    pub fn typed_data<T: NativePType>(&self) -> &[T] {
        self.buffer().typed_data::<T>()
    }
}

impl PrimitiveArray<'_> {
    pub fn try_new<T: NativePType + ArrowNativeType>(
        buffer: ScalarBuffer<T>,
        validity: Validity,
    ) -> VortexResult<Self> {
        let d_type = DType::from(T::PTYPE);
        Self::try_from_parts(

            d_type.with_nullability(validity.nullability()),
            PrimitiveMetadata {
                validity: validity.to_metadata(buffer.len())?,
            },
            vec![Buffer::Owned(buffer.into_inner())].into(),
            validity.into_array_data().into_iter().collect_vec().into(),
            HashMap::default(),
        )
    }

    pub fn from_vec<T: NativePType + ArrowNativeType>(values: Vec<T>, validity: Validity) -> Self {
        Self::try_new(ScalarBuffer::from(values), validity).unwrap()
    }

    pub fn from_nullable_vec<T: NativePType + ArrowNativeType>(values: Vec<Option<T>>) -> Self {
        let elems: Vec<T> = values.iter().map(|v| v.unwrap_or_default()).collect();
        let validity = Validity::from(values.iter().map(|v| v.is_some()).collect::<Vec<_>>());
        Self::from_vec(elems, validity)
    }
}

impl<T: NativePType> From<Vec<T>> for PrimitiveArray<'_> {
    fn from(values: Vec<T>) -> Self {
        PrimitiveArray::from_vec(values, Validity::NonNullable)
    }
}

impl<T: NativePType> IntoArray<'static> for Vec<T> {
    fn into_array(self) -> Array<'static> {
        PrimitiveArray::from(self).into_array()
    }
}

impl ArrayFlatten for PrimitiveArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        Ok(Flattened::Primitive(self))
    }
}

impl ArrayTrait for PrimitiveArray<'_> {
    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }
}

impl ArrayValidity for PrimitiveArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for PrimitiveArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer())?;
        visitor.visit_validity(&self.validity())
    }
}

impl EncodingCompression for PrimitiveEncoding {}
