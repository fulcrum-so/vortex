mod compute;
mod stats;

use arrow_buffer::{ArrowNativeType, ScalarBuffer};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use vortex::ptype::{NativePType, PType};
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::buffer::Buffer;
use crate::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::TypedArrayData;
use crate::{impl_encoding, IntoArray};
use crate::{ArrayFlatten, ArrayMetadata};

impl_encoding!("vortex.primitive", Primitive);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrimitiveMetadata {
    validity: ValidityMetadata,
}
//
// pub struct PrimitiveArray<'a> {
//     ptype: PType,
//     dtype: &'a DType,
//     // TODO(ngates): reference or clone?
//     buffer: &'a Buffer<'a>,
//     validity: Validity<'a>,
//     stats: &'a (dyn Statistics + 'a),
// }
//
// impl PrimitiveArray<'_> {
//     pub fn buffer(&self) -> &Buffer {
//         self.buffer
//     }
//
//     pub fn validity(&self) -> &Validity {
//         &self.validity
//     }
//
//     pub fn ptype(&self) -> PType {
//         self.ptype
//     }
//
//     pub fn typed_data<T: NativePType>(&self) -> &[T] {
//         self.buffer.typed_data::<T>()
//     }
// }

impl PrimitiveArray<'_> {
    pub fn buffer(&self) -> &Buffer {
        self.array().buffer(0).expect("missing buffer")
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

    pub fn typed_data<T: NativePType>(&self) -> &[T] {
        self.buffer().typed_data::<T>()
    }
}

impl PrimitiveData {
    pub fn try_new<T: NativePType + ArrowNativeType>(
        buffer: ScalarBuffer<T>,
        validity: Validity,
    ) -> VortexResult<Self> {
        Ok(Self::new_unchecked(
            DType::from(T::PTYPE).with_nullability(validity.nullability()),
            Arc::new(PrimitiveMetadata {
                validity: validity.to_metadata(buffer.len())?,
            }),
            vec![Buffer::Owned(buffer.into_inner())].into(),
            validity.to_array_data().into_iter().collect_vec().into(),
        ))
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

impl<T: NativePType> From<Vec<T>> for PrimitiveData {
    fn from(values: Vec<T>) -> Self {
        PrimitiveData::from_vec(values, Validity::NonNullable)
    }
}

impl<T: NativePType> IntoArray<'static> for Vec<T> {
    fn into_array(self) -> Array<'static> {
        PrimitiveData::from(self).into_array()
    }
}

impl ArrayFlatten for PrimitiveArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        todo!()
    }
}

impl ArrayTrait for PrimitiveArray<'_> {
    fn dtype(&self) -> &DType {
        self.array().dtype()
    }

    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }

    fn metadata(&self) -> Arc<dyn ArrayMetadata> {
        Arc::new(self.metadata().clone())
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
