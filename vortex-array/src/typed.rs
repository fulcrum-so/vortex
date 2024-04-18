use std::collections::HashMap;
use std::sync::Arc;

use vortex_error::{vortex_err, VortexError, VortexResult};
use vortex_schema::DType;

use crate::buffer::OwnedBuffer;
use crate::scalar::Scalar;
use crate::stats::Stat;
use crate::{Array, ArrayData, ArrayDef, AsArray, IntoArray, ToArray, TryDeserializeArrayMetadata};

#[derive(Debug, Clone)]
pub struct TypedArray<'a, D: ArrayDef> {
    array: Array<'a>,
    metadata: D::Metadata,
}

impl<D: ArrayDef> TypedArray<'_, D> {
    pub fn try_from_parts(
        dtype: DType,
        metadata: D::Metadata,
        buffers: Box<[OwnedBuffer]>,
        children: Box<[ArrayData]>,
        stats: HashMap<Stat, Scalar>,
    ) -> VortexResult<Self> {
        let array = Array::Data(ArrayData::try_new(
            D::ENCODING,
            dtype,
            Arc::new(metadata.clone()),
            buffers,
            children,
            stats,
        )?);
        Ok(Self { array, metadata })
    }

    pub fn metadata(&self) -> &D::Metadata {
        &self.metadata
    }
}

impl<'a, 'b, D: ArrayDef> TypedArray<'b, D> {
    pub fn array(&'a self) -> &'a Array<'b> {
        &self.array
    }
}

impl<'a, D: ArrayDef> TryFrom<Array<'a>> for TypedArray<'a, D> {
    type Error = VortexError;

    fn try_from(array: Array<'a>) -> Result<Self, Self::Error> {
        if array.encoding().id() != D::ENCODING.id() {
            return Err(vortex_err!("incorrect encoding"));
        }
        let metadata = match &array {
            Array::Data(d) => d
                .metadata()
                .as_any()
                .downcast_ref::<D::Metadata>()
                .unwrap()
                .clone(),
            Array::DataRef(d) => d
                .metadata()
                .as_any()
                .downcast_ref::<D::Metadata>()
                .unwrap()
                .clone(),
            Array::View(v) => D::Metadata::try_deserialize_metadata(v.metadata())?,
        };
        Ok(TypedArray { array, metadata })
    }
}

impl<'a, D: ArrayDef> TryFrom<&'a Array<'a>> for TypedArray<'a, D> {
    type Error = VortexError;

    fn try_from(value: &'a Array<'a>) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl<'a, D: ArrayDef> AsArray for TypedArray<'a, D> {
    fn as_array_ref(&self) -> &Array {
        &self.array
    }
}

impl<D: ArrayDef> ToArray for TypedArray<'_, D> {
    fn to_array(&self) -> Array {
        self.array.clone()
    }
}

impl<'a, D: ArrayDef> IntoArray<'a> for TypedArray<'a, D> {
    fn into_array(self) -> Array<'a> {
        self.array
    }
}
