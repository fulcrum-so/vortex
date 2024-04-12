use vortex_error::{vortex_err, VortexResult};

use crate::array::bool::BoolData;
use crate::array::primitive::PrimitiveData;
use crate::array::r#struct::StructData;
use crate::{Array, ArrayData, IntoArray, IntoArrayData, OwnedArray};

pub trait FlattenFn {
    fn flatten(&self) -> VortexResult<FlattenedData>;
}

/// The set of encodings that can be converted to Arrow with zero-copy.
pub enum FlattenedData {
    Bool(BoolData),
    Primitive(PrimitiveData),
    Struct(StructData),
}

impl IntoArray<'static> for FlattenedData {
    fn into_array(self) -> OwnedArray {
        Array::Data(self.into_array_data())
    }
}

impl IntoArrayData for FlattenedData {
    fn into_array_data(self) -> ArrayData {
        match self {
            FlattenedData::Bool(array) => array.into_array_data(),
            FlattenedData::Primitive(array) => array.into_array_data(),
            FlattenedData::Struct(array) => array.into_array_data(),
        }
    }
}

/// Flatten an array into one of the flat encodings.
/// This does not guarantee that the array is recursively flattened.
pub fn flatten(array: &Array) -> VortexResult<FlattenedData> {
    array.with_dyn(|a| {
        a.flatten().map(|f| f.flatten()).unwrap_or_else(|| {
            Err(vortex_err!(NotImplemented: "flatten", array.encoding().id().name()))
        })
    })
}

pub fn flatten_bool(array: &Array) -> VortexResult<BoolData> {
    if let FlattenedData::Bool(b) = flatten(array)? {
        Ok(b)
    } else {
        Err(vortex_err!("Cannot flatten array {} into bool", array))
    }
}

pub fn flatten_primitive(array: &Array) -> VortexResult<PrimitiveData> {
    if let FlattenedData::Primitive(p) = flatten(array)? {
        Ok(p)
    } else {
        Err(vortex_err!("Cannot flatten array {} into primitive", array))
    }
}

pub fn flatten_struct(array: &Array) -> VortexResult<StructData> {
    if let FlattenedData::Struct(s) = flatten(array)? {
        Ok(s)
    } else {
        Err(vortex_err!("Cannot flatten array {} into struct", array))
    }
}
