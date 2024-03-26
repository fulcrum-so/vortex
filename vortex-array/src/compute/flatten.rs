use vortex_error::{VortexError, VortexResult};

use crate::array::bool::BoolArray;
use crate::array::chunked::ChunkedArray;
use crate::array::composite::CompositeArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::varbin::VarBinArray;
use crate::array::varbinview::VarBinViewArray;
use crate::array::{Array, ArrayRef};

pub trait FlattenFn {
    fn flatten(&self) -> VortexResult<FlattenedArray>;
}

/// The set of encodings that can be converted to Arrow with zero-copy.
pub enum FlattenedArray {
    Bool(BoolArray),
    Chunked(ChunkedArray),
    Composite(CompositeArray),
    Primitive(PrimitiveArray),
    Struct(StructArray),
    VarBin(VarBinArray),
    VarBinView(VarBinViewArray),
}

impl FlattenedArray {
    pub fn into_array(self) -> ArrayRef {
        match self {
            FlattenedArray::Bool(array) => array.into_array(),
            FlattenedArray::Chunked(array) => array.into_array(),
            FlattenedArray::Composite(array) => array.into_array(),
            FlattenedArray::Primitive(array) => array.into_array(),
            FlattenedArray::Struct(array) => array.into_array(),
            FlattenedArray::VarBin(array) => array.into_array(),
            FlattenedArray::VarBinView(array) => array.into_array(),
        }
    }
}

/// Flatten an array into one of the flat encodings.
/// This does not guarantee that the array is recursively flattened.
pub fn flatten(array: &dyn Array) -> VortexResult<FlattenedArray> {
    array.flatten().map(|f| f.flatten()).unwrap_or_else(|| {
        Err(VortexError::NotImplemented(
            "flatten",
            array.encoding().id().name(),
        ))
    })
}

pub fn flatten_bool(array: &dyn Array) -> VortexResult<BoolArray> {
    if let FlattenedArray::Bool(b) = flatten(array)? {
        Ok(b)
    } else {
        Err(VortexError::InvalidArgument(
            format!("Cannot flatten array {} into bool", array).into(),
        ))
    }
}

pub fn flatten_primitive(array: &dyn Array) -> VortexResult<PrimitiveArray> {
    if let FlattenedArray::Primitive(p) = flatten(array)? {
        Ok(p)
    } else {
        Err(VortexError::InvalidArgument(
            format!("Cannot flatten array {} into primitive", array).into(),
        ))
    }
}

pub fn flatten_struct(array: &dyn Array) -> VortexResult<StructArray> {
    if let FlattenedArray::Struct(s) = flatten(array)? {
        Ok(s)
    } else {
        Err(VortexError::InvalidArgument(
            format!("Cannot flatten array {} into struct", array).into(),
        ))
    }
}
