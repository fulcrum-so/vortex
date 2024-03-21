use std::any::Any;
use std::fmt::{Debug, Display, Formatter};

use linkme::distributed_slice;
use vortex_schema::{DType, Nullability};

use crate::array::bool::{BoolArray, BoolEncoding};
use crate::array::chunked::{ChunkedArray, ChunkedEncoding};
use crate::array::composite::{CompositeArray, CompositeEncoding};
use crate::array::constant::{ConstantArray, ConstantEncoding};
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::{PrimitiveArray, PrimitiveEncoding};
use crate::array::sparse::{SparseArray, SparseEncoding};
use crate::array::struct_::{StructArray, StructEncoding};
use crate::array::varbin::{VarBinArray, VarBinEncoding};
use crate::array::varbinview::{VarBinViewArray, VarBinViewEncoding};
use crate::compress::EncodingCompression;
use crate::compute::ArrayCompute;
use crate::error::{VortexError, VortexResult};
use crate::formatter::ArrayDisplay;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::Stats;

pub mod bool;
pub mod chunked;
pub mod composite;
pub mod constant;
pub mod downcast;
pub mod primitive;
pub mod sparse;
pub mod struct_;
pub mod varbin;
pub mod varbinview;

pub type ArrayRef = Box<dyn Array>;

/// An Enc Array is the base object representing all arrays in enc.
///
/// Arrays have a dtype and an encoding. DTypes represent the logical type of the
/// values stored in a vortex array. Encodings represent the physical layout of the
/// array.
///
/// This differs from Apache Arrow where logical and physical are combined in
/// the data type, e.g. LargeString, RunEndEncoded.
pub trait Array:
    ArrayDisplay + ArrayCompute + Debug + Send + Sync + dyn_clone::DynClone + 'static
{
    /// Converts itself to a reference of [`Any`], which enables downcasting to concrete types.
    fn as_any(&self) -> &dyn Any;
    /// Move an owned array to `ArrayRef`
    fn boxed(self) -> ArrayRef;
    /// Convert boxed array into `Box<dyn Any>`
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    /// Get the length of the array
    fn len(&self) -> usize;
    /// Check whether the array is empty
    fn is_empty(&self) -> bool;
    /// Get the dtype of the array
    fn dtype(&self) -> &DType;
    /// Get statistics for the array
    fn stats(&self) -> Stats;
    /// Limit array to start..stop range
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef>;
    /// Encoding kind of the array
    fn encoding(&self) -> &'static dyn Encoding;
    /// Approximate size in bytes of the array. Only takes into account variable size portion of the array
    fn nbytes(&self) -> usize;

    fn serde(&self) -> Option<&dyn ArraySerde> {
        None
    }
}

dyn_clone::clone_trait_object!(Array);

pub trait CloneOptionalArray {
    fn clone_optional(&self) -> Option<ArrayRef>;
}

impl CloneOptionalArray for Option<&dyn Array> {
    fn clone_optional(&self) -> Option<ArrayRef> {
        self.map(dyn_clone::clone_box)
    }
}

pub fn check_slice_bounds(array: &dyn Array, start: usize, stop: usize) -> VortexResult<()> {
    if start > array.len() {
        return Err(VortexError::OutOfBounds(start, 0, array.len()));
    }
    if stop > array.len() {
        return Err(VortexError::OutOfBounds(stop, 0, array.len()));
    }
    Ok(())
}

pub fn check_validity_buffer(
    validity: Option<&dyn Array>,
    expected_len: usize,
) -> VortexResult<()> {
    if let Some(v) = validity {
        if !matches!(v.dtype(), DType::Bool(Nullability::NonNullable)) {
            return Err(VortexError::MismatchedTypes(
                validity.unwrap().dtype().clone(),
                DType::Bool(Nullability::NonNullable),
            ));
        }
        if v.len() != expected_len {
            return Err(VortexError::InvalidArgument(
                format!(
                    "Validity buffer {} has incorrect length {}, expected {}",
                    v,
                    v.len(),
                    expected_len
                )
                .into(),
            ));
        }
    }

    Ok(())
}

impl<'a> AsRef<(dyn Array + 'a)> for dyn Array {
    fn as_ref(&self) -> &(dyn Array + 'a) {
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EncodingId(&'static str);

impl EncodingId {
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.0
    }
}

impl Display for EncodingId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.0, f)
    }
}

pub trait Encoding: Debug + Send + Sync + 'static {
    fn id(&self) -> &EncodingId;

    /// Whether this encoding provides a compressor.
    fn compression(&self) -> Option<&dyn EncodingCompression> {
        None
    }

    /// Array serialization
    fn serde(&self) -> Option<&dyn EncodingSerde> {
        None
    }
}

impl Display for dyn Encoding {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id())
    }
}

pub type EncodingRef = &'static dyn Encoding;

#[distributed_slice]
pub static ENCODINGS: [EncodingRef] = [..];

#[derive(Debug, Clone)]
pub enum ArrayKind<'a> {
    Bool(&'a BoolArray),
    Chunked(&'a ChunkedArray),
    Composite(&'a CompositeArray),
    Constant(&'a ConstantArray),
    Primitive(&'a PrimitiveArray),
    Sparse(&'a SparseArray),
    Struct(&'a StructArray),
    VarBin(&'a VarBinArray),
    VarBinView(&'a VarBinViewArray),
    Other(&'a dyn Array),
}

impl<'a> From<&'a dyn Array> for ArrayKind<'a> {
    fn from(value: &'a dyn Array) -> Self {
        match *value.encoding().id() {
            BoolEncoding::ID => ArrayKind::Bool(value.as_bool()),
            ChunkedEncoding::ID => ArrayKind::Chunked(value.as_chunked()),
            CompositeEncoding::ID => ArrayKind::Composite(value.as_composite()),
            ConstantEncoding::ID => ArrayKind::Constant(value.as_constant()),
            PrimitiveEncoding::ID => ArrayKind::Primitive(value.as_primitive()),
            SparseEncoding::ID => ArrayKind::Sparse(value.as_sparse()),
            StructEncoding::ID => ArrayKind::Struct(value.as_struct()),
            VarBinEncoding::ID => ArrayKind::VarBin(value.as_varbin()),
            VarBinViewEncoding::ID => ArrayKind::VarBinView(value.as_varbinview()),
            _ => ArrayKind::Other(value),
        }
    }
}

impl Display for dyn Array {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}, len={})",
            self.encoding().id(),
            self.dtype(),
            self.len()
        )
    }
}
