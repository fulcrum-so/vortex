use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::cast::CastFn;
use crate::compute::fill::FillForwardFn;
use crate::compute::flatten::FlattenFn;
use crate::compute::patch::PatchFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::search_sorted::SearchSortedFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::error::{VortexError, VortexResult};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::Stats;

pub mod bool;
pub mod chunked;
pub mod composite;
pub mod constant;
pub mod downcast;
pub mod lazy;
pub mod primitive;
pub mod sparse;
pub mod struct_;
pub mod varbin;
pub mod varbinview;

pub type ArrayRef = Arc<dyn Array>;

/// A Vortex Array is the base object representing all arrays in enc.
///
/// Arrays have a dtype and an encoding. DTypes represent the logical type of the
/// values stored in a vortex array. Encodings represent the physical layout of the
/// array.
///
/// This differs from Apache Arrow where logical and physical are combined in
/// the data type, e.g. LargeString, RunEndEncoded.
pub trait Array: ArrayCompute + ArrayDisplay + Debug + Send + Sync {
    /// Converts itself to a reference of [`Any`], which enables downcasting to concrete types.
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn to_array(&self) -> ArrayRef;
    fn into_array(self) -> ArrayRef;

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
    fn encoding(&self) -> EncodingRef;
    /// Approximate size in bytes of the array. Only takes into account variable size portion of the array
    fn nbytes(&self) -> usize;

    fn serde(&self) -> Option<&dyn ArraySerde> {
        None
    }
}

pub trait IntoArray {
    fn into_array(self) -> ArrayRef;
}

#[macro_export]
macro_rules! impl_array {
    () => {
        #[inline]
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        #[inline]
        fn into_any(self: Arc<Self>) -> std::sync::Arc<dyn std::any::Any + Send + Sync> {
            self
        }

        #[inline]
        fn to_array(&self) -> ArrayRef {
            self.clone().into_array()
        }

        #[inline]
        fn into_array(self) -> ArrayRef {
            std::sync::Arc::new(self)
        }
    };
}

pub use impl_array;

impl ArrayCompute for ArrayRef {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        self.as_ref().as_arrow()
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        self.as_ref().as_contiguous()
    }

    fn cast(&self) -> Option<&dyn CastFn> {
        self.as_ref().cast()
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        self.as_ref().flatten()
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        self.as_ref().fill_forward()
    }

    fn patch(&self) -> Option<&dyn PatchFn> {
        self.as_ref().patch()
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        self.as_ref().scalar_at()
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        self.as_ref().search_sorted()
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        self.as_ref().take()
    }
}

impl Array for ArrayRef {
    fn as_any(&self) -> &dyn Any {
        self.as_ref().as_any()
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn to_array(&self) -> ArrayRef {
        self.as_ref().to_array()
    }

    fn into_array(self) -> ArrayRef {
        self
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    fn dtype(&self) -> &DType {
        self.as_ref().dtype()
    }

    fn stats(&self) -> Stats {
        self.as_ref().stats()
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        self.as_ref().slice(start, stop)
    }

    fn encoding(&self) -> EncodingRef {
        self.as_ref().encoding()
    }

    fn nbytes(&self) -> usize {
        self.as_ref().nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        self.as_ref().serde()
    }
}

impl ArrayDisplay for ArrayRef {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        ArrayDisplay::fmt(self.as_ref(), fmt)
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

pub fn check_validity_buffer(validity: Option<&ArrayRef>, expected_len: usize) -> VortexResult<()> {
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
                    v.clone(),
                    v.len(),
                    expected_len
                )
                .into(),
            ));
        }
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
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
    fn id(&self) -> EncodingId;

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

impl PartialEq<Self> for EncodingRef {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for EncodingRef {}

impl Hash for EncodingRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state)
    }
}

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
        match value.encoding().id() {
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

impl Display for dyn Array + '_ {
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
