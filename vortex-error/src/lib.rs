use std::borrow::Cow;
use std::env;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use crate::array::EncodingId;
use crate::dtype::DType;
use crate::ptype::PType;

#[derive(Debug, PartialEq)]
pub struct ErrString(Cow<'static, str>);

impl<T> From<T> for ErrString
where
    T: Into<Cow<'static, str>>,
{
    fn from(msg: T) -> Self {
        if env::var("ENC_PANIC_ON_ERR").as_deref().unwrap_or("") == "1" {
            panic!("{}", msg.into())
        } else {
            ErrString(msg.into())
        }
    }
}

impl AsRef<str> for ErrString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for ErrString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for ErrString {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum VortexError {
    #[error("index {0} out of bounds from {1} to {2}")]
    OutOfBounds(usize, usize, usize),
    #[error("arguments have different lengths")]
    LengthMismatch,
    #[error("{0}")]
    ComputeError(ErrString),
    #[error("{0}")]
    InvalidArgument(ErrString),
    // Used when a function is not implemented for a given array type.
    #[error("function {0} not implemented for {1}")]
    NotImplemented(&'static str, &'static EncodingId),
    // Used when a function is implemented for an array type, but the RHS is not supported.
    #[error("missing kernel {0} for {1} and {2:?}")]
    MissingKernel(&'static str, &'static EncodingId, Vec<&'static EncodingId>),
    #[error("invalid data type: {0}")]
    InvalidDType(DType),
    #[error("invalid physical type: {0:?}")]
    InvalidPType(PType),
    #[error("invalid array encoding: {0:?}")]
    InvalidEncoding(EncodingId),
    #[error("can't convert type {0} into {1}")]
    IncompatibleTypes(DType, DType),
    #[error("Expected type {0} but found type {1}")]
    MismatchedTypes(DType, DType),
    #[error("unexpected arrow data type: {0:?}")]
    InvalidArrowDataType(arrow_schema::DataType),
    #[error("arrow error: {0:?}")]
    ArrowError(ArrowError),
    #[error("patch values may not be null for base dtype {0}")]
    NullPatchValuesNotAllowed(DType),
    #[error("unsupported DType {0} for data array")]
    UnsupportedDataArrayDType(DType),
    #[error("unsupported DType {0} for offsets array")]
    UnsupportedOffsetsArrayDType(DType),
    #[error("array containing indices or run ends must be strictly monotonically increasing")]
    IndexArrayMustBeStrictSorted,
}

pub type VortexResult<T> = Result<T, VortexError>;

// Wrap up external errors so that we can implement a dumb PartialEq
#[derive(Debug)]
pub struct ArrowError(pub arrow_schema::ArrowError);

impl PartialEq for ArrowError {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl From<arrow_schema::ArrowError> for VortexError {
    fn from(err: arrow_schema::ArrowError) -> Self {
        VortexError::ArrowError(ArrowError(err))
    }
}
