use std::io;

use vortex_schema::{DType, ErrString};

use crate::array::EncodingId;
use crate::ptype::PType;

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
    #[error("patch values may not be null for base dtype {0}")]
    NullPatchValuesNotAllowed(DType),
    #[error("unsupported DType {0} for data array")]
    UnsupportedDataArrayDType(DType),
    #[error("unsupported DType {0} for offsets array")]
    UnsupportedOffsetsArrayDType(DType),
    #[error("array containing indices or run ends must be strictly monotonically increasing")]
    IndexArrayMustBeStrictSorted,
    #[error("arrow error: {0:?}")]
    ArrowError(ArrowError),
    #[error("io error: {0:?}")]
    IOError(IOError),
}

pub type VortexResult<T> = Result<T, VortexError>;

impl From<&str> for VortexError {
    fn from(value: &str) -> Self {
        VortexError::InvalidArgument(value.to_string().into())
    }
}

macro_rules! wrapped_error {
    ($E:ty, $e:ident) => {
        #[derive(Debug)]
        pub struct $e(pub $E);

        impl PartialEq for $e {
            fn eq(&self, _other: &Self) -> bool {
                false
            }
        }

        impl From<$E> for VortexError {
            fn from(err: $E) -> Self {
                VortexError::$e($e(err))
            }
        }
    };
}

wrapped_error!(arrow_schema::ArrowError, ArrowError);
wrapped_error!(io::Error, IOError);
