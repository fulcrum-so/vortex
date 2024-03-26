use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;

use vortex_schema::{DType, ErrString};

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum VortexError {
    #[error("index {0} out of bounds from {1} to {2}")]
    OutOfBounds(usize, usize, usize),
    #[error("{0}")]
    ComputeError(ErrString),
    #[error("{0}")]
    InvalidArgument(ErrString),
    // Used when a function is not implemented for a given array type.
    #[error("function {0} not implemented for {1}")]
    NotImplemented(&'static str, &'static str),
    #[error("missing kernel {0} for {1} and {2:?}")]
    MissingKernel(&'static str, &'static str, Vec<&'static str>),
    #[error("invalid data type: {0}")]
    InvalidDType(DType),
    #[error("Expected type {0} but found type {1}")]
    MismatchedTypes(DType, DType),
    #[error("unexpected arrow data type: {0:?}")]
    InvalidArrowDataType(arrow_schema::DataType),
    #[error("unsupported DType {0} for data array")]
    UnsupportedDataArrayDType(DType),
    #[error("unsupported DType {0} for offsets array")]
    UnsupportedOffsetsArrayDType(DType),
    #[error("array containing indices or run ends must be strictly monotonically increasing")]
    IndexArrayMustBeStrictSorted,
    #[error(transparent)]
    ArrowError(ArrowError),
    #[error(transparent)]
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

        impl Display for $e {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self)
            }
        }

        impl Error for $e {}
    };
}

wrapped_error!(arrow_schema::ArrowError, ArrowError);
wrapped_error!(io::Error, IOError);
