#![feature(error_generic_member_access)]

use std::backtrace::Backtrace;
use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;
use std::{env, fmt, io};

#[derive(Debug)]
pub struct ErrString(Cow<'static, str>);

impl<T> From<T> for ErrString
where
    T: Into<Cow<'static, str>>,
{
    fn from(msg: T) -> Self {
        if env::var("VORTEX_PANIC_ON_ERR").as_deref().unwrap_or("") == "1" {
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
        Display::fmt(&self.0, f)
    }
}

#[derive(thiserror::Error)]
pub enum VortexError {
    #[error("index {0} out of bounds from {1} to {2}\nBacktrace:\n{3}")]
    OutOfBounds(usize, usize, usize, Backtrace),
    #[error("{0}\nBacktrace:\n{1}")]
    ComputeError(ErrString, Backtrace),
    #[error("{0}\nBacktrace:\n{1}")]
    InvalidArgument(ErrString, Backtrace),
    #[error("{0}\nBacktrace:\n{1}")]
    InvalidSerde(ErrString, Backtrace),
    #[error("function {0} not implemented for {1}\nBacktrace:\n{2}")]
    NotImplemented(ErrString, ErrString, Backtrace),
    #[error("expected type: {0} but instead got {1}\nBacktrace:\n{2}")]
    MismatchedTypes(ErrString, ErrString, Backtrace),
    #[error(transparent)]
    ArrowError(
        #[from]
        #[backtrace]
        arrow_schema::ArrowError,
    ),
    #[error(transparent)]
    FlatBuffersError(
        #[from]
        #[backtrace]
        flatbuffers::InvalidFlatbuffer,
    ),
    #[error(transparent)]
    IOError(
        #[from]
        #[backtrace]
        io::Error,
    ),
    #[cfg(feature = "parquet")]
    #[error(transparent)]
    ParquetError(
        #[from]
        #[backtrace]
        parquet::errors::ParquetError,
    ),
}

pub type VortexResult<T> = Result<T, VortexError>;

impl Debug for VortexError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

#[macro_export]
macro_rules! vortex_err {
    (OutOfBounds: $idx:expr, $start:expr, $stop:expr) => {{
        use std::backtrace::Backtrace;
        $crate::__private::must_use(
            $crate::VortexError::OutOfBounds($idx, $start, $stop, Backtrace::capture())
        )
    }};
    (NotImplemented: $func:expr, $arr:expr) => {{
        use std::backtrace::Backtrace;
        $crate::__private::must_use(
            $crate::VortexError::NotImplemented($func.into(), $arr.into(), Backtrace::capture())
        )
    }};
    (MismatchedTypes: $expected:literal, $actual:expr) => {{
        use std::backtrace::Backtrace;
        $crate::__private::must_use(
            $crate::VortexError::MismatchedTypes($expected.into(), $actual.to_string().into(), Backtrace::capture())
        )
    }};
    (MismatchedTypes: $expected:expr, $actual:expr) => {{
        use std::backtrace::Backtrace;
        $crate::__private::must_use(
            $crate::VortexError::MismatchedTypes($expected.to_string().into(), $actual.to_string().into(), Backtrace::capture())
        )
    }};
    ($variant:ident: $fmt:literal $(, $arg:expr)* $(,)?) => {{
        use std::backtrace::Backtrace;
        $crate::__private::must_use(
            $crate::VortexError::$variant(format!($fmt, $($arg),*).into(), Backtrace::capture())
        )
    }};
    ($variant:ident: $err:expr $(,)?) => {
        $crate::__private::must_use(
            $crate::VortexError::$variant($err)
        )
    };
    ($fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::vortex_err!(InvalidArgument: $fmt, $($arg),*)
    };
}

#[macro_export]
macro_rules! vortex_bail {
    ($($tt:tt)+) => {
        return Err($crate::vortex_err!($($tt)+))
    };
}

// Not public, referenced by macros only.
#[doc(hidden)]
pub mod __private {
    #[doc(hidden)]
    #[inline]
    #[cold]
    #[must_use]
    pub fn must_use(error: crate::VortexError) -> crate::VortexError {
        error
    }
}
