use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::{env, fmt, io};

#[derive(Debug, PartialEq)]
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

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SchemaError {
    #[error("{0}")]
    InvalidArgument(ErrString),
    #[error("io error: {0:?}")]
    IOError(IOError),
}

pub type SchemaResult<T> = Result<T, SchemaError>;

macro_rules! wrapped_error {
    ($E:ty, $e:ident) => {
        #[derive(Debug)]
        pub struct $e(pub $E);

        impl PartialEq for $e {
            fn eq(&self, _other: &Self) -> bool {
                false
            }
        }

        impl From<$E> for SchemaError {
            fn from(err: $E) -> Self {
                SchemaError::$e($e(err))
            }
        }
    };
}

wrapped_error!(io::Error, IOError);
