use std::fmt::{Debug, Display};
use std::panic::RefUnwindSafe;

use arrow::datatypes::ArrowNativeType;
use half::f16;
use num_traits::{Num, NumCast};

use crate::dtype::{DType, FloatWidth, IntWidth, Signedness};
use crate::error::{VortexError, VortexResult};
use crate::scalar::{PScalar, ScalarRef};

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Hash)]
pub enum PType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F16,
    F32,
    F64,
}

pub trait NativePType:
    Send
    + Sync
    + Sized
    + Debug
    + Display
    + PartialEq
    + PartialOrd
    + Default
    + ArrowNativeType
    + RefUnwindSafe
    + Num
    + NumCast
    + Into<ScalarRef>
    + TryFrom<ScalarRef, Error = VortexError>
    + Into<PScalar>
{
    const PTYPE: PType;
}

macro_rules! native_ptype {
    ($T:ty, $ptype:tt) => {
        impl NativePType for $T {
            const PTYPE: PType = PType::$ptype;
        }
    };
}

native_ptype!(u8, U8);
native_ptype!(u16, U16);
native_ptype!(u32, U32);
native_ptype!(u64, U64);
native_ptype!(i8, I8);
native_ptype!(i16, I16);
native_ptype!(i32, I32);
native_ptype!(i64, I64);
native_ptype!(f16, F16);
native_ptype!(f32, F32);
native_ptype!(f64, F64);

#[macro_export]
macro_rules! match_each_native_ptype {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use $crate::ptype::PType;
        use half::f16;
        match $self {
            PType::I8 => __with__! { i8 },
            PType::I16 => __with__! { i16 },
            PType::I32 => __with__! { i32 },
            PType::I64 => __with__! { i64 },
            PType::U8 => __with__! { u8 },
            PType::U16 => __with__! { u16 },
            PType::U32 => __with__! { u32 },
            PType::U64 => __with__! { u64 },
            PType::F16 => __with__! { f16 },
            PType::F32 => __with__! { f32 },
            PType::F64 => __with__! { f64 },
        }
    })
}
pub use match_each_native_ptype;

#[macro_export]
macro_rules! match_each_integer_ptype {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use $crate::ptype::PType;
        match $self {
            PType::I8 => __with__! { i8 },
            PType::I16 => __with__! { i16 },
            PType::I32 => __with__! { i32 },
            PType::I64 => __with__! { i64 },
            PType::U8 => __with__! { u8 },
            PType::U16 => __with__! { u16 },
            PType::U32 => __with__! { u32 },
            PType::U64 => __with__! { u64 },
            _ => panic!("Unsupported ptype {:?}", $self),
        }
    })
}
pub use match_each_integer_ptype;

#[macro_export]
macro_rules! match_each_signed_integer_ptype {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use $crate::ptype::PType;
        match $self {
            PType::I8 => __with__! { i8 },
            PType::I16 => __with__! { i16 },
            PType::I32 => __with__! { i32 },
            PType::I64 => __with__! { i64 },
            _ => panic!("Unsupported ptype {:?}", $self),
        }
    })
}
pub use match_each_signed_integer_ptype;

#[macro_export]
macro_rules! match_each_unsigned_integer_ptype {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use $crate::ptype::PType;
        match $self {
            PType::U8 => __with__! { u8 },
            PType::U16 => __with__! { u16 },
            PType::U32 => __with__! { u32 },
            PType::U64 => __with__! { u64 },
            _ => panic!("Unsupported ptype {:?}", $self),
        }
    })
}
pub use match_each_unsigned_integer_ptype;

impl PType {
    pub fn is_unsigned_int(self) -> bool {
        matches!(self, PType::U8 | PType::U16 | PType::U32 | PType::U64)
    }

    pub fn is_signed_int(self) -> bool {
        matches!(self, PType::I8 | PType::I16 | PType::I32 | PType::I64)
    }

    pub fn is_int(self) -> bool {
        self.is_unsigned_int() || self.is_signed_int()
    }

    pub fn is_float(self) -> bool {
        matches!(self, PType::F16 | PType::F32 | PType::F64)
    }

    pub fn byte_width(&self) -> usize {
        match_each_native_ptype!(self, |$T| std::mem::size_of::<$T>())
    }

    pub fn bit_width(&self) -> usize {
        self.byte_width() * 8
    }
}

impl TryFrom<&DType> for PType {
    type Error = VortexError;

    fn try_from(value: &DType) -> VortexResult<Self> {
        use Signedness::*;
        match value {
            DType::Int(w, s, _) => match w {
                IntWidth::Unknown => match s {
                    Unknown => Ok(PType::I64),
                    Unsigned => Ok(PType::U64),
                    Signed => Ok(PType::I64),
                },
                IntWidth::_8 => match s {
                    Unknown => Ok(PType::I8),
                    Unsigned => Ok(PType::U8),
                    Signed => Ok(PType::I8),
                },
                IntWidth::_16 => match s {
                    Unknown => Ok(PType::I16),
                    Unsigned => Ok(PType::U16),
                    Signed => Ok(PType::I16),
                },
                IntWidth::_32 => match s {
                    Unknown => Ok(PType::I32),
                    Unsigned => Ok(PType::U32),
                    Signed => Ok(PType::I32),
                },
                IntWidth::_64 => match s {
                    Unknown => Ok(PType::I64),
                    Unsigned => Ok(PType::U64),
                    Signed => Ok(PType::I64),
                },
            },
            DType::Float(f, _) => match f {
                FloatWidth::Unknown => Ok(PType::F64),
                FloatWidth::_16 => Ok(PType::F16),
                FloatWidth::_32 => Ok(PType::F32),
                FloatWidth::_64 => Ok(PType::F64),
            },
            _ => Err(VortexError::InvalidDType(value.clone())),
        }
    }
}
