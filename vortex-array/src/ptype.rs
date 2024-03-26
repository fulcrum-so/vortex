use std::fmt::{Debug, Display, Formatter};
use std::panic::RefUnwindSafe;

use arrow_buffer::ArrowNativeType;
use half::f16;
use num_traits::{Num, NumCast};

use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType::*;
use vortex_schema::{DType, FloatWidth, IntWidth};

use crate::scalar::{PScalar, Scalar};

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
    + Into<Scalar>
    + TryFrom<Scalar, Error = VortexError>
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

impl PType {
    pub const fn is_unsigned_int(self) -> bool {
        matches!(self, PType::U8 | PType::U16 | PType::U32 | PType::U64)
    }

    pub const fn is_signed_int(self) -> bool {
        matches!(self, PType::I8 | PType::I16 | PType::I32 | PType::I64)
    }

    pub const fn is_int(self) -> bool {
        self.is_unsigned_int() || self.is_signed_int()
    }

    pub const fn is_float(self) -> bool {
        matches!(self, PType::F16 | PType::F32 | PType::F64)
    }

    pub const fn byte_width(&self) -> usize {
        match_each_native_ptype!(self, |$T| std::mem::size_of::<$T>())
    }

    pub const fn bit_width(&self) -> usize {
        self.byte_width() * 8
    }

    pub fn to_signed(self) -> PType {
        match self {
            PType::U8 => PType::I8,
            PType::U16 => PType::I16,
            PType::U32 => PType::I32,
            PType::U64 => PType::I64,
            _ => self,
        }
    }

    pub fn to_unsigned(self) -> PType {
        match self {
            PType::I8 => PType::U8,
            PType::I16 => PType::U16,
            PType::I32 => PType::U32,
            PType::I64 => PType::U64,
            _ => self,
        }
    }
}

impl Display for PType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PType::U8 => write!(f, "u8"),
            PType::U16 => write!(f, "u16"),
            PType::U32 => write!(f, "u32"),
            PType::U64 => write!(f, "u64"),
            PType::I8 => write!(f, "i8"),
            PType::I16 => write!(f, "i16"),
            PType::I32 => write!(f, "i32"),
            PType::I64 => write!(f, "i64"),
            PType::F16 => write!(f, "f16"),
            PType::F32 => write!(f, "f32"),
            PType::F64 => write!(f, "f64"),
        }
    }
}

impl TryFrom<&DType> for PType {
    type Error = VortexError;

    fn try_from(value: &DType) -> VortexResult<Self> {
        use vortex_schema::Signedness::*;
        match value {
            Int(w, s, _) => match (w, s) {
                (IntWidth::Unknown, Unknown | Signed) => Ok(PType::I64),
                (IntWidth::_8, Unknown | Signed) => Ok(PType::I8),
                (IntWidth::_16, Unknown | Signed) => Ok(PType::I16),
                (IntWidth::_32, Unknown | Signed) => Ok(PType::I32),
                (IntWidth::_64, Unknown | Signed) => Ok(PType::I64),
                (IntWidth::Unknown, Unsigned) => Ok(PType::U64),
                (IntWidth::_8, Unsigned) => Ok(PType::U8),
                (IntWidth::_16, Unsigned) => Ok(PType::U16),
                (IntWidth::_32, Unsigned) => Ok(PType::U32),
                (IntWidth::_64, Unsigned) => Ok(PType::U64),
            },
            Float(f, _) => match f {
                FloatWidth::Unknown => Ok(PType::F64),
                FloatWidth::_16 => Ok(PType::F16),
                FloatWidth::_32 => Ok(PType::F32),
                FloatWidth::_64 => Ok(PType::F64),
            },
            _ => Err(VortexError::InvalidArgument(
                format!("Cannot convert DType {} into PType", value.clone()).into(),
            )),
        }
    }
}

impl From<PType> for &DType {
    fn from(item: PType) -> Self {
        use vortex_schema::Nullability::*;
        use vortex_schema::Signedness::*;

        match item {
            PType::I8 => &Int(IntWidth::_8, Signed, NonNullable),
            PType::I16 => &Int(IntWidth::_16, Signed, NonNullable),
            PType::I32 => &Int(IntWidth::_32, Signed, NonNullable),
            PType::I64 => &Int(IntWidth::_64, Signed, NonNullable),
            PType::U8 => &Int(IntWidth::_8, Unsigned, NonNullable),
            PType::U16 => &Int(IntWidth::_16, Unsigned, NonNullable),
            PType::U32 => &Int(IntWidth::_32, Unsigned, NonNullable),
            PType::U64 => &Int(IntWidth::_64, Unsigned, NonNullable),
            PType::F16 => &Float(FloatWidth::_16, NonNullable),
            PType::F32 => &Float(FloatWidth::_32, NonNullable),
            PType::F64 => &Float(FloatWidth::_64, NonNullable),
        }
    }
}

impl From<PType> for DType {
    fn from(item: PType) -> Self {
        use vortex_schema::Nullability::*;
        use vortex_schema::Signedness::*;

        match item {
            PType::I8 => Int(IntWidth::_8, Signed, NonNullable),
            PType::I16 => Int(IntWidth::_16, Signed, NonNullable),
            PType::I32 => Int(IntWidth::_32, Signed, NonNullable),
            PType::I64 => Int(IntWidth::_64, Signed, NonNullable),
            PType::U8 => Int(IntWidth::_8, Unsigned, NonNullable),
            PType::U16 => Int(IntWidth::_16, Unsigned, NonNullable),
            PType::U32 => Int(IntWidth::_32, Unsigned, NonNullable),
            PType::U64 => Int(IntWidth::_64, Unsigned, NonNullable),
            PType::F16 => Float(FloatWidth::_16, NonNullable),
            PType::F32 => Float(FloatWidth::_32, NonNullable),
            PType::F64 => Float(FloatWidth::_64, NonNullable),
        }
    }
}
