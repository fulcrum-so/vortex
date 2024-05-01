use std::fmt::{Debug, Display, Formatter};
use std::panic::RefUnwindSafe;

use num_traits::{FromPrimitive, Num, NumCast};
use vortex_error::{vortex_err, VortexError, VortexResult};

use crate::half::f16;
use crate::DType;
use crate::DType::*;
use crate::Nullability::NonNullable;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
    + Clone
    + Copy
    + Debug
    + Display
    + PartialEq
    + PartialOrd
    + Default
    + RefUnwindSafe
    + Num
    + NumCast
    + FromPrimitive
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
        use $crate::PType;
        use $crate::half::f16;
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

#[macro_export]
macro_rules! match_each_integer_ptype {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use $crate::PType;
        match $self {
            PType::I8 => __with__! { i8 },
            PType::I16 => __with__! { i16 },
            PType::I32 => __with__! { i32 },
            PType::I64 => __with__! { i64 },
            PType::U8 => __with__! { u8 },
            PType::U16 => __with__! { u16 },
            PType::U32 => __with__! { u32 },
            PType::U64 => __with__! { u64 },
            _ => panic!("Unsupported ptype {}", $self),
        }
    })
}

#[macro_export]
macro_rules! match_each_float_ptype {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use $crate::PType;
        use vortex_dtype::half::f16;
        match $self {
            PType::F16 => __with__! { f16 },
            PType::F32 => __with__! { f32 },
            PType::F64 => __with__! { f64 },
            _ => panic!("Unsupported ptype {}", $self),
        }
    })
}

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
        match value {
            Primitive(p, _) => Ok(*p),
            _ => Err(vortex_err!("Cannot convert DType {} into PType", value)),
        }
    }
}

impl From<PType> for &DType {
    fn from(item: PType) -> Self {
        // We expand this match statement so that we can return a static reference.
        match item {
            PType::I8 => &Primitive(PType::I8, NonNullable),
            PType::I16 => &Primitive(PType::I16, NonNullable),
            PType::I32 => &Primitive(PType::I32, NonNullable),
            PType::I64 => &Primitive(PType::I64, NonNullable),
            PType::U8 => &Primitive(PType::U8, NonNullable),
            PType::U16 => &Primitive(PType::U16, NonNullable),
            PType::U32 => &Primitive(PType::U32, NonNullable),
            PType::U64 => &Primitive(PType::U64, NonNullable),
            PType::F16 => &Primitive(PType::F16, NonNullable),
            PType::F32 => &Primitive(PType::F32, NonNullable),
            PType::F64 => &Primitive(PType::F64, NonNullable),
        }
    }
}

impl From<PType> for DType {
    fn from(item: PType) -> Self {
        Primitive(item, NonNullable)
    }
}
