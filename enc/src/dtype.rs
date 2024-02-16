use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use itertools::Itertools;

use DType::*;

use crate::ptype::PType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum Nullability {
    NonNullable,
    Nullable,
}

impl From<bool> for Nullability {
    fn from(value: bool) -> Self {
        if value {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        }
    }
}

impl Display for Nullability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Nullability::NonNullable => write!(f, ""),
            Nullability::Nullable => write!(f, "?"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum Signedness {
    Unknown,
    Unsigned,
    Signed,
}

impl From<bool> for Signedness {
    fn from(value: bool) -> Self {
        if value {
            Signedness::Signed
        } else {
            Signedness::Unsigned
        }
    }
}

impl Display for Signedness {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Signedness::Unknown => write!(f, "unknown"),
            Signedness::Unsigned => write!(f, "unsigned"),
            Signedness::Signed => write!(f, "signed"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum IntWidth {
    Unknown,
    _8,
    _16,
    _32,
    _64,
}

impl From<i8> for IntWidth {
    fn from(item: i8) -> Self {
        match item {
            8 => IntWidth::_8,
            16 => IntWidth::_16,
            32 => IntWidth::_32,
            64 => IntWidth::_64,
            _ => IntWidth::Unknown,
        }
    }
}

impl Display for IntWidth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IntWidth::Unknown => write!(f, "_"),
            IntWidth::_8 => write!(f, "8"),
            IntWidth::_16 => write!(f, "16"),
            IntWidth::_32 => write!(f, "32"),
            IntWidth::_64 => write!(f, "64"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum FloatWidth {
    Unknown,
    _16,
    _32,
    _64,
}

impl From<i8> for FloatWidth {
    fn from(item: i8) -> Self {
        match item {
            16 => FloatWidth::_16,
            32 => FloatWidth::_32,
            64 => FloatWidth::_64,
            _ => FloatWidth::Unknown,
        }
    }
}

impl Display for FloatWidth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FloatWidth::Unknown => write!(f, "_"),
            FloatWidth::_16 => write!(f, "16"),
            FloatWidth::_32 => write!(f, "32"),
            FloatWidth::_64 => write!(f, "64"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum TimeUnit {
    Ns,
    Us,
    Ms,
    S,
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeUnit::Ns => write!(f, "ns"),
            TimeUnit::Us => write!(f, "us"),
            TimeUnit::Ms => write!(f, "ms"),
            TimeUnit::S => write!(f, "s"),
        }
    }
}

pub type FieldNames = Vec<Arc<String>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum DType {
    Null,
    Bool(Nullability),
    Int(IntWidth, Signedness, Nullability),
    Decimal(u8, i8, Nullability),
    Float(FloatWidth, Nullability),
    Utf8(Nullability),
    Binary(Nullability),
    LocalTime(TimeUnit, Nullability),
    LocalDate(Nullability),
    Instant(TimeUnit, Nullability),
    ZonedDateTime(TimeUnit, Nullability),
    Struct(FieldNames, Vec<DType>),
    List(Box<DType>, Nullability),
    Map(Box<DType>, Box<DType>, Nullability),
}

impl DType {
    pub fn is_nullable(&self) -> bool {
        use Nullability::*;

        match self {
            Null => true,
            Bool(n) => matches!(n, Nullable),
            Int(_, _, n) => matches!(n, Nullable),
            Decimal(_, _, n) => matches!(n, Nullable),
            Float(_, n) => matches!(n, Nullable),
            Utf8(n) => matches!(n, Nullable),
            Binary(n) => matches!(n, Nullable),
            LocalTime(_, n) => matches!(n, Nullable),
            LocalDate(n) => matches!(n, Nullable),
            Instant(_, n) => matches!(n, Nullable),
            ZonedDateTime(_, n) => matches!(n, Nullable),
            Struct(_, fs) => fs.iter().all(|f| f.is_nullable()),
            List(_, n) => matches!(n, Nullable),
            Map(_, _, n) => matches!(n, Nullable),
        }
    }

    pub fn as_nullable(&self) -> Self {
        use Nullability::*;
        match self {
            Null => Null,
            Bool(_) => Bool(Nullable),
            Int(w, s, _) => Int(*w, *s, Nullable),
            Decimal(s, p, _) => Decimal(*s, *p, Nullable),
            Float(w, _) => Float(*w, Nullable),
            Utf8(_) => Utf8(Nullable),
            Binary(_) => Binary(Nullable),
            LocalTime(u, _) => LocalTime(*u, Nullable),
            LocalDate(_) => LocalDate(Nullable),
            Instant(u, _) => Instant(*u, Nullable),
            ZonedDateTime(u, _) => ZonedDateTime(*u, Nullable),
            Struct(n, fs) => Struct(n.clone(), fs.iter().map(|f| f.as_nullable()).collect()),
            List(c, _) => List(c.clone(), Nullable),
            Map(k, v, _) => Map(k.clone(), v.clone(), Nullable),
        }
    }

    pub fn eq_ignore_nullability(&self, other: &Self) -> bool {
        self.as_nullable().eq(&other.as_nullable())
    }
}

impl Display for DType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Signedness::*;
        match self {
            Null => write!(f, "null"),
            Bool(n) => write!(f, "bool{}", n),
            Int(w, s, n) => match s {
                Unknown => write!(f, "int({}){}", w, n),
                Unsigned => write!(f, "unsigned_int({})", w),
                Signed => write!(f, "signed_int({})", w),
            },
            Decimal(p, s, n) => write!(f, "decimal({}, {}){}", p, s, n),
            Float(w, n) => write!(f, "float({}){}", w, n),
            Utf8(n) => write!(f, "utf8{}", n),
            Binary(n) => write!(f, "binary{}", n),
            LocalTime(u, n) => write!(f, "localtime({}){}", u, n),
            LocalDate(n) => write!(f, "localdate{}", n),
            Instant(u, n) => write!(f, "instant({}){}", u, n),
            ZonedDateTime(u, n) => write!(f, "zoned_date_time({}){}", u, n),
            Struct(n, dt) => write!(
                f,
                "{{{}}}",
                n.iter()
                    .zip(dt.iter())
                    .map(|(n, dt)| format!("{}={}", n, dt))
                    .join(", ")
            ),
            List(c, n) => write!(f, "list({}){}", c, n),
            Map(k, v, n) => write!(f, "map({}, {}){}", k, v, n),
        }
    }
}

impl From<PType> for &DType {
    fn from(item: PType) -> Self {
        use Nullability::*;
        use Signedness::*;

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
        use Nullability::*;
        use Signedness::*;

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
