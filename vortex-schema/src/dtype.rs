use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use itertools::Itertools;

use DType::*;

use crate::CompositeID;

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

impl From<u16> for IntWidth {
    fn from(item: u16) -> Self {
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

pub type FieldNames = Vec<Arc<String>>;

pub type Metadata = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum DType {
    Null,
    Bool(Nullability),
    Int(IntWidth, Signedness, Nullability),
    Decimal(u8, i8, Nullability),
    Float(FloatWidth, Nullability),
    Utf8(Nullability),
    Binary(Nullability),
    Struct(FieldNames, Vec<DType>),
    List(Box<DType>, Nullability),
    Composite(CompositeID, Nullability),
}

impl DType {
    /// The default DType for indices
    pub const IDX: DType = Int(
        IntWidth::_64,
        Signedness::Unsigned,
        Nullability::NonNullable,
    );

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
            Struct(_, fs) => fs.iter().all(|f| f.is_nullable()),
            List(_, n) => matches!(n, Nullable),
            Composite(_, n) => matches!(n, Nullable),
        }
    }

    pub fn as_nonnullable(&self) -> Self {
        self.with_nullability(Nullability::NonNullable)
    }

    pub fn as_nullable(&self) -> Self {
        self.with_nullability(Nullability::Nullable)
    }

    pub fn with_nullability(&self, nullability: Nullability) -> Self {
        match self {
            Null => Null,
            Bool(_) => Bool(nullability),
            Int(w, s, _) => Int(*w, *s, nullability),
            Decimal(s, p, _) => Decimal(*s, *p, nullability),
            Float(w, _) => Float(*w, nullability),
            Utf8(_) => Utf8(nullability),
            Binary(_) => Binary(nullability),
            Struct(n, fs) => Struct(
                n.clone(),
                fs.iter().map(|f| f.with_nullability(nullability)).collect(),
            ),
            List(c, _) => List(c.clone(), nullability),
            Composite(id, _) => Composite(*id, nullability),
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
            Struct(n, dt) => write!(
                f,
                "{{{}}}",
                n.iter()
                    .zip(dt.iter())
                    .map(|(n, dt)| format!("{}={}", n, dt))
                    .join(", ")
            ),
            List(c, n) => write!(f, "list({}){}", c, n),
            Composite(id, n) => write!(f, "<{}>{}", id, n),
        }
    }
}

#[cfg(test)]
mod test {
    use std::mem;

    use crate::dtype::DType;

    #[test]
    fn size_of() {
        assert_eq!(mem::size_of::<DType>(), 48);
    }
}
