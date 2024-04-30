use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use itertools::Itertools;
use DType::*;

use crate::{CompositeID, PType};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub enum Nullability {
    #[default]
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

impl From<Nullability> for bool {
    fn from(value: Nullability) -> Self {
        match value {
            Nullability::NonNullable => false,
            Nullability::Nullable => true,
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

pub type FieldNames = Vec<Arc<String>>;

pub type Metadata = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DType {
    Null,
    Bool(Nullability),
    Primitive(PType, Nullability),
    Decimal(u8, i8, Nullability),
    Utf8(Nullability),
    Binary(Nullability),
    Struct(FieldNames, Vec<DType>),
    List(Box<DType>, Nullability),
    Composite(CompositeID, Nullability),
}

impl DType {
    pub const BYTES: DType = Primitive(PType::U8, Nullability::NonNullable);

    /// The default DType for indices
    pub const IDX: DType = Primitive(PType::U64, Nullability::NonNullable);

    pub fn nullability(&self) -> Nullability {
        self.is_nullable().into()
    }

    pub fn is_nullable(&self) -> bool {
        use Nullability::*;

        match self {
            Null => true,
            Bool(n) => matches!(n, Nullable),
            Primitive(_, n) => matches!(n, Nullable),
            Decimal(_, _, n) => matches!(n, Nullable),
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
            Primitive(p, _) => Primitive(*p, nullability),
            Decimal(s, p, _) => Decimal(*s, *p, nullability),
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

    pub fn is_unsigned_int(&self) -> bool {
        PType::try_from(self)
            .map(|ptype| ptype.is_unsigned_int())
            .unwrap_or_default()
    }

    pub fn is_signed_int(&self) -> bool {
        PType::try_from(self)
            .map(|ptype| ptype.is_signed_int())
            .unwrap_or_default()
    }

    pub fn is_int(&self) -> bool {
        PType::try_from(self)
            .map(|ptype| ptype.is_int())
            .unwrap_or_default()
    }

    pub fn is_float(&self) -> bool {
        PType::try_from(self)
            .map(|ptype| ptype.is_float())
            .unwrap_or_default()
    }
}

impl Display for DType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Null => write!(f, "null"),
            Bool(n) => write!(f, "bool{}", n),
            Primitive(p, n) => write!(f, "{}{}", p, n),
            Decimal(p, s, n) => write!(f, "decimal({}, {}){}", p, s, n),
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
