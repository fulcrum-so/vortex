use std::fmt::{Debug, Display, Formatter};

pub use binary::*;
pub use bool::*;
pub use composite::*;
pub use list::*;
pub use null::*;
pub use primitive::*;
pub use serde::*;
pub use struct_::*;
pub use utf8::*;
use vortex_schema::{DType, FloatWidth, IntWidth, Signedness};

use crate::error::VortexResult;
use crate::ptype::{NativePType, PType};

mod binary;
mod bool;
mod composite;
mod list;
mod null;
mod primitive;
mod serde;
mod struct_;
mod utf8;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Scalar {
    Binary(BinaryScalar),
    Bool(BoolScalar),
    List(ListScalar),
    Null(NullScalar),
    Primitive(PrimitiveScalar),
    Struct(StructScalar),
    Utf8(Utf8Scalar),
    Composite(CompositeScalar),
}

macro_rules! impls_for_scalars {
    ($variant:tt, $E:ty) => {
        impl From<$E> for Scalar {
            fn from(arr: $E) -> Self {
                Self::$variant(arr)
            }
        }
    };
}

impls_for_scalars!(Binary, BinaryScalar);
impls_for_scalars!(Bool, BoolScalar);
impls_for_scalars!(List, ListScalar);
impls_for_scalars!(Null, NullScalar);
impls_for_scalars!(Primitive, PrimitiveScalar);
impls_for_scalars!(Struct, StructScalar);
impls_for_scalars!(Utf8, Utf8Scalar);
impls_for_scalars!(Composite, CompositeScalar);

macro_rules! match_each_scalar {
    ($self:expr, | $_:tt $scalar:ident | $($body:tt)*) => ({
        macro_rules! __with_scalar__ {( $_ $scalar:ident ) => ( $($body)* )}
        match $self {
            Scalar::Binary(s) => __with_scalar__! { s },
            Scalar::Bool(s) => __with_scalar__! { s },
            Scalar::List(s) => __with_scalar__! { s },
            Scalar::Null(s) => __with_scalar__! { s },
            Scalar::Primitive(s) => __with_scalar__! { s },
            Scalar::Struct(s) => __with_scalar__! { s },
            Scalar::Utf8(s) => __with_scalar__! { s },
            Scalar::Composite(s) => __with_scalar__! { s },
        }
    })
}

impl Scalar {
    pub fn dtype(&self) -> &DType {
        match_each_scalar! { self, |$s| $s.dtype() }
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Self> {
        match_each_scalar! { self, |$s| $s.cast(dtype) }
    }

    pub fn nbytes(&self) -> usize {
        match_each_scalar! { self, |$s| $s.nbytes() }
    }

    pub fn null(dtype: &DType) -> Self {
        match dtype {
            DType::Null => NullScalar::new().into(),
            DType::Bool(_) => BoolScalar::new(None).into(),
            DType::Int(w, s, _) => match (w, s) {
                (IntWidth::Unknown, Signedness::Unknown | Signedness::Signed) => {
                    PrimitiveScalar::none(PType::I64).into()
                }
                (IntWidth::_8, Signedness::Unknown | Signedness::Signed) => {
                    PrimitiveScalar::none(PType::I8).into()
                }
                (IntWidth::_16, Signedness::Unknown | Signedness::Signed) => {
                    PrimitiveScalar::none(PType::I16).into()
                }
                (IntWidth::_32, Signedness::Unknown | Signedness::Signed) => {
                    PrimitiveScalar::none(PType::I32).into()
                }
                (IntWidth::_64, Signedness::Unknown | Signedness::Signed) => {
                    PrimitiveScalar::none(PType::I64).into()
                }
                (IntWidth::Unknown, Signedness::Unsigned) => {
                    PrimitiveScalar::none(PType::U64).into()
                }
                (IntWidth::_8, Signedness::Unsigned) => PrimitiveScalar::none(PType::U8).into(),
                (IntWidth::_16, Signedness::Unsigned) => PrimitiveScalar::none(PType::U16).into(),
                (IntWidth::_32, Signedness::Unsigned) => PrimitiveScalar::none(PType::U32).into(),
                (IntWidth::_64, Signedness::Unsigned) => PrimitiveScalar::none(PType::U64).into(),
            },
            DType::Decimal(_, _, _) => unimplemented!("DecimalScalar"),
            DType::Float(w, _) => match w {
                FloatWidth::Unknown => PrimitiveScalar::none(PType::F64).into(),
                FloatWidth::_16 => PrimitiveScalar::none(PType::F16).into(),
                FloatWidth::_32 => PrimitiveScalar::none(PType::F32).into(),
                FloatWidth::_64 => PrimitiveScalar::none(PType::F64).into(),
            },
            DType::Utf8(_) => Utf8Scalar::new(None).into(),
            DType::Binary(_) => BinaryScalar::new(None).into(),
            DType::Struct(_, _) => StructScalar::new(dtype.clone(), vec![]).into(),
            DType::List(_, _) => ListScalar::new(dtype.clone(), None).into(),
            DType::Composite(_, _) => unimplemented!("CompositeScalar"),
        }
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match_each_scalar! { self, |$s| Display::fmt($s, f) }
    }
}

/// Allows conversion from Enc scalars to a byte slice.
pub trait AsBytes {
    /// Converts this instance into a byte slice
    fn as_bytes(&self) -> &[u8];
}

impl<T: NativePType> AsBytes for [T] {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        let raw_ptr = self.as_ptr() as *const u8;
        unsafe { std::slice::from_raw_parts(raw_ptr, std::mem::size_of_val(self)) }
    }
}

impl<T: NativePType> AsBytes for &[T] {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        let raw_ptr = (*self).as_ptr() as *const u8;
        unsafe { std::slice::from_raw_parts(raw_ptr, std::mem::size_of_val(*self)) }
    }
}

impl<T: NativePType> AsBytes for T {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        let raw_ptr = self as *const T as *const u8;
        unsafe { std::slice::from_raw_parts(raw_ptr, std::mem::size_of::<T>()) }
    }
}

#[cfg(test)]
mod test {
    use std::mem;

    use crate::scalar::Scalar;

    #[test]
    fn size_of() {
        assert_eq!(mem::size_of::<Scalar>(), 80);
    }
}
