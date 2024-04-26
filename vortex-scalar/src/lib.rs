use std::fmt::{Debug, Display, Formatter};

pub use binary::*;
pub use bool::*;
pub use composite::*;
pub use list::*;
pub use null::*;
pub use primitive::*;
pub use struct_::*;
pub use utf8::*;
use vortex_dtype::{DType, Nullability};
use vortex_dtype::NativePType;
use vortex_error::VortexResult;

mod binary;
mod bool;
mod composite;
mod list;
mod null;
mod primitive;
mod serde;
mod struct_;
mod utf8;
mod value;

pub mod flatbuffers {
    pub use gen_scalar::vortex::*;

    #[allow(clippy::all)]
    #[allow(clippy::unwrap_used)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(unsafe_op_in_unsafe_fn)]
    #[allow(unused_imports)]
    mod gen_scalar {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/scalar.rs"));
    }

    mod deps {
        pub mod dtype {
            #[allow(unused_imports)]
            pub use vortex_dtype::flatbuffers as dtype;
        }
    }
}

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

    pub fn nullability(&self) -> Nullability {
        self.dtype().nullability()
    }

    pub fn is_null(&self) -> bool {
        match self {
            Scalar::Binary(b) => b.value().is_none(),
            Scalar::Bool(b) => b.value().is_none(),
            Scalar::List(l) => l.values().is_none(),
            Scalar::Null(_) => true,
            Scalar::Primitive(p) => p.value().is_none(),
            // FIXME(ngates): can't have a null struct?
            Scalar::Struct(_) => false,
            Scalar::Utf8(u) => u.value().is_none(),
            Scalar::Composite(c) => c.scalar().is_null(),
        }
    }

    pub fn null(dtype: &DType) -> Self {
        assert!(dtype.is_nullable());
        match dtype {
            DType::Null => NullScalar::new().into(),
            DType::Bool(_) => BoolScalar::none().into(),
            DType::Primitive(p, _) => PrimitiveScalar::none_from_ptype(*p).into(),
            DType::Decimal(..) => unimplemented!("DecimalScalar"),
            DType::Utf8(_) => Utf8Scalar::none().into(),
            DType::Binary(_) => BinaryScalar::none().into(),
            DType::Struct(..) => StructScalar::new(dtype.clone(), vec![]).into(),
            DType::List(..) => ListScalar::new(dtype.clone(), None).into(),
            DType::Composite(..) => unimplemented!("CompositeScalar"),
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

    use crate::Scalar;

    #[test]
    fn size_of() {
        assert_eq!(mem::size_of::<Scalar>(), 80);
    }
}
