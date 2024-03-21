use std::fmt::{Display, Formatter};
use std::mem::size_of;

use half::f16;
use vortex_schema::DType;

use crate::error::{VortexError, VortexResult};
use crate::ptype::{NativePType, PType};
use crate::scalar::composite::CompositeScalar;
use crate::scalar::Scalar;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct PrimitiveScalar {
    ptype: PType,
    value: Option<PScalar>,
    exponent: u8,
}

impl PrimitiveScalar {
    pub fn new(ptype: PType, value: Option<PScalar>) -> Self {
        Self {
            ptype,
            value,
            exponent: 0,
        }
    }

    pub fn some(value: PScalar) -> Self {
        Self {
            ptype: value.ptype(),
            value: Some(value),
            exponent: 0,
        }
    }

    pub fn none(ptype: PType) -> Self {
        Self {
            ptype,
            value: None,
            exponent: 0,
        }
    }

    #[inline]
    pub fn value(&self) -> Option<PScalar> {
        self.value
    }

    #[inline]
    pub fn factor(&self) -> u8 {
        self.exponent
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.ptype
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        self.ptype.into()
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Scalar> {
        let ptype: VortexResult<PType> = dtype.try_into();
        ptype
            .and_then(|p| match self.value() {
                None => Ok(PrimitiveScalar::none(p).into()),
                Some(ps) => ps.cast_ptype(p),
            })
            .or_else(|_| self.cast_dtype(dtype))
    }

    // General conversion function that handles casting primitive scalar to non-primitive.
    // TODO(robert): Implement storage conversions
    fn cast_dtype(&self, dtype: &DType) -> VortexResult<Scalar> {
        Ok(CompositeScalar::new(dtype.clone(), Box::new(self.clone().into())).into())
    }

    pub fn nbytes(&self) -> usize {
        size_of::<Self>()
    }
}

impl Display for PrimitiveScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value() {
            None => write!(f, "<none>({}?)", self.ptype),
            Some(v) => write!(f, "{}({})", v, self.ptype),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub enum PScalar {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F16(f16),
    F32(f32),
    F64(f64),
}

impl PScalar {
    pub fn ptype(&self) -> PType {
        match self {
            PScalar::U8(_) => PType::U8,
            PScalar::U16(_) => PType::U16,
            PScalar::U32(_) => PType::U32,
            PScalar::U64(_) => PType::U64,
            PScalar::I8(_) => PType::I8,
            PScalar::I16(_) => PType::I16,
            PScalar::I32(_) => PType::I32,
            PScalar::I64(_) => PType::I64,
            PScalar::F16(_) => PType::F16,
            PScalar::F32(_) => PType::F32,
            PScalar::F64(_) => PType::F64,
        }
    }

    pub fn cast_ptype(&self, ptype: PType) -> VortexResult<Scalar> {
        macro_rules! from_int {
            ($ptype:ident , $v:ident) => {
                match $ptype {
                    PType::U8 => Ok((*$v as u8).into()),
                    PType::U16 => Ok((*$v as u16).into()),
                    PType::U32 => Ok((*$v as u32).into()),
                    PType::U64 => Ok((*$v as u64).into()),
                    PType::I8 => Ok((*$v as i8).into()),
                    PType::I16 => Ok((*$v as i16).into()),
                    PType::I32 => Ok((*$v as i32).into()),
                    PType::I64 => Ok((*$v as i64).into()),
                    PType::F16 => Ok(f16::from_f32(*$v as f32).into()),
                    PType::F32 => Ok((*$v as f32).into()),
                    PType::F64 => Ok((*$v as f64).into()),
                }
            };
        }

        macro_rules! from_floating {
            ($ptype:ident , $v:ident) => {
                match $ptype {
                    PType::F16 => Ok((f16::from_f32(*$v as f32)).into()),
                    PType::F32 => Ok((*$v as f32).into()),
                    PType::F64 => Ok((*$v as f64).into()),
                    _ => Err(VortexError::InvalidDType(ptype.into())),
                }
            };
        }

        match self {
            PScalar::U8(v) => from_int!(ptype, v),
            PScalar::U16(v) => from_int!(ptype, v),
            PScalar::U32(v) => from_int!(ptype, v),
            PScalar::U64(v) => from_int!(ptype, v),
            PScalar::I8(v) => from_int!(ptype, v),
            PScalar::I16(v) => from_int!(ptype, v),
            PScalar::I32(v) => from_int!(ptype, v),
            PScalar::I64(v) => from_int!(ptype, v),
            PScalar::F16(v) => match ptype {
                PType::F16 => Ok((*v).into()),
                PType::F32 => Ok(v.to_f32().into()),
                PType::F64 => Ok(v.to_f64().into()),
                _ => Err(VortexError::InvalidDType(ptype.into())),
            },
            PScalar::F32(v) => from_floating!(ptype, v),
            PScalar::F64(v) => from_floating!(ptype, v),
        }
    }
}

#[inline]
fn is_negative<T: Default + PartialOrd>(value: T) -> bool {
    value < T::default()
}

macro_rules! pscalar {
    ($T:ty, $ptype:tt) => {
        impl From<$T> for PScalar {
            fn from(value: $T) -> Self {
                PScalar::$ptype(value)
            }
        }

        impl From<$T> for Scalar {
            fn from(value: $T) -> Self {
                PrimitiveScalar::some(PScalar::from(value)).into()
            }
        }

        impl TryFrom<&Scalar> for $T {
            type Error = VortexError;

            fn try_from(value: &Scalar) -> VortexResult<Self> {
                match value {
                    Scalar::Primitive(PrimitiveScalar {
                        value: Some(pscalar),
                        ..
                    }) => match pscalar {
                        PScalar::$ptype(v) => Ok(*v),
                        _ => Err(VortexError::InvalidDType(pscalar.ptype().into())),
                    },
                    _ => Err(VortexError::InvalidDType(value.dtype().clone())),
                }
            }
        }

        impl TryFrom<Scalar> for $T {
            type Error = VortexError;

            fn try_from(value: Scalar) -> VortexResult<Self> {
                match value {
                    Scalar::Primitive(PrimitiveScalar {
                        value: Some(pscalar),
                        ..
                    }) => match pscalar {
                        PScalar::$ptype(v) => Ok(v),
                        _ => Err(VortexError::InvalidDType(pscalar.ptype().into())),
                    },
                    _ => Err(VortexError::InvalidDType(value.dtype().clone())),
                }
            }
        }
    };
}

pscalar!(u8, U8);
pscalar!(u16, U16);
pscalar!(u32, U32);
pscalar!(u64, U64);
pscalar!(i8, I8);
pscalar!(i16, I16);
pscalar!(i32, I32);
pscalar!(i64, I64);
pscalar!(f16, F16);
pscalar!(f32, F32);
pscalar!(f64, F64);

impl<T: NativePType> From<Option<T>> for Scalar {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => PrimitiveScalar::new(T::PTYPE, None).into(),
        }
    }
}

impl From<usize> for Scalar {
    #[inline]
    fn from(value: usize) -> Self {
        PrimitiveScalar::new(PType::U64, Some(PScalar::U64(value as u64))).into()
    }
}

impl TryFrom<Scalar> for usize {
    type Error = VortexError;

    fn try_from(value: Scalar) -> VortexResult<Self> {
        macro_rules! match_each_pscalar_integer {
            ($self:expr, | $_:tt $pscalar:ident | $($body:tt)*) => ({
                macro_rules! __with_pscalar__ {( $_ $pscalar:ident ) => ( $($body)* )}
                match $self {
                    PScalar::U8(v) => __with_pscalar__! { v },
                    PScalar::U16(v) => __with_pscalar__! { v },
                    PScalar::U32(v) => __with_pscalar__! { v },
                    PScalar::U64(v) => __with_pscalar__! { v },
                    PScalar::I8(v) => __with_pscalar__! { v },
                    PScalar::I16(v) => __with_pscalar__! { v },
                    PScalar::I32(v) => __with_pscalar__! { v },
                    PScalar::I64(v) => __with_pscalar__! { v },
                    _ => Err(VortexError::InvalidDType($self.ptype().into())),
                }
            })
        }

        match value {
            Scalar::Primitive(PrimitiveScalar {
                value: Some(pscalar),
                ..
            }) => match_each_pscalar_integer!(pscalar, |$V| {
                if is_negative($V) {
                    return Err(VortexError::ComputeError("required positive integer".into()));
                }
                Ok($V as usize)
            }),
            _ => Err(VortexError::InvalidDType(value.dtype().clone())),
        }
    }
}

impl TryFrom<&Scalar> for usize {
    type Error = VortexError;

    fn try_from(value: &Scalar) -> VortexResult<Self> {
        macro_rules! match_each_pscalar_integer {
            ($self:expr, | $_:tt $pscalar:ident | $($body:tt)*) => ({
                macro_rules! __with_pscalar__ {( $_ $pscalar:ident ) => ( $($body)* )}
                match $self {
                    PScalar::U8(v) => __with_pscalar__! { v },
                    PScalar::U16(v) => __with_pscalar__! { v },
                    PScalar::U32(v) => __with_pscalar__! { v },
                    PScalar::U64(v) => __with_pscalar__! { v },
                    PScalar::I8(v) => __with_pscalar__! { v },
                    PScalar::I16(v) => __with_pscalar__! { v },
                    PScalar::I32(v) => __with_pscalar__! { v },
                    PScalar::I64(v) => __with_pscalar__! { v },
                    _ => Err(VortexError::InvalidDType($self.ptype().into())),
                }
            })
        }

        match value {
            Scalar::Primitive(PrimitiveScalar {
                value: Some(pscalar),
                ..
            }) => match_each_pscalar_integer!(pscalar, |$V| {
                if is_negative(*$V) {
                    return Err(VortexError::ComputeError("required positive integer".into()));
                }
                Ok(*$V as usize)
            }),
            _ => Err(VortexError::InvalidDType(value.dtype().clone())),
        }
    }
}

impl Display for PScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PScalar::U8(p) => Display::fmt(p, f),
            PScalar::U16(p) => Display::fmt(p, f),
            PScalar::U32(p) => Display::fmt(p, f),
            PScalar::U64(p) => Display::fmt(p, f),
            PScalar::I8(p) => Display::fmt(p, f),
            PScalar::I16(p) => Display::fmt(p, f),
            PScalar::I32(p) => Display::fmt(p, f),
            PScalar::I64(p) => Display::fmt(p, f),
            PScalar::F16(p) => Display::fmt(p, f),
            PScalar::F32(p) => Display::fmt(p, f),
            PScalar::F64(p) => Display::fmt(p, f),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::error::VortexError;
    use crate::ptype::PType;
    use crate::scalar::Scalar;
    use vortex_schema::{DType, IntWidth, Nullability, Signedness};

    #[test]
    fn into_from() {
        let scalar: Scalar = 10u16.into();
        assert_eq!(scalar.clone().try_into(), Ok(10u16));
        // All integers should be convertible to usize
        assert_eq!(scalar.try_into(), Ok(10usize));

        let scalar: Scalar = (-10i16).into();
        assert_eq!(
            scalar.try_into(),
            Err::<usize, VortexError>(VortexError::ComputeError(
                "required positive integer".into()
            ))
        );
    }

    #[test]
    fn cast() {
        let scalar: Scalar = 10u16.into();
        let u32_scalar = scalar
            .cast(&DType::Int(
                IntWidth::_32,
                Signedness::Unsigned,
                Nullability::NonNullable,
            ))
            .unwrap();
        let u32_scalar_ptype: PType = u32_scalar.dtype().try_into().unwrap();
        assert_eq!(u32_scalar_ptype, PType::U32);
    }
}
