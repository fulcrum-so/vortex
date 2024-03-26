use std::fmt::{Display, Formatter};

use itertools::Itertools;

use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

use crate::scalar::Scalar;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct ListScalar {
    dtype: DType,
    values: Option<Vec<Scalar>>,
}

impl ListScalar {
    #[inline]
    pub fn new(dtype: DType, values: Option<Vec<Scalar>>) -> Self {
        Self { dtype, values }
    }

    #[inline]
    pub fn values(&self) -> Option<&[Scalar]> {
        self.values.as_deref()
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Scalar> {
        match dtype {
            DType::List(field_dtype, n) => {
                let new_fields: Option<Vec<Scalar>> = self
                    .values()
                    .map(|v| v.iter().map(|field| field.cast(field_dtype)).try_collect())
                    .transpose()?;

                let new_type = if let Some(nf) = new_fields.as_ref() {
                    if nf.is_empty() {
                        dtype.clone()
                    } else {
                        DType::List(Box::new(nf[0].dtype().clone()), *n)
                    }
                } else {
                    dtype.clone()
                };
                Ok(ListScalar::new(new_type, new_fields).into())
            }
            _ => Err(VortexError::InvalidDType(dtype.clone())),
        }
    }

    pub fn nbytes(&self) -> usize {
        self.values()
            .map(|v| v.iter().map(|s| s.nbytes()).sum())
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListScalarVec<T>(pub Vec<T>);

impl<T: Into<Scalar>> From<ListScalarVec<T>> for Scalar {
    fn from(value: ListScalarVec<T>) -> Self {
        let values: Vec<Scalar> = value.0.into_iter().map(|v| v.into()).collect();
        if values.is_empty() {
            panic!("Can't implicitly convert empty list into ListScalar");
        }
        ListScalar::new(values[0].dtype().clone(), Some(values)).into()
    }
}

impl<T: TryFrom<Scalar, Error = VortexError>> TryFrom<Scalar> for ListScalarVec<T> {
    type Error = VortexError;

    fn try_from(value: Scalar) -> Result<Self, Self::Error> {
        if let Scalar::List(ls) = value {
            if let Some(vs) = ls.values {
                Ok(ListScalarVec(
                    vs.into_iter().map(|v| v.try_into()).try_collect()?,
                ))
            } else {
                Err(VortexError::InvalidDType(ls.dtype().clone()))
            }
        } else {
            Err(VortexError::InvalidDType(value.dtype().clone()))
        }
    }
}

impl<'a, T: TryFrom<&'a Scalar, Error = VortexError>> TryFrom<&'a Scalar> for ListScalarVec<T> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if let Scalar::List(ls) = value {
            if let Some(vs) = ls.values() {
                Ok(ListScalarVec(
                    vs.iter().map(|v| v.try_into()).try_collect()?,
                ))
            } else {
                Err(VortexError::InvalidDType(ls.dtype().clone()))
            }
        } else {
            Err(VortexError::InvalidDType(value.dtype().clone()))
        }
    }
}

impl Display for ListScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.values() {
            None => write!(f, "<none>"),
            Some(vs) => write!(f, "{}", vs.iter().format(", ")),
        }
    }
}
