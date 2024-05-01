use std::fmt::{Display, Formatter};
use std::sync::Arc;

use vortex_dtype::{DType, ExtDType, ExtID, ExtMetadata, Nullability};
use vortex_error::{vortex_bail, VortexResult};

use crate::Scalar;

#[derive(Debug, Clone, PartialEq)]
pub struct ExtScalar {
    dtype: DType,
    value: Option<Arc<Scalar>>,
}

impl ExtScalar {
    pub fn try_new(
        ext: ExtDType,
        nullability: Nullability,
        value: Option<Arc<Scalar>>,
    ) -> VortexResult<Self> {
        if value.is_none() && nullability == Nullability::NonNullable {
            vortex_bail!("Value cannot be None for NonNullable Scalar");
        }
        Ok(Self {
            dtype: DType::Extension(ext, nullability),
            value,
        })
    }

    pub fn null(ext: ExtDType) -> Self {
        Self::try_new(ext, Nullability::Nullable, None).expect("Incorrect nullability check")
    }

    #[inline]
    pub fn id(&self) -> &ExtID {
        self.ext_dtype().id()
    }

    #[inline]
    pub fn metadata(&self) -> Option<&ExtMetadata> {
        self.ext_dtype().metadata()
    }

    #[inline]
    pub fn ext_dtype(&self) -> &ExtDType {
        let DType::Extension(ext, _) = &self.dtype else {
            unreachable!()
        };
        ext
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn value(&self) -> Option<&Arc<Scalar>> {
        self.value.as_ref()
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }

    pub fn nbytes(&self) -> usize {
        todo!()
    }
}

impl PartialOrd for ExtScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if let (Some(s), Some(o)) = (self.value(), other.value()) {
            s.partial_cmp(o)
        } else {
            None
        }
    }
}

impl Display for ExtScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({})",
            self.value()
                .map(|s| format!("{}", s))
                .unwrap_or_else(|| "<null>".to_string()),
            self.dtype
        )
    }
}
