use std::fmt::{Display, Formatter};

use vortex_dtype::{DType, Nullability::NonNullable, Nullability::Nullable};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::value::ScalarValue;
use crate::Scalar;

pub type Utf8Scalar = ScalarValue<String>;

impl Utf8Scalar {
    #[inline]
    pub fn dtype(&self) -> &DType {
        match self.nullability() {
            NonNullable => &DType::Utf8(NonNullable),
            Nullable => &DType::Utf8(Nullable),
        }
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }

    pub fn nbytes(&self) -> usize {
        self.value().map(|v| v.len()).unwrap_or(0)
    }
}

impl From<String> for Scalar {
    fn from(value: String) -> Self {
        Utf8Scalar::some(value).into()
    }
}

impl From<&str> for Scalar {
    fn from(value: &str) -> Self {
        Utf8Scalar::some(value.to_string()).into()
    }
}

impl TryFrom<Scalar> for String {
    type Error = VortexError;

    fn try_from(value: Scalar) -> Result<Self, Self::Error> {
        let Scalar::Utf8(u) = value else {
            vortex_bail!(MismatchedTypes: "Utf8", value.dtype());
        };

        u.into_value()
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
    }
}

impl TryFrom<&Scalar> for String {
    type Error = VortexError;

    fn try_from(value: &Scalar) -> Result<Self, Self::Error> {
        let Scalar::Utf8(u) = value else {
            vortex_bail!(MismatchedTypes: "Utf8", value.dtype());
        };

        u.value()
            .cloned()
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
    }
}

impl Display for Utf8Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value() {
            None => write!(f, "<none>"),
            Some(v) => write!(f, "\"{}\"", v),
        }
    }
}
