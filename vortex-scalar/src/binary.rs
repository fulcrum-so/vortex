use std::fmt::{Display, Formatter};

use vortex_dtype::DType;
use vortex_dtype::Nullability::{NonNullable, Nullable};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::value::ScalarValue;
use crate::Scalar;

pub type BinaryScalar = ScalarValue<Vec<u8>>;

impl BinaryScalar {
    #[inline]
    pub fn dtype(&self) -> &DType {
        match self.nullability() {
            NonNullable => &DType::Binary(NonNullable),
            Nullable => &DType::Binary(Nullable),
        }
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }

    pub fn nbytes(&self) -> usize {
        self.value().map(|s| s.len()).unwrap_or(1)
    }
}

impl From<Vec<u8>> for Scalar {
    fn from(value: Vec<u8>) -> Self {
        BinaryScalar::some(value).into()
    }
}

impl From<&[u8]> for Scalar {
    fn from(value: &[u8]) -> Self {
        BinaryScalar::some(value.to_vec()).into()
    }
}

impl TryFrom<Scalar> for Vec<u8> {
    type Error = VortexError;

    fn try_from(value: Scalar) -> VortexResult<Self> {
        let Scalar::Binary(b) = value else {
            vortex_bail!(MismatchedTypes: "binary", value.dtype());
        };
        b.into_value()
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
    }
}

impl TryFrom<&Scalar> for Vec<u8> {
    type Error = VortexError;

    fn try_from(value: &Scalar) -> VortexResult<Self> {
        let Scalar::Binary(b) = value else {
            vortex_bail!(MismatchedTypes: "binary", value.dtype());
        };
        b.value()
            .cloned()
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
    }
}

impl Display for BinaryScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value() {
            None => write!(f, "bytes[none]"),
            Some(b) => write!(f, "bytes[{}]", b.len()),
        }
    }
}
