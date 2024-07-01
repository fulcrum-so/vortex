pub use compress::*;
use serde::{Deserialize, Serialize};
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, Canonical, IntoCanonical};
use vortex_dtype::PType;
use vortex_error::vortex_bail;
use vortex_scalar::Scalar;

mod compress;
mod compute;

impl_encoding!("fastlanes.for", FoR);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FoRMetadata {
    reference: Scalar,
    shift: u8,
}

impl FoRArray {
    pub fn try_new(child: Array, reference: Scalar, shift: u8) -> VortexResult<Self> {
        if reference.is_null() {
            vortex_bail!("Reference value cannot be null",);
        }
        let reference = reference.cast(
            &reference
                .dtype()
                .with_nullability(child.dtype().nullability()),
        )?;
        Self::try_from_parts(
            reference.dtype().clone(),
            FoRMetadata { reference, shift },
            [child].into(),
            StatsSet::new(),
        )
    }

    #[inline]
    pub fn encoded(&self) -> Array {
        let dtype = if self.ptype().is_signed_int() {
            &DType::Primitive(self.ptype().to_unsigned(), self.dtype().nullability())
        } else {
            self.dtype()
        };
        self.array().child(0, dtype).expect("Missing FoR child")
    }

    #[inline]
    pub fn reference(&self) -> &Scalar {
        &self.metadata().reference
    }

    #[inline]
    pub fn shift(&self) -> u8 {
        self.metadata().shift
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.dtype().try_into().unwrap()
    }
}

impl ArrayValidity for FoRArray {
    fn is_valid(&self, index: usize) -> bool {
        self.encoded().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.encoded().with_dyn(|a| a.logical_validity())
    }
}

impl IntoCanonical for FoRArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        decompress(self).map(Canonical::Primitive)
    }
}

impl AcceptArrayVisitor for FoRArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("encoded", &self.encoded())
    }
}

impl ArrayStatisticsCompute for FoRArray {}

impl ArrayTrait for FoRArray {
    fn len(&self) -> usize {
        self.encoded().len()
    }

    fn nbytes(&self) -> usize {
        self.encoded().nbytes()
    }
}
