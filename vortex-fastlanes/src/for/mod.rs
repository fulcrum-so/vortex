use serde::{Deserialize, Serialize};
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, ArrayFlatten, ToArrayData};
use vortex_error::vortex_bail;
use vortex_scalar::Scalar;

use crate::r#for::compress::decompress;

mod compress;
mod compute;

impl_encoding!("fastlanes.for", FoR);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FoRMetadata {
    reference: Scalar,
    shift: u8,
}

impl FoRArray<'_> {
    pub fn try_new(child: Array, reference: Scalar, shift: u8) -> VortexResult<Self> {
        if reference.is_null() {
            vortex_bail!("Reference value cannot be null",);
        }
        let reference = reference.cast(child.dtype())?;
        Self::try_from_parts(
            child.dtype().clone(),
            FoRMetadata { reference, shift },
            [child.to_array_data()].into(),
            StatsSet::new(),
        )
    }

    #[inline]
    pub fn encoded(&self) -> Array {
        self.array()
            .child(0, self.dtype())
            .expect("Missing FoR child")
    }

    #[inline]
    pub fn reference(&self) -> &Scalar {
        &self.metadata().reference
    }

    #[inline]
    pub fn shift(&self) -> u8 {
        self.metadata().shift
    }
}

impl ArrayValidity for FoRArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.encoded().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.encoded().with_dyn(|a| a.logical_validity())
    }
}

impl ArrayFlatten for FoRArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        decompress(self).map(Flattened::Primitive)
    }
}

impl AcceptArrayVisitor for FoRArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("encoded", &self.encoded())
    }
}

impl ArrayStatisticsCompute for FoRArray<'_> {}

impl ArrayTrait for FoRArray<'_> {
    fn len(&self) -> usize {
        self.encoded().len()
    }

    fn nbytes(&self) -> usize {
        self.reference().nbytes() + self.encoded().nbytes()
    }
}
