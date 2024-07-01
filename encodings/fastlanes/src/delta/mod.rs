pub use compress::*;
use serde::{Deserialize, Serialize};
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::ValidityMetadata;
use vortex::validity::{ArrayValidity, LogicalValidity, Validity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, Canonical, IntoCanonical};
use vortex_dtype::match_each_unsigned_integer_ptype;
use vortex_error::vortex_bail;

mod compress;
mod compute;

impl_encoding!("fastlanes.delta", Delta);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaMetadata {
    validity: ValidityMetadata,
    len: usize,
}

impl DeltaArray {
    pub fn try_new(
        len: usize,
        bases: Array,
        deltas: Array,
        validity: Validity,
    ) -> VortexResult<Self> {
        if bases.dtype() != deltas.dtype() {
            vortex_bail!(
                "DeltaArray: bases and deltas must have the same dtype, got {:?} and {:?}",
                bases.dtype(),
                deltas.dtype()
            );
        }
        if deltas.len() != len {
            vortex_bail!(
                "DeltaArray: provided deltas array of len {} does not match array len {}",
                deltas.len(),
                len
            );
        }

        let dtype = bases.dtype().clone();
        let metadata = DeltaMetadata {
            validity: validity.to_metadata(len)?,
            len,
        };

        let mut children = vec![bases, deltas];
        if let Some(varray) = validity.into_array() {
            children.push(varray)
        }

        let delta = Self::try_from_parts(dtype, metadata, children.into(), StatsSet::new())?;

        let expected_bases_len = {
            let num_chunks = len / 1024;
            let remainder_base_size = if len % 1024 > 0 { 1 } else { 0 };
            num_chunks * delta.lanes() + remainder_base_size
        };
        if delta.bases().len() != expected_bases_len {
            vortex_bail!(
                "DeltaArray: bases.len() ({}) != expected_bases_len ({}), based on len ({}) and lane count ({})",
                delta.bases().len(),
                expected_bases_len,
                len,
                delta.lanes()
            );
        }
        Ok(delta)
    }

    #[inline]
    pub fn bases(&self) -> Array {
        self.array().child(0, self.dtype()).expect("Missing bases")
    }

    #[inline]
    pub fn deltas(&self) -> Array {
        self.array().child(1, self.dtype()).expect("Missing deltas")
    }

    #[inline]
    fn lanes(&self) -> usize {
        let ptype = self.dtype().try_into().unwrap();
        match_each_unsigned_integer_ptype!(ptype, |$T| {
            <$T as fastlanes::FastLanes>::LANES
        })
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(2, &Validity::DTYPE))
    }
}

impl IntoCanonical for DeltaArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        delta_decompress(self).map(Canonical::Primitive)
    }
}

impl ArrayValidity for DeltaArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for DeltaArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("bases", &self.bases())?;
        visitor.visit_child("deltas", &self.deltas())
    }
}

impl ArrayStatisticsCompute for DeltaArray {}

impl ArrayTrait for DeltaArray {
    fn len(&self) -> usize {
        self.metadata().len
    }
}
