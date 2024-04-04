use std::sync::{Arc, RwLock};

use vortex::array::validity::Validity;
use vortex::array::{Array, ArrayRef};
use vortex::compress::EncodingCompression;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};
use vortex::{impl_array, impl_array_compute, match_each_integer_ptype, ArrayWalker};
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::DType;

mod compress;
mod compute;
mod serde;

#[derive(Debug, Clone)]
pub struct DeltaArray {
    len: usize,
    bases: ArrayRef,
    deltas: ArrayRef,
    validity: Option<Validity>,
    stats: Arc<RwLock<StatsSet>>,
}

impl DeltaArray {
    pub fn try_new(
        len: usize,
        bases: ArrayRef,
        deltas: ArrayRef,
        validity: Option<Validity>,
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

        let delta = Self {
            len,
            bases,
            deltas,
            validity,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        };

        let expected_bases_len = {
            let num_chunks = len / 1024;
            let remainder_base_size = if len % 1024 > 0 { 1 } else { 0 };
            num_chunks * delta.lanes() + remainder_base_size
        };
        if delta.bases.len() != expected_bases_len {
            vortex_bail!(
                "DeltaArray: bases.len() ({}) != expected_bases_len ({}), based on len ({}) and lane count ({})",
                delta.bases.len(),
                expected_bases_len,
                len,
                delta.lanes()
            );
        }
        Ok(delta)
    }

    #[inline]
    pub fn bases(&self) -> &ArrayRef {
        &self.bases
    }

    #[inline]
    pub fn deltas(&self) -> &ArrayRef {
        &self.deltas
    }

    #[inline]
    fn lanes(&self) -> usize {
        let ptype = self.dtype().try_into().unwrap();
        match_each_integer_ptype!(ptype, |$T| {
            <$T as fastlanez::Delta>::lanes()
        })
    }
}

impl Array for DeltaArray {
    impl_array!();
    impl_array_compute!();

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.bases.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.bases.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        unimplemented!("DeltaArray::slice")
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &DeltaEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.bases().nbytes()
            + self.deltas().nbytes()
            + self.validity().map(|v| v.nbytes()).unwrap_or(0)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn validity(&self) -> Option<Validity> {
        self.validity.clone()
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.bases())?;
        walker.visit_child(self.deltas())
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for DeltaArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for DeltaArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("bases", self.bases())?;
        f.child("deltas", self.deltas())?;
        f.validity(self.validity())
    }
}

impl StatsCompute for DeltaArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::default())
    }
}

#[derive(Debug)]
pub struct DeltaEncoding;

impl DeltaEncoding {
    pub const ID: EncodingId = EncodingId::new("fastlanes.delta");
}

impl Encoding for DeltaEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
