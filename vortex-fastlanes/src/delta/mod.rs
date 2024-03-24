use std::sync::{Arc, RwLock};

use vortex::array::{Array, ArrayRef, Encoding, EncodingId, EncodingRef};
use vortex::compress::EncodingCompression;
use vortex::compute::scalar_at::scalar_at;
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};
use vortex::{impl_array, match_each_integer_ptype};
use vortex_schema::DType;

mod compress;
mod compute;
mod serde;

#[derive(Debug, Clone)]
pub struct DeltaArray {
    len: usize,
    bases: ArrayRef,
    deltas: ArrayRef,
    validity: Option<ArrayRef>,
    stats: Arc<RwLock<StatsSet>>,
}

impl DeltaArray {
    pub fn try_new(
        len: usize,
        bases: ArrayRef,
        deltas: ArrayRef,
        validity: Option<ArrayRef>,
    ) -> VortexResult<Self> {
        if bases.dtype() != deltas.dtype() {
            return Err(VortexError::InvalidArgument(
                format!(
                    "DeltaArray: bases and deltas must have the same dtype, got {:?} and {:?}",
                    bases.dtype(),
                    deltas.dtype()
                )
                .into(),
            ));
        }
        if deltas.len() != len {
            return Err(VortexError::InvalidArgument(
                format!(
                    "DeltaArray: provided deltas array of len {} does not match array len {}",
                    deltas.len(),
                    len
                )
                .into(),
            ));
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
            return Err(VortexError::InvalidArgument(
                format!(
                    "DeltaArray: bases.len() ({}) != expected_bases_len ({}), based on len ({}) and lane count ({})",
                    delta.bases.len(),
                    expected_bases_len,
                    len,
                    delta.lanes()
                )
                .into(),
            ));
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
            <$T as fastlanez_sys::Delta>::lanes()
        })
    }

    #[inline]
    pub fn validity(&self) -> Option<&ArrayRef> {
        self.validity.as_ref()
    }

    pub fn is_valid(&self, index: usize) -> bool {
        self.validity()
            .map(|v| scalar_at(v, index).and_then(|v| v.try_into()).unwrap())
            .unwrap_or(true)
    }
}

impl Array for DeltaArray {
    impl_array!();

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
        f.maybe_child("validity", self.validity())
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
