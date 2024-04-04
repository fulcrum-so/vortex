use std::sync::{Arc, RwLock};

use vortex::array::{Array, ArrayRef};
use vortex::compress::EncodingCompression;
use vortex::compute::ArrayCompute;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::scalar::Scalar;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};
use vortex::validity::ArrayValidity;
use vortex::validity::Validity;
use vortex::{impl_array, ArrayWalker};
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::DType;

mod compress;
mod compute;
mod serde;

#[derive(Debug, Clone)]
pub struct FoRArray {
    encoded: ArrayRef,
    reference: Scalar,
    shift: u8,
    stats: Arc<RwLock<StatsSet>>,
}

impl FoRArray {
    pub fn try_new(child: ArrayRef, reference: Scalar, shift: u8) -> VortexResult<Self> {
        if reference.is_null() {
            vortex_bail!("Reference value cannot be null",);
        }
        let reference = reference.cast(child.dtype())?;
        Ok(Self {
            encoded: child,
            reference,
            shift,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn encoded(&self) -> &ArrayRef {
        &self.encoded
    }

    #[inline]
    pub fn reference(&self) -> &Scalar {
        &self.reference
    }

    #[inline]
    pub fn shift(&self) -> u8 {
        self.shift
    }
}

impl OwnedArray for FoRArray {
    impl_array!();
}

impl Array for FoRArray {
    fn to_array(&self) -> ArrayRef {
        self.clone().into_array()
    }

    #[inline]
    fn len(&self) -> usize {
        self.encoded.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.encoded.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.encoded.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(Self {
            encoded: self.encoded.slice(start, stop)?,
            reference: self.reference.clone(),
            shift: self.shift,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &FoREncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.encoded.nbytes() + self.reference.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.encoded())
    }

    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }
}

impl ArrayValidity for FoRArray {
    fn logical_validity(&self) -> Validity {
        self.encoded().logical_validity()
    }

    fn is_valid(&self, index: usize) -> bool {
        self.encoded().is_valid(index)
    }
}

impl ArrayDisplay for FoRArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("reference", self.reference())?;
        f.property("shift", self.shift())?;
        f.child("encoded", self.encoded())
    }
}

impl StatsCompute for FoRArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::default())
    }
}

#[derive(Debug)]
pub struct FoREncoding;

impl FoREncoding {
    pub const ID: EncodingId = EncodingId::new("fastlanes.for");
}

impl Encoding for FoREncoding {
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
