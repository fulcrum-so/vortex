use std::sync::{Arc, RwLock};

use vortex::array::validity::Validity;
use vortex::array::{check_slice_bounds, Array, ArrayRef};
use vortex::compress::EncodingCompression;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};
use vortex::{impl_array, impl_array_compute, ArrayWalker};
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{DType, Signedness};

#[derive(Debug, Clone)]
pub struct DictArray {
    codes: ArrayRef,
    values: ArrayRef,
    stats: Arc<RwLock<StatsSet>>,
}

impl DictArray {
    pub fn new(codes: ArrayRef, dict: ArrayRef) -> Self {
        Self::try_new(codes, dict).unwrap()
    }

    pub fn try_new(codes: ArrayRef, dict: ArrayRef) -> VortexResult<Self> {
        if !matches!(codes.dtype(), DType::Int(_, Signedness::Unsigned, _)) {
            vortex_bail!(MismatchedTypes: "unsigned int", codes.dtype());
        }
        Ok(Self {
            codes,
            values: dict,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    #[inline]
    pub fn codes(&self) -> &ArrayRef {
        &self.codes
    }
}

impl Array for DictArray {
    impl_array!();
    impl_array_compute!();

    fn len(&self) -> usize {
        self.codes.len()
    }

    fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }

    fn dtype(&self) -> &DType {
        self.values.dtype()
    }

    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    // TODO(robert): Add function to trim the dictionary
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;
        Ok(Self::new(self.codes().slice(start, stop)?, self.values.clone()).into_array())
    }

    fn encoding(&self) -> EncodingRef {
        &DictEncoding
    }

    fn nbytes(&self) -> usize {
        self.codes().nbytes() + self.values().nbytes()
    }

    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn validity(&self) -> Option<Validity> {
        todo!()
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.values())?;
        walker.visit_child(self.codes())
    }
}

impl ArrayDisplay for DictArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("values", self.values())?;
        f.child("codes", self.codes())
    }
}

#[derive(Debug)]
pub struct DictEncoding;

impl DictEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.dict");
}

impl Encoding for DictEncoding {
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
