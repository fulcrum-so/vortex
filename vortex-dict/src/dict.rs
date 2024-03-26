use std::sync::{Arc, RwLock};

use vortex::array::{check_slice_bounds, Array, ArrayRef, Encoding, EncodingId, EncodingRef};
use vortex::compress::EncodingCompression;
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::impl_array;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};
use vortex::validity::{ArrayValidity, Validity};
use vortex_error::{VortexError, VortexResult};
use vortex_schema::{DType, Signedness};

#[derive(Debug, Clone)]
pub struct DictArray {
    codes: ArrayRef,
    dict: ArrayRef,
    stats: Arc<RwLock<StatsSet>>,
}

impl DictArray {
    pub fn new(codes: ArrayRef, dict: ArrayRef) -> Self {
        Self::try_new(codes, dict).unwrap()
    }

    pub fn try_new(codes: ArrayRef, dict: ArrayRef) -> VortexResult<Self> {
        if !matches!(codes.dtype(), DType::Int(_, Signedness::Unsigned, _)) {
            return Err(VortexError::InvalidDType(codes.dtype().clone()));
        }
        Ok(Self {
            codes,
            dict,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn dict(&self) -> &ArrayRef {
        &self.dict
    }

    #[inline]
    pub fn codes(&self) -> &ArrayRef {
        &self.codes
    }
}

impl Array for DictArray {
    impl_array!();

    fn len(&self) -> usize {
        self.codes.len()
    }

    fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }

    fn dtype(&self) -> &DType {
        self.dict.dtype()
    }

    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    // TODO(robert): Add function to trim the dictionary
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;
        Ok(Self::new(self.codes().slice(start, stop)?, self.dict.clone()).into_array())
    }

    fn encoding(&self) -> EncodingRef {
        &DictEncoding
    }

    fn nbytes(&self) -> usize {
        self.codes().nbytes() + self.dict().nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl ArrayDisplay for DictArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("values", self.dict())?;
        f.child("codes", self.codes())
    }
}

impl ArrayValidity for DictArray {
    fn validity(&self) -> Option<Validity> {
        todo!()
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
