use std::any::Any;
use std::sync::{Arc, RwLock};

use vortex::array::{check_slice_bounds, Array, ArrayRef, Encoding, EncodingId};
use vortex::compress::EncodingCompression;
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};
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
    pub fn dict(&self) -> &dyn Array {
        self.dict.as_ref()
    }

    #[inline]
    pub fn codes(&self) -> &dyn Array {
        self.codes.as_ref()
    }
}

impl Array for DictArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

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
        Ok(Self::new(self.codes().slice(start, stop)?, self.dict.clone()).boxed())
    }

    fn encoding(&self) -> &'static dyn Encoding {
        &DictEncoding
    }

    fn nbytes(&self) -> usize {
        self.codes().nbytes() + self.dict().nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for DictArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for DictArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("values", self.dict())?;
        f.child("codes", self.codes())
    }
}

#[derive(Debug)]
pub struct DictEncoding;

impl DictEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.dict");
}

impl Encoding for DictEncoding {
    fn id(&self) -> &'static EncodingId {
        &Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
