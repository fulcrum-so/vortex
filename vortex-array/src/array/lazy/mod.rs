use crate::array::{Array, ArrayRef, Encoding, EncodingId, EncodingRef};
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::impl_array;
use crate::stats::{Stats, StatsCompute, StatsSet};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, OnceLock, RwLock};
use vortex_schema::DType;

pub type LazyFn = Arc<dyn Fn() -> ArrayRef + Send + Sync>;

#[derive(Clone)]
pub struct LazyArray {
    dtype: DType,
    len: usize,
    array: OnceLock<ArrayRef>,
    ctor: LazyFn,
    stats: Arc<RwLock<StatsSet>>,
}

impl LazyArray {
    pub fn new(dtype: DType, len: usize, ctor: LazyFn) -> Self {
        Self {
            dtype,
            len,
            array: OnceLock::new(),
            ctor,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    fn array(&self) -> &ArrayRef {
        self.array.get_or_init(|| (self.ctor)())
    }
}

impl Array for LazyArray {
    impl_array!();

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        self.array().slice(start, stop)
    }

    fn encoding(&self) -> EncodingRef {
        &LazyEncoding
    }

    fn nbytes(&self) -> usize {
        self.array().nbytes()
    }
}

impl ArrayCompute for LazyArray {}

impl ArrayDisplay for LazyArray {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        fmt.maybe_child("array", self.array.get())
    }
}

impl StatsCompute for LazyArray {}

impl Debug for LazyArray {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyArray")
            .field("dtype", &self.dtype)
            .field("len", &self.len)
            .field("array", &self.array.get())
            .finish()
    }
}

#[derive(Debug)]
struct LazyEncoding;

impl LazyEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.lazy");
}

impl Encoding for LazyEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }
}
