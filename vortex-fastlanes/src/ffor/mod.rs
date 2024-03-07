use std::any::Any;
use std::sync::{Arc, RwLock};

use vortex::array::{
    check_validity_buffer, Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef,
};
use vortex::compress::EncodingCompression;
use vortex::compute::scalar_at::scalar_at;
use vortex::compute::ArrayCompute;
use vortex::dtype::DType;
use vortex::error::VortexResult;
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::scalar::{Scalar, ScalarRef};
use vortex::serde::ArraySerde;
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};

mod compress;

#[derive(Debug, Clone)]
pub struct FFoRArray {
    encoded: ArrayRef,
    validity: Option<ArrayRef>,
    patches: Option<ArrayRef>,
    bit_width: usize,
    bit_shift: usize,
    reference: Option<ScalarRef>,
    len: usize,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl FFoRArray {
    pub fn try_new(
        encoded: ArrayRef,
        validity: Option<ArrayRef>,
        patches: Option<ArrayRef>,
        bit_width: usize,
        bit_shift: usize,
        reference: Option<ScalarRef>,
        dtype: DType,
        len: usize,
    ) -> VortexResult<Self> {
        let validity = validity.filter(|v| !v.is_empty());
        check_validity_buffer(validity.as_deref(), len)?;
        // TODO(ngates): check encoded has type u8
        Ok(Self {
            encoded,
            validity,
            patches,
            bit_width,
            bit_shift,
            reference,
            len,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn encoded(&self) -> &dyn Array {
        self.encoded.as_ref()
    }

    #[inline]
    pub fn bit_width(&self) -> usize {
        self.bit_width
    }

    #[inline]
    pub fn bit_shift(&self) -> usize {
        self.bit_shift
    }

    #[inline]
    pub fn reference(&self) -> Option<&dyn Scalar> {
        self.reference.as_deref()
    }

    #[inline]
    pub fn validity(&self) -> Option<&dyn Array> {
        self.validity.as_deref()
    }

    #[inline]
    pub fn patches(&self) -> Option<&dyn Array> {
        self.patches.as_deref()
    }

    pub fn is_valid(&self, index: usize) -> bool {
        self.validity()
            .map(|v| scalar_at(v, index).and_then(|v| v.try_into()).unwrap())
            .unwrap_or(true)
    }
}

impl Array for FFoRArray {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        unimplemented!("FFoRArray::slice")
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &FFoREncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.encoded().nbytes()
            + self.patches().map(|p| p.nbytes()).unwrap_or(0)
            + self.validity().map(|v| v.nbytes()).unwrap_or(0)
    }

    fn serde(&self) -> &dyn ArraySerde {
        todo!()
    }
}

impl ArrayCompute for FFoRArray {}

impl<'arr> AsRef<(dyn Array + 'arr)> for FFoRArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for FFoRArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("packed", format!("u{}", self.bit_width()))?;
        f.property("reference", format!("{:?}", self.reference()))?;
        f.child("encoded", self.encoded())?;
        f.maybe_child("patches", self.patches())?;
        f.maybe_child("validity", self.validity())
    }
}

impl StatsCompute for FFoRArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::default())
    }
}

#[derive(Debug)]
pub struct FFoREncoding;

impl FFoREncoding {
    pub const ID: EncodingId = EncodingId::new("fastlanes.ffor");
}

impl Encoding for FFoREncoding {
    fn id(&self) -> &EncodingId {
        &Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }
}
