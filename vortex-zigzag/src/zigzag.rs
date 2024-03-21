use std::any::Any;
use std::sync::{Arc, RwLock};

use vortex::array::{Array, ArrayKind, ArrayRef, Encoding, EncodingId, EncodingRef};
use vortex::compress::EncodingCompression;
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};
use vortex_schema::{DType, Signedness};

use crate::compress::zigzag_encode;

#[derive(Debug, Clone)]
pub struct ZigZagArray {
    encoded: ArrayRef,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl ZigZagArray {
    pub fn new(encoded: ArrayRef) -> Self {
        Self::try_new(encoded).unwrap()
    }

    pub fn try_new(encoded: ArrayRef) -> VortexResult<Self> {
        let dtype = match encoded.dtype() {
            DType::Int(width, Signedness::Unsigned, nullability) => {
                DType::Int(*width, Signedness::Signed, *nullability)
            }
            d => return Err(VortexError::InvalidDType(d.clone())),
        };
        Ok(Self {
            encoded,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub fn encode(array: &dyn Array) -> VortexResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => Ok(zigzag_encode(p)?.boxed()),
            _ => Err(VortexError::InvalidEncoding(array.encoding().id().clone())),
        }
    }

    pub fn encoded(&self) -> &dyn Array {
        self.encoded.as_ref()
    }
}

impl Array for ZigZagArray {
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
        self.encoded.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.encoded.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(Self::try_new(self.encoded.slice(start, stop)?)?.boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &ZigZagEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.encoded.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for ZigZagArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for ZigZagArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("zigzag", self.encoded())
    }
}

#[derive(Debug)]
pub struct ZigZagEncoding;

impl ZigZagEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.zigzag");
}

impl Encoding for ZigZagEncoding {
    fn id(&self) -> &EncodingId {
        &Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
