use std::any::Any;
use std::sync::{Arc, RwLock};

use crate::alp::Exponents;
use vortex::array::{Array, ArrayKind, ArrayRef, Encoding, EncodingId, EncodingRef};
use vortex::compress::EncodingCompression;
use vortex::dtype::{DType, IntWidth, Signedness};
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};

use crate::compress::alp_encode;

#[derive(Debug, Clone)]
pub struct ALPArray {
    encoded: ArrayRef,
    exponents: Exponents,
    patches: Option<ArrayRef>,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl ALPArray {
    pub fn new(encoded: ArrayRef, exponents: Exponents, patches: Option<ArrayRef>) -> Self {
        Self::try_new(encoded, exponents, patches).unwrap()
    }

    pub fn try_new(
        encoded: ArrayRef,
        exponents: Exponents,
        patches: Option<ArrayRef>,
    ) -> VortexResult<Self> {
        let dtype = match encoded.dtype() {
            d @ DType::Int(width, Signedness::Signed, nullability) => match width {
                IntWidth::_32 => DType::Float(32.into(), *nullability),
                IntWidth::_64 => DType::Float(64.into(), *nullability),
                _ => return Err(VortexError::InvalidDType(d.clone())),
            },
            d => return Err(VortexError::InvalidDType(d.clone())),
        };
        Ok(Self {
            encoded,
            exponents,
            patches,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub fn encode(array: &dyn Array) -> VortexResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => Ok(alp_encode(p)?.boxed()),
            _ => Err(VortexError::InvalidEncoding(array.encoding().id().clone())),
        }
    }

    pub fn encoded(&self) -> &dyn Array {
        self.encoded.as_ref()
    }

    pub fn exponents(&self) -> &Exponents {
        &self.exponents
    }

    pub fn patches(&self) -> Option<&dyn Array> {
        self.patches.as_deref()
    }
}

impl Array for ALPArray {
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

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(Self::try_new(
            self.encoded().slice(start, stop)?,
            self.exponents().clone(),
            self.patches().map(|p| p.slice(start, stop)).transpose()?,
        )?
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &ALPEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.encoded().nbytes() + self.patches().map(|p| p.nbytes()).unwrap_or(0)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for ALPArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for ALPArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("exponents", format!("{:?}", self.exponents()))?;
        f.child("encoded", self.encoded())?;
        f.maybe_child("patches", self.patches())
    }
}

#[derive(Debug)]
pub struct ALPEncoding;

impl ALPEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.alp");
}

impl Encoding for ALPEncoding {
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
