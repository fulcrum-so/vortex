use std::sync::{Arc, RwLock};

use crate::alp::Exponents;
use vortex::array::{Array, ArrayKind, ArrayRef, Encoding, EncodingId, EncodingRef};
use vortex::compress::EncodingCompression;
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::impl_array;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};
use vortex_schema::{DType, IntWidth, Signedness};

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
            ArrayKind::Primitive(p) => Ok(alp_encode(p)?.into_array()),
            _ => Err(VortexError::InvalidEncoding(array.encoding().id())),
        }
    }

    pub fn encoded(&self) -> &ArrayRef {
        &self.encoded
    }

    pub fn exponents(&self) -> &Exponents {
        &self.exponents
    }

    pub fn patches(&self) -> Option<&ArrayRef> {
        self.patches.as_ref()
    }
}

impl Array for ALPArray {
    impl_array!();

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
        Ok(Self::try_new(
            self.encoded().slice(start, stop)?,
            self.exponents().clone(),
            self.patches().map(|p| p.slice(start, stop)).transpose()?,
        )?
        .into_array())
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
