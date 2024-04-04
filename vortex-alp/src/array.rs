use std::sync::{Arc, RwLock};

use vortex::array::validity::Validity;
use vortex::array::{Array, ArrayKind, ArrayRef};
use vortex::compress::EncodingCompression;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};
use vortex::{impl_array, impl_array_compute, ArrayWalker};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_schema::{DType, IntWidth, Signedness};

use crate::alp::Exponents;
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
            DType::Int(IntWidth::_32, Signedness::Signed, nullability) => {
                DType::Float(32.into(), *nullability)
            }
            DType::Int(IntWidth::_64, Signedness::Signed, nullability) => {
                DType::Float(64.into(), *nullability)
            }
            d => vortex_bail!(MismatchedTypes: "int32 or int64", d),
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
            _ => Err(vortex_err!("ALP can only encoding primitive arrays")),
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
    impl_array_compute!();

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

    fn validity(&self) -> Option<Validity> {
        self.encoded().validity()
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

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.encoded())
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
