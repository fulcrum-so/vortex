use std::fmt::{Debug, Display};
use std::sync::{Arc, RwLock};

use linkme::distributed_slice;
use vortex_error::VortexResult;
use vortex_schema::{CompositeID, DType};

use crate::array::composite::{find_extension, CompositeExtensionRef, TypedCompositeArray};
use crate::array::validity::Validity;
use crate::array::{Array, ArrayRef};
use crate::compress::EncodingCompression;
use crate::compute::ArrayCompute;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, BytesSerde, EncodingSerde};
use crate::stats::{Stats, StatsCompute, StatsSet};
use crate::{impl_array, impl_array_compute, ArrayWalker};

pub trait CompositeMetadata:
    'static + Debug + Display + Send + Sync + Sized + Clone + BytesSerde
{
    fn id(&self) -> CompositeID;
}

#[derive(Debug, Clone)]
pub struct CompositeArray {
    extension: CompositeExtensionRef,
    metadata: Arc<Vec<u8>>,
    underlying: ArrayRef,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl CompositeArray {
    pub fn new(id: CompositeID, metadata: Arc<Vec<u8>>, underlying: ArrayRef) -> Self {
        let dtype = DType::Composite(id, underlying.dtype().is_nullable().into());
        let extension = find_extension(id.0).expect("Unrecognized composite extension");
        Self {
            extension,
            metadata,
            underlying,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    #[inline]
    pub fn id(&self) -> CompositeID {
        self.extension.id()
    }

    #[inline]
    pub fn extension(&self) -> CompositeExtensionRef {
        self.extension
    }

    pub fn metadata(&self) -> Arc<Vec<u8>> {
        self.metadata.clone()
    }

    #[inline]
    pub fn underlying(&self) -> &ArrayRef {
        &self.underlying
    }

    pub fn as_typed<M: CompositeMetadata>(&self) -> TypedCompositeArray<M> {
        TypedCompositeArray::new(
            M::deserialize(self.metadata().as_slice()).unwrap(),
            self.underlying().clone(),
        )
    }

    pub fn as_typed_compute(&self) -> Box<dyn ArrayCompute> {
        self.extension.as_typed_compute(self)
    }
}

impl Array for CompositeArray {
    impl_array!();
    impl_array_compute!();

    #[inline]
    fn len(&self) -> usize {
        self.underlying.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.underlying.is_empty()
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
        Ok(Self::new(
            self.id(),
            self.metadata.clone(),
            self.underlying.slice(start, stop)?,
        )
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &CompositeEncoding
    }

    fn nbytes(&self) -> usize {
        self.underlying.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn validity(&self) -> Option<Validity> {
        self.underlying().validity()
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.underlying())
    }
}

impl StatsCompute for CompositeArray {}

impl ArrayDisplay for CompositeArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("metadata", format!("{:#?}", self.metadata().as_slice()))?;
        f.child("underlying", self.underlying.as_ref())
    }
}

#[derive(Debug)]
pub struct CompositeEncoding;

impl CompositeEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.composite");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_COMPOSITE: EncodingRef = &CompositeEncoding;

impl Encoding for CompositeEncoding {
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
