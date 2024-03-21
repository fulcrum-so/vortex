use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::{Arc, RwLock};

use linkme::distributed_slice;
use vortex_schema::{CompositeID, DType};

use crate::array::composite::{find_extension, CompositeExtensionRef, TypedCompositeArray};
use crate::array::{Array, ArrayRef, Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::compress::EncodingCompression;
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, BytesSerde, EncodingSerde};
use crate::stats::{Stats, StatsCompute, StatsSet};

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
    pub fn underlying(&self) -> &dyn Array {
        self.underlying.as_ref()
    }

    pub fn as_typed<M: CompositeMetadata>(&self) -> TypedCompositeArray<M> {
        TypedCompositeArray::new(
            M::deserialize(self.metadata().as_slice()).unwrap(),
            dyn_clone::clone_box(self.underlying()),
        )
    }

    pub fn as_typed_compute(&self) -> Box<dyn ArrayCompute> {
        self.extension.as_typed_compute(self)
    }
}

impl Array for CompositeArray {
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
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &CompositeEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.underlying.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl StatsCompute for CompositeArray {}

impl<'arr> AsRef<(dyn Array + 'arr)> for CompositeArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

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
