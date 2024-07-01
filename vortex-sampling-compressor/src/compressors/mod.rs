use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

use vortex::Array;
use vortex_error::VortexResult;

use crate::SamplingCompressor;

pub mod alp;
pub mod bitpacked;
pub mod constant;
pub mod delta;
pub mod dict;
pub mod r#for;
pub mod localdatetime;
pub mod roaring_bool;
pub mod roaring_int;
pub mod runend;
pub mod sparse;
pub mod zigzag;

pub trait EncodingCompressor: Sync + Send + Debug {
    fn id(&self) -> &str;

    fn cost(&self) -> u8 {
        1
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor>;

    fn compress<'a>(
        &'a self,
        array: &Array,
        like: Option<CompressionTree<'a>>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>>;
}

pub type CompressorRef<'a> = &'a dyn EncodingCompressor;

impl PartialEq for dyn EncodingCompressor + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
impl Eq for dyn EncodingCompressor + '_ {}
impl Hash for dyn EncodingCompressor + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state)
    }
}

#[derive(Debug, Clone)]
pub struct CompressionTree<'a> {
    compressor: &'a dyn EncodingCompressor,
    children: Vec<Option<CompressionTree<'a>>>,
}

impl Display for CompressionTree<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.compressor.id(), f)
    }
}

impl<'a> CompressionTree<'a> {
    pub fn flat(compressor: &'a dyn EncodingCompressor) -> Self {
        Self::new(compressor, vec![])
    }

    pub fn new(
        compressor: &'a dyn EncodingCompressor,
        children: Vec<Option<CompressionTree<'a>>>,
    ) -> Self {
        Self {
            compressor,
            children,
        }
    }

    pub fn child(&self, idx: usize) -> Option<&CompressionTree<'a>> {
        self.children[idx].as_ref()
    }

    /// Compresses array with our compressor without verifying that the compressor can compress this array
    pub fn compress_unchecked(
        &self,
        array: &Array,
        ctx: &SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        self.compressor.compress(
            array,
            Some(self.clone()),
            ctx.for_compressor(self.compressor),
        )
    }

    pub fn compress(
        &self,
        array: &Array,
        ctx: &SamplingCompressor<'a>,
    ) -> Option<VortexResult<CompressedArray<'a>>> {
        self.compressor
            .can_compress(array)
            .map(|c| c.compress(array, Some(self.clone()), ctx.for_compressor(c)))
    }
}

#[derive(Debug, Clone)]
pub struct CompressedArray<'a> {
    array: Array,
    path: Option<CompressionTree<'a>>,
}

impl<'a> CompressedArray<'a> {
    pub fn uncompressed(array: Array) -> Self {
        Self::new(array, None)
    }

    pub fn new(array: Array, path: Option<CompressionTree<'a>>) -> Self {
        Self { array, path }
    }

    #[inline]
    pub fn array(&self) -> &Array {
        &self.array
    }

    #[inline]
    pub fn into_array(self) -> Array {
        self.array
    }

    #[inline]
    pub fn path(&self) -> &Option<CompressionTree> {
        &self.path
    }

    #[inline]
    pub fn into_path(self) -> Option<CompressionTree<'a>> {
        self.path
    }

    #[inline]
    pub fn nbytes(&self) -> usize {
        self.array.nbytes()
    }
}
