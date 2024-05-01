use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use log::{debug, info, warn};
use vortex_error::{vortex_bail, VortexResult};

use crate::array::chunked::{Chunked, ChunkedArray, ChunkedEncoding};
use crate::array::composite::CompositeEncoding;
use crate::array::constant::{Constant, ConstantArray};
use crate::array::r#struct::{Struct, StructArray, StructEncoding};
use crate::array::sparse::SparseEncoding;
use crate::array::varbin::VarBinEncoding;
use crate::compute::scalar_at::scalar_at;
use crate::compute::slice::slice;
use crate::encoding::{ArrayEncoding, EncodingRef, VORTEX_ENCODINGS};
use crate::sampling::stratified_slices;
use crate::stats::ArrayStatistics;
use crate::validity::Validity;
use crate::{compute, Array, ArrayDType, ArrayDef, ArrayTrait, IntoArray, OwnedArray, ToStatic};

pub trait EncodingCompression: ArrayEncoding {
    fn cost(&self) -> u8 {
        1
    }

    fn can_compress(
        &self,
        _array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        None
    }

    fn compress(
        &self,
        _array: &Array,
        _like: Option<&Array>,
        _ctx: CompressCtx,
    ) -> VortexResult<OwnedArray> {
        vortex_bail!(NotImplemented: "compress", self.id())
    }

    // For an array returned by this encoding, give the size in bytes minus any constant overheads.
    fn compressed_nbytes(&self, array: &Array) -> usize {
        array.with_dyn(|a| a.nbytes())
    }
}

#[derive(Debug, Clone)]
pub struct CompressConfig {
    #[allow(dead_code)]
    block_size: u32,
    sample_size: u16,
    sample_count: u16,
    max_depth: u8,
    // TODO(ngates): can each encoding define their own configs?
    pub ree_average_run_threshold: f32,
    encodings: HashSet<EncodingRef>,
    disabled_encodings: HashSet<EncodingRef>,
}

impl Default for CompressConfig {
    fn default() -> Self {
        // TODO(ngates): we should ensure that sample_size * sample_count <= block_size
        Self {
            block_size: 65_536,
            // Sample length should always be multiple of 1024
            sample_size: 128,
            sample_count: 8,
            max_depth: 3,
            ree_average_run_threshold: 2.0,
            encodings: HashSet::from([
                &ChunkedEncoding as EncodingRef,
                &CompositeEncoding,
                &SparseEncoding,
                &StructEncoding,
                &VarBinEncoding,
            ]),
            disabled_encodings: HashSet::new(),
        }
    }
}

impl CompressConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_enabled<E: IntoIterator<Item = EncodingRef>>(self, encodings: E) -> Self {
        let mut new_self = self.clone();
        encodings.into_iter().for_each(|e| {
            new_self.encodings.insert(e);
        });
        new_self
    }

    pub fn with_disabled<E: IntoIterator<Item = EncodingRef>>(self, disabled_encodings: E) -> Self {
        let mut new_self = self.clone();
        disabled_encodings.into_iter().for_each(|e| {
            new_self.disabled_encodings.insert(e);
        });
        new_self
    }

    pub fn is_enabled(&self, kind: EncodingRef) -> bool {
        (self.encodings.is_empty() || self.encodings.contains(&kind))
            && !self.disabled_encodings.contains(&kind)
    }
}

#[derive(Debug, Clone)]
pub struct CompressCtx {
    path: Vec<String>,
    // TODO(ngates): put this back to a reference
    options: Arc<CompressConfig>,
    depth: u8,
    disabled_encodings: HashSet<EncodingRef>,
}

impl Display for CompressCtx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}|{}]", self.depth, self.path.join("."))
    }
}

impl CompressCtx {
    pub fn new(options: Arc<CompressConfig>) -> Self {
        Self {
            path: Vec::new(),
            options,
            depth: 0,
            disabled_encodings: HashSet::new(),
        }
    }

    pub fn named(&self, name: &str) -> Self {
        let mut cloned = self.clone();
        cloned.path.push(name.into());
        cloned
    }

    // Returns a new ctx used for compressing an auxiliary arrays.
    // In practice, this means resetting any disabled encodings back to the original config.
    pub fn auxiliary(&self, name: &str) -> Self {
        let mut cloned = self.clone();
        cloned.path.push(name.into());
        cloned.disabled_encodings = HashSet::new();
        cloned
    }

    pub fn for_encoding(&self, compression: &dyn EncodingCompression) -> Self {
        let mut cloned = self.clone();
        cloned.depth += compression.cost();
        cloned
    }

    #[inline]
    pub fn options(&self) -> Arc<CompressConfig> {
        self.options.clone()
    }

    pub fn excluding(&self, encoding: EncodingRef) -> Self {
        let mut cloned = self.clone();
        cloned.disabled_encodings.insert(encoding);
        cloned
    }

    // We don't take a reference to self to force the caller to think about whether to use
    // an auxilliary ctx.
    pub fn compress(&self, arr: &Array, like: Option<&Array>) -> VortexResult<OwnedArray> {
        if arr.is_empty() {
            return Ok(arr.to_static());
        }

        // Attempt to compress using the "like" array, otherwise fall back to sampled compression
        if let Some(l) = like {
            if let Some(compressed) = l
                .encoding()
                .compression()
                .can_compress(arr, self.options().as_ref())
                .map(|c| c.compress(arr, Some(l), self.for_encoding(l.encoding().compression())))
            {
                let compressed = compressed?;
                if compressed.dtype() != arr.dtype() {
                    panic!(
                        "Compression changed dtype: {:?} -> {:?} for {}",
                        arr.dtype(),
                        compressed.dtype(),
                        compressed.tree_display(),
                    );
                }
                return Ok(compressed);
            } else {
                warn!(
                    "{} cannot find compressor to compress {} like {}",
                    self, arr, l
                );
            }
        }

        // Otherwise, attempt to compress the array
        let compressed = self.compress_array(arr)?;
        if compressed.dtype() != arr.dtype() {
            panic!(
                "Compression changed dtype: {:?} -> {:?} for {}",
                arr.dtype(),
                compressed.dtype(),
                compressed.tree_display(),
            );
        }
        Ok(compressed)
    }

    pub fn compress_validity<'a>(&self, validity: Validity<'a>) -> VortexResult<Validity<'a>> {
        match validity {
            Validity::Array(a) => Ok(Validity::Array(self.compress(&a, None)?)),
            a => Ok(a),
        }
    }

    fn compress_array(&self, arr: &Array) -> VortexResult<OwnedArray> {
        match arr.encoding().id() {
            Chunked::ID => {
                // For chunked arrays, we compress each chunk individually
                let chunked = ChunkedArray::try_from(arr)?;
                let compressed_chunks: VortexResult<Vec<OwnedArray>> = chunked
                    .chunks()
                    .map(|chunk| self.compress_array(&chunk))
                    .collect();
                Ok(
                    ChunkedArray::try_new(compressed_chunks?, chunked.dtype().clone())?
                        .into_array(),
                )
            }
            Constant::ID => {
                // Not much better we can do than constant!
                Ok(arr.to_static())
            }
            Struct::ID => {
                // For struct arrays, we compress each field individually
                let strct = StructArray::try_from(arr)?;
                let compressed_fields = strct
                    .children()
                    .map(|field| self.compress_array(&field))
                    .collect::<VortexResult<Vec<_>>>()?;
                Ok(
                    StructArray::try_new(strct.names().clone(), compressed_fields, strct.len())?
                        .into_array(),
                )
            }
            _ => {
                // Otherwise, we run sampled compression over pluggable encodings
                let sampled = sampled_compression(arr, self)?;
                Ok(sampled.unwrap_or_else(|| arr.to_static()))
            }
        }
    }
}

impl Default for CompressCtx {
    fn default() -> Self {
        Self::new(Arc::new(CompressConfig::default()))
    }
}

pub fn sampled_compression(array: &Array, ctx: &CompressCtx) -> VortexResult<Option<OwnedArray>> {
    // First, we try constant compression and shortcut any sampling.
    if !array.is_empty() && array.statistics().compute_is_constant().unwrap_or(false) {
        return Ok(Some(
            ConstantArray::new(scalar_at(array, 0)?, array.len()).into_array(),
        ));
    }

    let mut candidates: Vec<&dyn EncodingCompression> = VORTEX_ENCODINGS
        .iter()
        .filter(|&encoding| ctx.options().is_enabled(*encoding))
        .filter(|&encoding| !ctx.disabled_encodings.contains(encoding))
        .map(|encoding| encoding.compression())
        .filter(|compression| {
            if compression
                .can_compress(array, ctx.options().as_ref())
                .is_some()
            {
                if ctx.depth + compression.cost() > ctx.options.max_depth {
                    debug!(
                        "{} skipping encoding {} due to depth",
                        ctx,
                        compression.id()
                    );
                    return false;
                }
                true
            } else {
                false
            }
        })
        .collect();
    debug!("{} candidates for {}: {:?}", ctx, array, candidates);

    if candidates.is_empty() {
        debug!(
            "{} no compressors for array with dtype: {} and encoding: {}",
            ctx,
            array.dtype(),
            array.encoding().id(),
        );
        return Ok(None);
    }

    // We prefer all other candidates to the array's own encoding.
    // This is because we assume that the array's own encoding is the least efficient, but useful
    // to destructure an array in the final stages of compression. e.g. VarBin would be DictEncoded
    // but then the dictionary itself remains a VarBin array. DictEncoding excludes itself from the
    // dictionary, but we still have a large offsets array that should be compressed.
    // TODO(ngates): we actually probably want some way to prefer dict encoding over other varbin
    //  encodings, e.g. FSST.
    if candidates.len() > 1 {
        candidates.retain(|&compression| compression.id() != array.encoding().id());
    }

    if array.len() <= (ctx.options.sample_size as usize * ctx.options.sample_count as usize) {
        // We're either already within a sample, or we're operating over a sufficiently small array.
        return find_best_compression(candidates, array, ctx)
            .map(|best| best.map(|(_compression, best)| best));
    }

    // Take a sample of the array, then ask codecs for their best compression estimate.
    let sample = compute::as_contiguous::as_contiguous(
        &stratified_slices(
            array.len(),
            ctx.options.sample_size,
            ctx.options.sample_count,
        )
        .into_iter()
        .map(|(start, stop)| slice(array, start, stop).unwrap())
        .collect::<Vec<_>>(),
    )?;

    find_best_compression(candidates, &sample, ctx)?
        .map(|(compression, best)| {
            info!("{} compressing array {} like {}", ctx, array, best);
            ctx.for_encoding(compression).compress(array, Some(&best))
        })
        .transpose()
}

fn find_best_compression<'a>(
    candidates: Vec<&'a dyn EncodingCompression>,
    sample: &Array,
    ctx: &CompressCtx,
) -> VortexResult<Option<(&'a dyn EncodingCompression, OwnedArray)>> {
    let mut best = None;
    let mut best_ratio = 1.0;
    for compression in candidates {
        debug!(
            "{} trying candidate {} for {}",
            ctx,
            compression.id(),
            sample
        );
        if compression
            .can_compress(sample, ctx.options.as_ref())
            .is_none()
        {
            continue;
        }
        let compressed_sample =
            compression.compress(sample, None, ctx.for_encoding(compression))?;
        let compressed_size = compression.compressed_nbytes(&compressed_sample);
        let ratio = compressed_size as f32 / sample.with_dyn(|a| a.nbytes()) as f32;
        debug!("{} ratio for {}: {}", ctx, compression.id(), ratio);
        if ratio < best_ratio {
            best_ratio = ratio;
            best = Some((compression, compressed_sample))
        }
    }
    Ok(best)
}
