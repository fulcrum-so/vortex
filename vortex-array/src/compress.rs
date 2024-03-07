use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use log::{debug, info, warn};

use crate::array::chunked::ChunkedArray;
use crate::array::constant::{ConstantArray, ConstantEncoding};
use crate::array::struct_::StructArray;
use crate::array::{Array, ArrayKind, ArrayRef, Encoding, EncodingId, ENCODINGS};
use crate::compute;
use crate::compute::scalar_at::scalar_at;
use crate::error::VortexResult;
use crate::sampling::stratified_slices;
use crate::stats::Stat;

pub trait EncodingCompression: Encoding {
    // TODO(ngates): we could return a weighted score here to allow for better selection?
    fn can_compress(
        &self,
        array: &dyn Array,
        config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression>;

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: &CompressCtx,
    ) -> VortexResult<ArrayRef>;

    // Given a sample returned by this encoding, estimate the compression ratio of the full array.
    fn estimate_ratio(
        &self,
        _array: &dyn Array,
        sample: &dyn Array,
        compressed_sample: &dyn Array,
    ) -> f32 {
        compressed_sample.nbytes() as f32 / sample.nbytes() as f32
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
    encodings: HashSet<&'static EncodingId>,
    disabled_encodings: HashSet<&'static EncodingId>,
}

impl Default for CompressConfig {
    fn default() -> Self {
        // TODO(ngates): we should ensure that sample_size * sample_count <= block_size
        Self {
            block_size: 65_536,
            // Sample length should always be multiple of 1024
            sample_size: 128,
            sample_count: 8,
            max_depth: 5,
            ree_average_run_threshold: 2.0,
            encodings: HashSet::new(),
            disabled_encodings: HashSet::new(),
        }
    }
}

impl CompressConfig {
    pub fn new(
        encodings: HashSet<&'static EncodingId>,
        mut disabled_encodings: HashSet<&'static EncodingId>,
    ) -> Self {
        // Always disable constant encoding, it's handled separately
        disabled_encodings.insert(&ConstantEncoding::ID);
        Self {
            encodings,
            disabled_encodings,
            ..CompressConfig::default()
        }
    }

    pub fn from_encodings(
        encodings: &[&'static dyn Encoding],
        disabled_encodings: &[&'static dyn Encoding],
    ) -> Self {
        Self::new(
            encodings.iter().map(|e| e.id()).collect(),
            disabled_encodings.iter().map(|e| e.id()).collect(),
        )
    }

    pub fn is_enabled(&self, kind: &EncodingId) -> bool {
        (self.encodings.is_empty() || self.encodings.contains(kind))
            && !self.disabled_encodings.contains(kind)
    }
}

#[derive(Debug, Clone)]
pub struct CompressCtx {
    options: Arc<CompressConfig>,
    sample_pct: f32, // The percentage of the overall array that has been sampled.
    depth: u8,
}

impl CompressCtx {
    pub fn new(options: Arc<CompressConfig>) -> Self {
        Self {
            options,
            sample_pct: 1.0,
            depth: 0,
        }
    }

    pub fn excluding(&self, encoding: &'static EncodingId) -> Self {
        let mut cloned = self.clone();
        let mut options = self.options.deref().clone();
        options.disabled_encodings.insert(encoding);
        cloned.options = Arc::new(options);
        cloned
    }

    pub fn for_sample(&self, sample: &dyn Array, array: &dyn Array) -> Self {
        let mut cloned = self.clone();
        cloned.sample_pct = sample.len() as f32 / array.len() as f32;
        cloned
    }

    pub fn compress(&self, arr: &dyn Array, like: Option<&dyn Array>) -> VortexResult<ArrayRef> {
        debug!(
            "Compressing {} array {} like {} at depth={}",
            if self.sample_pct < 1.0 {
                "sample"
            } else {
                "full"
            },
            arr,
            like.map(|l| l.encoding().id().name()).unwrap_or("<none>"),
            self.depth
        );
        if arr.is_empty() {
            return Ok(dyn_clone::clone_box(arr));
        }

        if self.depth >= self.options.max_depth {
            return Ok(dyn_clone::clone_box(arr));
        }

        // Attempt to compress using the "like" array, otherwise fall back to sampled compression
        if let Some(l) = like {
            if let Some(compression) = l
                .encoding()
                .compression()
                .and_then(|c| c.can_compress(arr, self.options.as_ref()))
            {
                return compression.compress(arr, Some(l), self);
            } else {
                warn!("Cannot find compressor to compress {} like {}", arr, l);
                // TODO(ngates): we shouldn't just bail, but we also probably don't want to fully
                //  re-sample.
                return Ok(dyn_clone::clone_box(arr));
            }
        }

        // Otherwise, attempt to compress the array
        self.compress_array(arr)
    }

    fn compress_array(&self, arr: &dyn Array) -> VortexResult<ArrayRef> {
        match ArrayKind::from(arr) {
            ArrayKind::Chunked(chunked) => {
                // For chunked arrays, we compress each chunk individually
                let compressed_chunks: VortexResult<Vec<ArrayRef>> = chunked
                    .chunks()
                    .iter()
                    .map(|chunk| self.compress_array(chunk.as_ref()))
                    .collect();
                Ok(ChunkedArray::new(compressed_chunks?, chunked.dtype().clone()).boxed())
            }
            ArrayKind::Constant(constant) => {
                // Not much better we can do than constant!
                Ok(constant.clone().boxed())
            }
            ArrayKind::Struct(strct) => {
                // For struct arrays, we compress each field individually
                let compressed_fields: VortexResult<Vec<ArrayRef>> = strct
                    .fields()
                    .iter()
                    .map(|field| self.compress_array(field.as_ref()))
                    .collect();
                Ok(StructArray::new(strct.names().clone(), compressed_fields?).boxed())
            }
            _ => {
                // Otherwise, we run sampled compression over pluggable encodings
                Ok(sampled_compression(arr, self)?.unwrap_or_else(|| dyn_clone::clone_box(arr)))
            }
        }
    }

    pub fn next_level(&self) -> Self {
        let mut cloned = self.clone();
        cloned.depth += 1;
        cloned
    }

    pub fn sample_pct(&self) -> f32 {
        self.sample_pct
    }

    #[inline]
    pub fn options(&self) -> Arc<CompressConfig> {
        self.options.clone()
    }
}

impl Default for CompressCtx {
    fn default() -> Self {
        Self::new(Arc::new(CompressConfig::default()))
    }
}

pub fn sampled_compression(array: &dyn Array, ctx: &CompressCtx) -> VortexResult<Option<ArrayRef>> {
    // First, we try constant compression and shortcut any sampling.
    if !array.is_empty()
        && array
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsConstant)
            .unwrap_or(false)
    {
        return Ok(Some(
            ConstantArray::new(scalar_at(array, 0)?, array.len()).boxed(),
        ));
    }

    let candidates: Vec<&dyn EncodingCompression> = ENCODINGS
        .iter()
        .filter(|encoding| ctx.options().is_enabled(encoding.id()))
        .filter_map(|encoding| encoding.compression())
        .filter_map(|compression| compression.can_compress(array, ctx.options().as_ref()))
        .collect();
    debug!("Candidates for {}: {:?}", array, candidates);

    if candidates.is_empty() {
        debug!(
            "No compressors for array with dtype: {} and encoding: {}",
            array.dtype(),
            array.encoding().id(),
        );
        return Ok(None);
    }

    if array.len() <= (ctx.options.sample_size as usize * ctx.options.sample_count as usize) {
        // We're either already within a sample, or we're operating over a sufficiently small array.
        return find_best_compression(candidates, array, array, ctx.clone());
    }

    // Take a sample of the array, then ask codecs for their best compression estimate.
    let sample = compute::as_contiguous::as_contiguous(
        stratified_slices(
            array.len(),
            ctx.options.sample_size,
            ctx.options.sample_count,
        )
        .into_iter()
        .map(|(start, stop)| array.slice(start, stop).unwrap())
        .collect(),
    )?;

    find_best_compression(
        candidates,
        array,
        sample.as_ref(),
        ctx.for_sample(sample.as_ref(), array),
    )?
    .map(|best| {
        info!("Compressing array {} like {}", array, best);
        ctx.compress(array, Some(best.as_ref()))
    })
    .transpose()
}

fn find_best_compression(
    candidates: Vec<&'static dyn EncodingCompression>,
    array: &dyn Array,
    sample: &dyn Array,
    ctx: CompressCtx,
) -> VortexResult<Option<ArrayRef>> {
    let mut best = None;
    let mut best_ratio = 1.0;
    for compression in candidates {
        let compressed_sample = compression.compress(sample, None, &ctx)?;
        let ratio = compression.estimate_ratio(array, sample, compressed_sample.as_ref());
        debug!(
            "Ratio for {}: {} {}",
            compression.id(),
            ratio,
            compressed_sample
        );
        if ratio < best_ratio {
            best_ratio = ratio;
            best = Some(compressed_sample)
        }
    }
    Ok(best)
}
