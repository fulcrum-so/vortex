use std::collections::HashSet;
use std::fmt::Debug;

use log::debug;
use once_cell::sync::Lazy;

use crate::array::constant::ConstantEncoding;
use crate::array::{Array, ArrayRef, Encoding, EncodingId, ENCODINGS};
use crate::compute;
use crate::sampling::stratified_slices;

pub trait EncodingCompression {
    fn compressor(&self, array: &dyn Array, config: &CompressConfig)
        -> Option<&'static Compressor>;
}

pub type Compressor = fn(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef;

#[derive(Debug, Clone)]
pub struct CompressConfig {
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
            block_size: 65536,
            sample_size: 64,
            sample_count: 10,
            max_depth: 4,
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

static DEFAULT_COMPRESS_CONFIG: Lazy<CompressConfig> = Lazy::new(CompressConfig::default);

#[derive(Debug, Clone)]
pub struct CompressCtx<'a> {
    options: &'a CompressConfig,
    depth: u8,
}

impl<'a> CompressCtx<'a> {
    pub fn new(options: &'a CompressConfig) -> Self {
        Self { options, depth: 0 }
    }

    pub fn compress(&self, arr: &dyn Array, like: Option<&dyn Array>) -> ArrayRef {
        if arr.is_empty() {
            return dyn_clone::clone_box(arr);
        }

        if self.depth >= self.options.max_depth {
            return dyn_clone::clone_box(arr);
        }

        if let Some(l) = like {
            l.encoding()
                .compression()
                .and_then(|c| c.compressor(arr, self.options))
                .map(|compressor| compressor(arr, Some(l), self.clone()))
                // TODO(ngates): wh
                .unwrap_or_else(|| dyn_clone::clone_box(arr))
        } else {
            sampled_compression(arr, self.clone())
        }
    }

    pub fn next_level(&self) -> Self {
        let mut cloned = self.clone();
        cloned.depth += 1;
        cloned
    }

    #[inline]
    pub fn options(&self) -> &CompressConfig {
        self.options
    }
}

impl Default for CompressCtx<'_> {
    fn default() -> Self {
        Self::new(&DEFAULT_COMPRESS_CONFIG)
    }
}

pub fn sampled_compression(array: &dyn Array, ctx: CompressCtx) -> ArrayRef {
    // First, we try constant compression
    if let Some(compressor) = ConstantEncoding.compressor(array, ctx.options()) {
        return compressor(array, None, ctx);
    }

    let candidate_compressors: Vec<&Compressor> = ENCODINGS
        .iter()
        // TODO(robert): Avoid own encoding to avoid infinite recursion
        // .filter(|encoding| encoding.id().name() != array.encoding().id().name())
        .filter(|encoding| ctx.options().is_enabled(encoding.id()))
        .filter_map(|encoding| encoding.compression())
        .filter_map(|compression| compression.compressor(array, ctx.options()))
        .collect();

    if candidate_compressors.is_empty() {
        debug!(
            "No compressors for array with dtype: {} and encoding: {}",
            array.dtype(),
            array.encoding().id(),
        );
        return dyn_clone::clone_box(array);
    }

    if array.len() < ctx.options.block_size as usize {
        // We're either in a sample or we're operating over a sufficiently small array.
        let (_, compressed_sample) = candidate_compressors.iter().fold(
            (array.nbytes(), None),
            |(compressed_bytes, curr_best), compressor| {
                let compressed = compressor(array, None, ctx.next_level());

                if compressed.nbytes() < compressed_bytes {
                    (compressed.nbytes(), Some(compressed))
                } else {
                    (compressed_bytes, curr_best)
                }
            },
        );

        return compressed_sample
            .map(|s| {
                debug!(
                    "Compressed small array with dtype: {} and encoding: {}, using: {}",
                    array.dtype(),
                    array.encoding().id(),
                    s.encoding().id()
                );
                s
            })
            .unwrap_or_else(|| dyn_clone::clone_box(array));
    }

    // Otherwise, take the sample and try each compressor on it.
    let sample = compute::as_contiguous::as_contiguous(
        stratified_slices(
            array.len(),
            ctx.options.sample_size,
            ctx.options.sample_count,
        )
        .into_iter()
        .map(|(start, stop)| array.slice(start, stop).unwrap())
        .collect(),
    )
    // FIXME(ngates): errors
    .unwrap();

    let compression_ratios: Vec<(ArrayRef, f32)> = candidate_compressors
        .into_iter()
        .map(|compressor| {
            let compressed_sample = compressor(sample.as_ref(), None, ctx.clone());
            let compression_ratio = compressed_sample.nbytes() as f32 / sample.nbytes() as f32;
            (compressed_sample, compression_ratio)
        })
        .collect();

    compression_ratios
        .into_iter()
        .filter(|(_, ratio)| *ratio < 1.0)
        .min_by(|(_, first_ratio), (_, second_ratio)| first_ratio.total_cmp(second_ratio))
        .map(|(sample, _)| {
            let c = ctx.next_level().compress(array, Some(sample.as_ref()));
            debug!(
                "Compressed array with dtype: {} and encoding: {} using: {}",
                array.dtype(),
                array.encoding().id(),
                c.encoding().id()
            );
            c
        })
        .unwrap_or_else(|| dyn_clone::clone_box(array))
}
