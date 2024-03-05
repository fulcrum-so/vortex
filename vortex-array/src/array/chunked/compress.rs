use rayon::prelude::*;

use crate::array::chunked::{ChunkedArray, ChunkedEncoding};
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};

impl EncodingCompression for ChunkedEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        let encoding = array.encoding().id();
        if encoding == &Self::ID {
            Some(&(chunked_compressor as Compressor))
        } else {
            None
        }
    }
}

fn chunked_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let chunked_array = array.as_chunked();
    let chunked_like = like.map(|like_array| like_array.as_chunked());

    let compressed_chunks = chunked_like
        .map(|c_like| {
            chunked_array
                .chunks()
                .par_iter()
                .zip_eq(c_like.chunks())
                .map(|(chunk, chunk_like)| ctx.compress(chunk.as_ref(), Some(chunk_like.as_ref())))
                .collect()
        })
        .unwrap_or_else(|| {
            chunked_array
                .chunks()
                .par_iter()
                .map(|chunk| ctx.compress(chunk.as_ref(), None))
                .collect()
        });

    ChunkedArray::new(compressed_chunks, array.dtype().clone()).boxed()
}
