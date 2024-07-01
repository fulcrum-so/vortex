use std::future::Future;

use futures_util::TryStreamExt;
use vortex_error::VortexResult;

use crate::array::chunked::ChunkedArray;
use crate::stream::take_rows::TakeRows;
use crate::stream::ArrayStream;
use crate::stream::ArrayStreamAdapter;
use crate::Array;

pub trait ArrayStreamExt: ArrayStream {
    fn collect_chunked(self) -> impl Future<Output = VortexResult<ChunkedArray>>
    where
        Self: Sized,
    {
        async {
            let dtype = self.dtype().clone();
            let chunks: Vec<Array> = self.try_collect().await.unwrap();
            ChunkedArray::try_new(chunks, dtype)
        }
    }

    fn take_rows(self, indices: Array) -> VortexResult<impl ArrayStream>
    where
        Self: Sized,
    {
        Ok(ArrayStreamAdapter::new(
            self.dtype().clone(),
            TakeRows::try_new(self, indices)?,
        ))
    }
}

impl<R: ArrayStream> ArrayStreamExt for R {}
