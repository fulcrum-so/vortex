use std::collections::HashMap;
use std::ops::{Deref, Range};

use bytes::BytesMut;
use futures_util::{stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use vortex::array::chunked::ChunkedArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::search_sorted::{search_sorted, SearchSortedSide};
use vortex::compute::slice::slice;
use vortex::compute::take::take;
use vortex::compute::unary::cast::try_cast;
use vortex::compute::unary::scalar_subtract::subtract_scalar;
use vortex::stats::ArrayStatistics;
use vortex::stream::{ArrayStream, ArrayStreamExt};
use vortex::{Array, ArrayDType, IntoArray, IntoCanonical};
use vortex_dtype::PType;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::chunked_reader::ChunkedArrayReader;
use crate::io::VortexReadAt;
use crate::stream_reader::StreamArrayReader;

impl<R: VortexReadAt> ChunkedArrayReader<R> {
    pub async fn take_rows(&mut self, indices: &Array) -> VortexResult<Array> {
        // Figure out if the row indices are sorted / unique. If not, we need to sort them.
        if indices
            .statistics()
            .compute_is_strict_sorted()
            .unwrap_or(false)
        {
            // With strict-sorted indices, we can take the rows directly.
            return self.take_rows_strict_sorted(indices).await;
        }

        //         // Figure out which chunks are relevant to the read operation using the row_offsets array.
        //         // Depending on whether there are more indices than chunks, we may wish to perform this
        //         // join differently.
        //
        //         // Coalesce the chunks we care about by some metric.
        //
        //         // TODO(ngates): we could support read_into for array builders since we know the size
        //         //  of the result.
        //         // Read the relevant chunks.
        // Reshuffle the result as per the original sort order.
        unimplemented!()
    }

    /// Take rows from a chunked array given strict sorted indices.
    ///
    /// The strategy for doing this depends on the quantity and distribution of the indices...
    ///
    /// For now, we will find the relevant chunks, coalesce them, and read.
    async fn take_rows_strict_sorted(&mut self, indices: &Array) -> VortexResult<Array> {
        // Figure out which chunks are relevant.
        let chunk_idxs = find_chunks(&self.row_offsets, indices)?;
        // Coalesce the chunks that we're going to read from.
        let coalesced_chunks = self.coalesce_chunks(chunk_idxs.as_ref());

        // Grab the row and byte offsets for each chunk range.
        let start_chunks = PrimitiveArray::from(
            coalesced_chunks
                .iter()
                .map(|chunks| chunks[0].chunk_idx)
                .collect_vec(),
        )
        .into_array();
        let start_rows = take(&self.row_offsets, &start_chunks)?
            .into_canonical()?
            .into_primitive()?;
        let start_bytes = take(&self.byte_offsets, &start_chunks)?
            .into_canonical()?
            .into_primitive()?;

        let stop_chunks = PrimitiveArray::from(
            coalesced_chunks
                .iter()
                .map(|chunks| chunks.last().unwrap().chunk_idx + 1)
                .collect_vec(),
        )
        .into_array();
        let stop_rows = take(&self.row_offsets, &stop_chunks)?
            .into_canonical()?
            .into_primitive()?;
        let stop_bytes = take(&self.byte_offsets, &stop_chunks)?
            .into_canonical()?
            .into_primitive()?;

        // For each chunk-range, read the data as an ArrayStream and call take on it.
        let chunks = stream::iter(0..coalesced_chunks.len())
            .map(|chunk_idx| {
                let (start_byte, stop_byte) = (
                    start_bytes.get_as_cast::<u64>(chunk_idx),
                    stop_bytes.get_as_cast::<u64>(chunk_idx),
                );
                let (start_row, stop_row) = (
                    start_rows.get_as_cast::<u64>(chunk_idx),
                    stop_rows.get_as_cast::<u64>(chunk_idx),
                );
                self.take_from_chunk(indices, start_byte..stop_byte, start_row..stop_row)
            })
            .buffered(10)
            .try_flatten()
            .try_collect()
            .await?;

        Ok(ChunkedArray::try_new(chunks, self.dtype.clone())?.into_array())
    }

    /// Coalesce reads for the given chunks.
    ///
    /// This depends on a few factors:
    /// * The number of bytes between adjacent selected chunks.
    /// * The latency of the underlying storage.
    /// * The throughput of the underlying storage.
    fn coalesce_chunks(&self, chunk_idxs: &[ChunkIndices]) -> Vec<Vec<ChunkIndices>> {
        let _hint = self.read.performance_hint();
        chunk_idxs
            .iter()
            .cloned()
            .map(|chunk_idx| vec![chunk_idx.clone()])
            .collect_vec()
    }

    async fn take_from_chunk(
        &self,
        indices: &Array,
        byte_range: Range<u64>,
        row_range: Range<u64>,
    ) -> VortexResult<impl ArrayStream> {
        let range_byte_len = (byte_range.end - byte_range.start) as usize;

        // Relativize the indices to these chunks
        let indices_start =
            search_sorted(indices, row_range.start, SearchSortedSide::Left)?.to_index();
        let indices_stop =
            search_sorted(indices, row_range.end, SearchSortedSide::Right)?.to_index();
        let relative_indices = slice(indices, indices_start, indices_stop)?;
        let row_start_scalar = Scalar::from(row_range.start).cast(relative_indices.dtype())?;
        let relative_indices = subtract_scalar(&relative_indices, &row_start_scalar)?;

        // Set up an array reader to read this range of chunks.
        let mut buffer = BytesMut::with_capacity(range_byte_len);
        unsafe { buffer.set_len(range_byte_len) }
        // TODO(ngates): instead of reading the whole range into a buffer, we should stream
        //  the byte range (e.g. if its coming from an HTTP endpoint) and wrap that with an
        //  MesssageReader.
        let buffer = self.read.read_at_into(byte_range.start, buffer).await?;

        let reader = StreamArrayReader::try_new(buffer)
            .await?
            .with_view_context(self.view_context.deref().clone())
            .with_dtype(self.dtype.clone());

        // Take the indices from the stream.
        reader.into_array_stream().take_rows(relative_indices)
    }
}

/// Find the chunks that are relevant to the read operation.
/// Both the row_offsets and indices arrays must be strict-sorted.
fn find_chunks(row_offsets: &Array, indices: &Array) -> VortexResult<Vec<ChunkIndices>> {
    // TODO(ngates): lots of optimizations to be had here, potentially lots of push-down.
    //  For now, we just flatten everything into primitive arrays and iterate.
    let row_offsets = try_cast(row_offsets, PType::U64.into())?
        .into_canonical()?
        .into_primitive()?;
    let _rows = format!("{:?}", row_offsets.maybe_null_slice::<u64>());
    let indices = try_cast(indices, PType::U64.into())?
        .into_canonical()?
        .into_primitive()?;
    let _indices = format!("{:?}", indices.maybe_null_slice::<u64>());

    if let (Some(last_idx), Some(num_rows)) = (
        indices.maybe_null_slice::<u64>().last(),
        row_offsets.maybe_null_slice::<u64>().last(),
    ) {
        if last_idx >= num_rows {
            vortex_bail!("Index {} out of bounds {}", last_idx, num_rows);
        }
    }

    let mut chunks = HashMap::new();

    let row_offsets_ref = row_offsets.maybe_null_slice::<u64>();
    for (pos, idx) in indices.maybe_null_slice::<u64>().iter().enumerate() {
        let chunk_idx = row_offsets_ref.binary_search(idx).unwrap_or_else(|x| x - 1);
        chunks
            .entry(chunk_idx as u32)
            .and_modify(|chunk_indices: &mut ChunkIndices| {
                chunk_indices.indices_stop = (pos + 1) as u64;
            })
            .or_insert(ChunkIndices {
                chunk_idx: chunk_idx as u32,
                indices_start: pos as u64,
                indices_stop: (pos + 1) as u64,
            });
    }

    Ok(chunks
        .keys()
        .sorted()
        .map(|k| chunks.get(k).unwrap())
        .cloned()
        .collect_vec())
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ChunkIndices {
    chunk_idx: u32,
    // The position into the indices array that is covered by this chunk.
    indices_start: u64,
    indices_stop: u64,
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use itertools::Itertools;
    use vortex::array::chunked::ChunkedArray;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::{ArrayTrait, IntoArray, IntoCanonical, ViewContext};
    use vortex_buffer::Buffer;
    use vortex_dtype::PType;
    use vortex_error::VortexResult;

    use crate::chunked_reader::ChunkedArrayReader;
    use crate::writer::ArrayWriter;
    use crate::MessageReader;

    async fn chunked_array() -> VortexResult<ArrayWriter<Vec<u8>>> {
        let c = ChunkedArray::try_new(
            vec![PrimitiveArray::from((0i32..1000).collect_vec()).into_array(); 10],
            PType::I32.into(),
        )?
        .into_array();

        ArrayWriter::new(vec![], ViewContext::default())
            .write_context()
            .await?
            .write_array(c)
            .await
    }

    #[tokio::test]
    async fn test_take_rows() -> VortexResult<()> {
        let writer = chunked_array().await?;

        let array_layout = writer.array_layouts()[0].clone();
        let row_offsets = PrimitiveArray::from(array_layout.chunks.row_offsets.clone());
        let byte_offsets = PrimitiveArray::from(array_layout.chunks.byte_offsets.clone());

        let buffer = Buffer::from(writer.into_inner());

        let mut msgs = MessageReader::try_new(Cursor::new(buffer.clone())).await?;
        let view_ctx = msgs.read_view_context(&Default::default()).await?;
        let dtype = msgs.read_dtype().await?;

        let mut reader = ChunkedArrayReader::try_new(
            buffer,
            view_ctx,
            dtype,
            byte_offsets.into_array(),
            row_offsets.into_array(),
        )
        .unwrap();

        let result = reader
            .take_rows(&PrimitiveArray::from(vec![0u64, 10, 10_000 - 1]).into_array())
            .await?
            .into_canonical()?
            .into_primitive()?;

        assert_eq!(result.len(), 3);
        assert_eq!(result.maybe_null_slice::<i32>(), &[0, 10, 999]);

        Ok(())
    }
}
