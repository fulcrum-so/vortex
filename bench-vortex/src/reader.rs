use std::collections::HashMap;
use std::fs::File;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow_array::types::Int64Type;
use arrow_array::{
    ArrayRef as ArrowArrayRef, PrimitiveArray as ArrowPrimitiveArray, RecordBatch,
    RecordBatchReader,
};
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use log::info;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::runtime::Runtime;
use vortex::array::chunked::ChunkedArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::arrow::FromArrowType;
use vortex::compress::Compressor;
use vortex::stream::ArrayStreamExt;
use vortex::{Array, IntoArray, IntoCanonical, ToArrayData, ViewContext};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_ipc::chunked_reader::ChunkedArrayReader;
use vortex_ipc::io::{TokioAdapter, VortexWrite};
use vortex_ipc::writer::ArrayWriter;
use vortex_ipc::MessageReader;
use vortex_sampling_compressor::SamplingCompressor;

use crate::{COMPRESSORS, CTX};

pub const BATCH_SIZE: usize = 65_536;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VortexFooter {
    pub byte_offsets: Vec<u64>,
    pub row_offsets: Vec<u64>,
}

pub fn open_vortex(path: &Path) -> VortexResult<Array> {
    Runtime::new()
        .unwrap()
        .block_on(async {
            let file = tokio::fs::File::open(path).await.unwrap();
            let mut msgs = MessageReader::try_new(TokioAdapter(file)).await.unwrap();
            msgs.array_stream_from_messages(&CTX)
                .await
                .unwrap()
                .collect_chunked()
                .await
        })
        .map(|a| a.into_array())
}

pub async fn open_vortex_async(path: &Path) -> VortexResult<Array> {
    let file = tokio::fs::File::open(path).await.unwrap();
    let mut msgs = MessageReader::try_new(TokioAdapter(file)).await.unwrap();
    msgs.array_stream_from_messages(&CTX)
        .await
        .unwrap()
        .collect_chunked()
        .await
        .map(|a| a.into_array())
}

pub async fn rewrite_parquet_as_vortex<W: VortexWrite>(
    parquet_path: PathBuf,
    write: W,
) -> VortexResult<()> {
    let chunked = compress_parquet_to_vortex(parquet_path.as_path())?;

    let written = ArrayWriter::new(write, ViewContext::from(&CTX.clone()))
        .write_context()
        .await?
        .write_array_stream(chunked.array_stream())
        .await?;

    let layout = written.array_layouts()[0].clone();
    let mut w = written.into_inner();
    let mut s = flexbuffers::FlexbufferSerializer::new();
    VortexFooter {
        byte_offsets: layout.chunks.byte_offsets,
        row_offsets: layout.chunks.row_offsets,
    }
    .serialize(&mut s)?;
    let footer_bytes = Buffer::Bytes(Bytes::from(s.take_buffer()));
    let footer_len = footer_bytes.len() as u64;
    w.write_all(footer_bytes).await?;
    w.write_all(footer_len.to_le_bytes()).await?;

    Ok(())
}

pub fn compress_parquet_to_vortex(parquet_path: &Path) -> VortexResult<ChunkedArray> {
    let taxi_pq = File::open(parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(taxi_pq)?;

    // FIXME(ngates): #157 the compressor should handle batch size.
    let reader = builder.with_batch_size(BATCH_SIZE).build()?;

    let dtype = DType::from_arrow(reader.schema());

    let strategy = SamplingCompressor::new(COMPRESSORS.clone());
    let compressor = Compressor::new(&strategy);
    let chunks = reader
        .map(|batch_result| batch_result.unwrap())
        .map(|record_batch| {
            let vortex_array = record_batch.to_array_data().into_array();
            compressor.compress(&vortex_array).unwrap()
        })
        .collect_vec();
    ChunkedArray::try_new(chunks, dtype)
}

pub fn write_csv_as_parquet(csv_path: PathBuf, output_path: &Path) -> VortexResult<()> {
    info!(
        "Compressing {} to parquet",
        csv_path.as_path().to_str().unwrap()
    );
    Command::new("duckdb")
        .arg("-c")
        .arg(format!(
            "COPY (SELECT * FROM read_csv('{}', delim = '|', header = false, nullstr = 'null')) TO '{}' (COMPRESSION ZSTD);",
            csv_path.as_path().to_str().unwrap(),
            output_path.to_str().unwrap()
        ))
        .status()?
        .exit_ok()
        .unwrap();
    Ok(())
}

pub async fn take_vortex(path: &Path, indices: &[u64]) -> VortexResult<Array> {
    let mut file = tokio::fs::File::open(path).await?;

    file.seek(SeekFrom::End(-8)).await?;
    let footer_len = file.read_u64_le().await? as usize;

    file.seek(SeekFrom::End(-(footer_len as i64 + 8))).await?;
    let mut footer_bytes = BytesMut::with_capacity(footer_len);
    unsafe { footer_bytes.set_len(footer_len) }
    file.read_exact(footer_bytes.as_mut()).await?;

    let footer: VortexFooter = VortexFooter::deserialize(
        flexbuffers::Reader::get_root(footer_bytes.as_ref()).map_err(|e| vortex_err!("{}", e))?,
    )?;

    file.seek(SeekFrom::Start(0)).await?;
    let mut reader = MessageReader::try_new(TokioAdapter(file.try_clone().await?)).await?;
    let view_ctx = reader.read_view_context(&CTX).await?;
    let dtype = reader.read_dtype().await?;

    file.seek(SeekFrom::Start(0)).await?;
    let mut reader = ChunkedArrayReader::try_new(
        TokioAdapter(file),
        view_ctx,
        dtype,
        PrimitiveArray::from(footer.byte_offsets).into_array(),
        PrimitiveArray::from(footer.row_offsets).into_array(),
    )?;

    let indices_array = indices.to_vec().into_array();
    let taken = reader.take_rows(&indices_array).await?;
    // For equivalence.... we flatten to make sure we're not cheating too much.
    Ok(taken.into_canonical()?.into_array())
}

pub fn take_parquet(path: &Path, indices: &[u64]) -> VortexResult<RecordBatch> {
    let file = File::open(path)?;

    // TODO(ngates): enable read_page_index
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    // We figure out which row groups we need to read and a selection filter for each of them.
    let mut row_groups = HashMap::new();
    let mut row_group_offsets = vec![0];
    row_group_offsets.extend(
        builder
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .scan(0i64, |acc, x| {
                *acc += x;
                Some(*acc)
            }),
    );

    for idx in indices {
        let row_group_idx = row_group_offsets
            .binary_search(&(*idx as i64))
            .unwrap_or_else(|e| e - 1);
        row_groups
            .entry(row_group_idx)
            .or_insert_with(Vec::new)
            .push((*idx as i64) - row_group_offsets[row_group_idx]);
    }
    let row_group_indices = row_groups
        .keys()
        .sorted()
        .map(|i| row_groups.get(i).unwrap().clone())
        .collect_vec();

    let reader = builder
        .with_row_groups(row_groups.keys().copied().collect_vec())
        // FIXME(ngates): our indices code assumes the batch size == the row group sizes
        .with_batch_size(10_000_000)
        .build()
        .unwrap();

    let schema = reader.schema();

    let batches = reader
        .into_iter()
        .enumerate()
        .map(|(idx, batch)| {
            let batch = batch.unwrap();
            let indices = ArrowPrimitiveArray::<Int64Type>::from(row_group_indices[idx].clone());
            let indices_array: ArrowArrayRef = Arc::new(indices);
            take_record_batch(&batch, &indices_array).unwrap()
        })
        .collect_vec();

    Ok(concat_batches(&schema, &batches)?)
}
