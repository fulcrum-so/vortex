use std::fs::File;
use std::path::PathBuf;
use arrow_array::RecordBatchReader;
use itertools::Itertools;
use lance::Dataset;
use lance::dataset::WriteParams;
use tokio::runtime::Runtime;
use crate::idempotent;
use lance_parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder as LanceParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use vortex::array::chunked::ChunkedArray;
use vortex::array::IntoArray;
use vortex::arrow::FromArrowType;
use vortex::serde::WriteCtx;
use vortex_schema::DType;
use crate::reader::{BATCH_SIZE, compress_parquet_to_vortex};


pub fn download_data(fname: &str, data_url: &str) -> PathBuf {
    idempotent(fname, |path| {
        let mut file = File::create(path).unwrap();

        reqwest::blocking::get(
            data_url,
        )
            .unwrap()
            .copy_to(&mut file)
    }).unwrap()
}


pub fn data_lance(lance_fname: &str, read: File) -> PathBuf {
    idempotent(lance_fname, |path| {
        let write_params = WriteParams::default();
        let reader = LanceParquetRecordBatchReaderBuilder::try_new(read)
            .unwrap()
            .build()
            .unwrap();

        Runtime::new().unwrap().block_on(Dataset::write(
            reader,
            path.to_str().unwrap(),
            Some(write_params),
        ))
    }).unwrap()
}

pub fn data_vortex(fname: &str, data_to_compress: PathBuf) -> PathBuf {
    idempotent(fname, |path| {
        let mut write = File::create(path).unwrap();
        compress_parquet_to_vortex(&data_to_compress, &mut write)
    }).unwrap()
}

pub fn data_vortex_uncompressed(fname_out: &str, downloaded_data: PathBuf) -> PathBuf {
    idempotent(fname_out, |path| {
        let taxi_pq = File::open(downloaded_data).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(taxi_pq).unwrap();

        // FIXME(ngates): #157 the compressor should handle batch size.
        let reader = builder.with_batch_size(BATCH_SIZE).build().unwrap();

        let dtype = DType::from_arrow(reader.schema());

        let chunks = reader
            .map(|batch_result| batch_result.unwrap())
            .map(|record_batch| record_batch.into_array())
            .collect_vec();
        let chunked = ChunkedArray::new(chunks, dtype.clone());

        let mut write = File::create(path).unwrap();
        let mut write_ctx = WriteCtx::new(&mut write);
        write_ctx.dtype(&dtype)?;
        write_ctx.write(&chunked)
    })
        .unwrap()
}