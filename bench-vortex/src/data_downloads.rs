use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use arrow_array::RecordBatchReader;
use bzip2::read::BzDecoder;
use itertools::Itertools;
use lance::dataset::WriteParams;
use lance::Dataset;
use lance_parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder as LanceParquetRecordBatchReaderBuilder;
use log::info;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::runtime::Runtime;
use vortex::array::chunked::ChunkedArray;
use vortex::array::{ArrayRef, IntoArray};
use vortex::arrow::FromArrowType;
use vortex::serde::WriteCtx;
use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

use crate::idempotent;
use crate::reader::BATCH_SIZE;

pub fn download_data(fname: PathBuf, data_url: &str) -> PathBuf {
    idempotent(&fname, |path| {
        info!("Downloading {} from {}", fname.to_str().unwrap(), data_url);
        let mut file = File::create(path).unwrap();
        let mut response = reqwest::blocking::get(data_url).unwrap();
        if !response.status().is_success() {
            panic!("Failed to download data from {}", data_url);
        }
        response.copy_to(&mut file)
    })
    .unwrap()
}

pub fn parquet_to_lance(lance_fname: &Path, parquet_file: &Path) -> VortexResult<PathBuf> {
    let write_params = WriteParams::default();
    let read = File::open(parquet_file).unwrap();
    let reader = LanceParquetRecordBatchReaderBuilder::try_new(read)
        .unwrap()
        .build()
        .unwrap();

    Runtime::new()
        .unwrap()
        .block_on(Dataset::write(
            reader,
            lance_fname.to_str().unwrap(),
            Some(write_params),
        ))
        .unwrap();
    Ok(PathBuf::from(lance_fname))
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

pub fn decompress_bz2(input_path: PathBuf, output_path: PathBuf) -> PathBuf {
    idempotent(&output_path, |path| {
        info!(
            "Decompressing bzip from {} to {}",
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap()
        );
        let input_file = File::open(input_path).unwrap();
        let mut decoder = BzDecoder::new(input_file);

        let mut buffer = Vec::new();
        decoder.read_to_end(&mut buffer).unwrap();

        let mut output_file = File::create(path).unwrap();
        output_file.write_all(&buffer).unwrap();
        Ok::<PathBuf, VortexError>(output_path.clone())
    })
    .unwrap()
}

pub trait BenchmarkDataset {
    fn as_uncompressed(&self);
    fn compress_to_vortex(&self) -> Vec<ArrayRef>;
    fn write_as_parquet(&self);
    fn write_as_vortex(&self);
    fn write_as_lance(&self);
    fn list_files(&self, file_type: FileType) -> Vec<PathBuf>;
    fn directory_location(&self) -> PathBuf;
}

#[derive(Clone, Copy)]
pub enum FileType {
    Csv,
    Parquet,
    Vortex,
    Lance,
}
