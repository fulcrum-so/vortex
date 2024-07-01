#![feature(exit_status_error)]

use std::collections::HashSet;
use std::env::temp_dir;
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};

use arrow_array::RecordBatchReader;
use humansize::DECIMAL;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::{info, LevelFilter};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};
use vortex::array::chunked::ChunkedArray;
use vortex::arrow::FromArrowType;
use vortex::compress::Compressor;
use vortex::encoding::EncodingRef;
use vortex::{Array, Context, IntoArray, ToArrayData};
use vortex_alp::ALPEncoding;
use vortex_datetime_parts::DateTimePartsEncoding;
use vortex_dict::DictEncoding;
use vortex_dtype::DType;
use vortex_fastlanes::{BitPackedEncoding, FoREncoding};
use vortex_roaring::RoaringBoolEncoding;
use vortex_runend::RunEndEncoding;
use vortex_sampling_compressor::compressors::alp::ALPCompressor;
use vortex_sampling_compressor::compressors::bitpacked::BitPackedCompressor;
use vortex_sampling_compressor::compressors::dict::DictCompressor;
use vortex_sampling_compressor::compressors::localdatetime::DateTimePartsCompressor;
use vortex_sampling_compressor::compressors::r#for::FoRCompressor;
use vortex_sampling_compressor::compressors::roaring_bool::RoaringBoolCompressor;
use vortex_sampling_compressor::compressors::runend::DEFAULT_RUN_END_COMPRESSOR;
use vortex_sampling_compressor::compressors::sparse::SparseCompressor;
use vortex_sampling_compressor::compressors::CompressorRef;
use vortex_sampling_compressor::SamplingCompressor;

use crate::data_downloads::FileType;
use crate::reader::BATCH_SIZE;
use crate::taxi_data::taxi_data_parquet;

pub mod data_downloads;
pub mod parquet_utils;
pub mod public_bi_data;
pub mod reader;
pub mod taxi_data;
pub mod vortex_utils;

lazy_static! {
    pub static ref CTX: Context = Context::default().with_encodings([
        &ALPEncoding as EncodingRef,
        &DictEncoding,
        &BitPackedEncoding,
        &FoREncoding,
        &DateTimePartsEncoding,
        // &DeltaEncoding,  Blows up the search space too much.
        &RunEndEncoding,
        &RoaringBoolEncoding,
        // &RoaringIntEncoding,
        // Doesn't offer anything more than FoR really
        // &ZigZagEncoding,
    ]);
}

lazy_static! {
    pub static ref COMPRESSORS: HashSet<CompressorRef<'static>> = [
        &ALPCompressor as CompressorRef<'static>,
        &DictCompressor,
        &BitPackedCompressor,
        &FoRCompressor,
        &DateTimePartsCompressor,
        &DEFAULT_RUN_END_COMPRESSOR,
        &RoaringBoolCompressor,
        &SparseCompressor
    ]
    .into();
}

/// Creates a file if it doesn't already exist.
/// NB: Does NOT modify the given path to ensure that it resides in the data directory.
pub fn idempotent<T, E, P: IdempotentPath + ?Sized>(
    path: &P,
    f: impl FnOnce(&Path) -> Result<T, E>,
) -> Result<PathBuf, E> {
    let data_path = path.to_data_path();
    if !data_path.exists() {
        let temp_location = path.to_temp_path();
        let temp_path = temp_location.as_path();
        f(temp_path)?;
        std::fs::rename(temp_path, &data_path).unwrap();
    }
    Ok(data_path)
}

pub trait IdempotentPath {
    fn to_data_path(&self) -> PathBuf;
    fn to_temp_path(&self) -> PathBuf;
}

impl IdempotentPath for str {
    fn to_data_path(&self) -> PathBuf {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .join(self);
        if !path.parent().unwrap().exists() {
            create_dir_all(path.parent().unwrap()).unwrap();
        }
        path
    }

    fn to_temp_path(&self) -> PathBuf {
        let temp_dir = temp_dir().join(uuid::Uuid::new_v4().to_string());
        if !temp_dir.exists() {
            create_dir_all(temp_dir.clone()).unwrap();
        }
        temp_dir.join(self)
    }
}

impl IdempotentPath for PathBuf {
    fn to_data_path(&self) -> PathBuf {
        if !self.parent().unwrap().exists() {
            create_dir_all(self.parent().unwrap()).unwrap();
        }
        self.to_path_buf()
    }

    fn to_temp_path(&self) -> PathBuf {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        if !temp_dir.exists() {
            create_dir_all(temp_dir.clone()).unwrap();
        }
        temp_dir.join(self.file_name().unwrap())
    }
}

pub fn setup_logger(level: LevelFilter) {
    TermLogger::init(
        level,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();
}

pub fn compress_taxi_data() -> Array {
    let file = File::open(taxi_data_parquet()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let _mask = ProjectionMask::roots(builder.parquet_schema(), [6]);
    let _no_datetime_mask = ProjectionMask::roots(
        builder.parquet_schema(),
        [0, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
    );
    let reader = builder
        .with_projection(_mask)
        //.with_projection(no_datetime_mask)
        .with_batch_size(BATCH_SIZE)
        // .with_batch_size(5_000_000)
        // .with_limit(100_000)
        .build()
        .unwrap();

    let schema = reader.schema();
    let mut uncompressed_size: usize = 0;
    let chunks = reader
        .into_iter()
        .map(|batch_result| batch_result.unwrap())
        .map(|batch| batch.to_array_data().into_array())
        .map(|array| {
            uncompressed_size += array.nbytes();
            Compressor::new(&SamplingCompressor::new(COMPRESSORS.clone()))
                .compress(&array)
                .unwrap()
        })
        .collect_vec();

    let compressed = ChunkedArray::try_new(chunks, DType::from_arrow(schema))
        .unwrap()
        .into_array();

    info!(
        "{}, Bytes: {}, Ratio {}",
        humansize::format_size(compressed.nbytes(), DECIMAL),
        compressed.nbytes(),
        compressed.nbytes() as f32 / uncompressed_size as f32
    );

    compressed
}

pub struct CompressionRunStats {
    schema: DType,
    total_compressed_size: Option<u64>,
    compressed_sizes: Vec<u64>,
    file_type: FileType,
    file_name: String,
}

impl CompressionRunStats {
    pub fn to_results(&self, dataset_name: String) -> Vec<CompressionRunResults> {
        let DType::Struct(st, _) = &self.schema else {
            unreachable!()
        };

        self.compressed_sizes
            .iter()
            .zip_eq(st.names().iter().zip_eq(st.dtypes().iter()))
            .map(
                |(&size, (column_name, column_type))| CompressionRunResults {
                    dataset_name: dataset_name.clone(),
                    file_name: self.file_name.clone(),
                    file_type: self.file_type.to_string(),
                    column_name: (**column_name).to_string(),
                    column_type: column_type.to_string(),
                    compressed_size: size,
                    total_compressed_size: self.total_compressed_size,
                },
            )
            .collect::<Vec<_>>()
    }
}

pub struct CompressionRunResults {
    pub dataset_name: String,
    pub file_name: String,
    pub file_type: String,
    pub column_name: String,
    pub column_type: String,
    pub compressed_size: u64,
    pub total_compressed_size: Option<u64>,
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::ops::Deref;
    use std::sync::Arc;

    use arrow_array::{ArrayRef as ArrowArrayRef, StructArray as ArrowStructArray};
    use log::LevelFilter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use vortex::arrow::FromArrowArray;
    use vortex::compress::Compressor;
    use vortex::{ArrayData, IntoArray, IntoCanonical};
    use vortex_sampling_compressor::SamplingCompressor;

    use crate::taxi_data::taxi_data_parquet;
    use crate::{compress_taxi_data, setup_logger, COMPRESSORS};

    #[ignore]
    #[test]
    fn compression_ratio() {
        setup_logger(LevelFilter::Debug);
        _ = compress_taxi_data();
    }

    #[ignore]
    #[test]
    fn round_trip_arrow() {
        let file = File::open(taxi_data_parquet()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.with_limit(1).build().unwrap();

        for record_batch in reader.map(|batch_result| batch_result.unwrap()) {
            let struct_arrow: ArrowStructArray = record_batch.into();
            let arrow_array: ArrowArrayRef = Arc::new(struct_arrow);
            let vortex_array = ArrayData::from_arrow(arrow_array.clone(), false).into_array();
            let vortex_as_arrow = vortex_array.into_canonical().unwrap().into_arrow();
            assert_eq!(vortex_as_arrow.deref(), arrow_array.deref());
        }
    }

    // Ignoring since Struct arrays don't currently support equality.
    // https://github.com/apache/arrow-rs/issues/5199
    #[ignore]
    #[test]
    fn round_trip_arrow_compressed() {
        let file = File::open(taxi_data_parquet()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.with_limit(1).build().unwrap();

        for record_batch in reader.map(|batch_result| batch_result.unwrap()) {
            let struct_arrow: ArrowStructArray = record_batch.into();
            let arrow_array: ArrowArrayRef = Arc::new(struct_arrow);
            let vortex_array = ArrayData::from_arrow(arrow_array.clone(), false).into_array();

            let compressed = Compressor::new(&SamplingCompressor::new(COMPRESSORS.clone()))
                .compress(&vortex_array)
                .unwrap();
            let compressed_as_arrow = compressed.into_canonical().unwrap().into_arrow();
            assert_eq!(compressed_as_arrow.deref(), arrow_array.deref());
        }
    }
}
