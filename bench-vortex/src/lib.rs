use std::env::temp_dir;
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatchReader;
use humansize::DECIMAL;
use itertools::Itertools;
use log::{info, warn, LevelFilter};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};
use vortex::array::chunked::ChunkedArray;
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::IntoArray;
use vortex::array::{Array, ArrayRef};
use vortex::arrow::FromArrowType;
use vortex::compress::{CompressConfig, CompressCtx};
use vortex::encoding::{EncodingRef, ENCODINGS};
use vortex::formatter::display_tree;
use vortex_alp::ALPEncoding;
use vortex_datetime::DateTimeEncoding;
use vortex_dict::DictEncoding;
use vortex_fastlanes::{BitPackedEncoding, FoREncoding};
use vortex_ree::REEEncoding;
use vortex_roaring::RoaringBoolEncoding;
use vortex_schema::DType;

use crate::reader::BATCH_SIZE;
use crate::taxi_data::taxi_data_parquet;

pub mod data_downloads;
pub mod public_bi_data;
pub mod reader;
pub mod taxi_data;

/// Creates a file if it doesn't already exist.
/// NB: Does NOT modify the given path to ensure that it resides in the data directory.
pub fn idempotent<T, E, P: IdempotentPath + ?Sized>(
    path: &P,
    f: impl FnOnce(&Path) -> Result<T, E>,
) -> Result<PathBuf, E> {
    let data_path = path.to_data_path();
    if !data_path.exists() {
        let run_uuid = uuid::Uuid::new_v4();
        let temp_location = path.to_temp_path(temp_dir().join(run_uuid.to_string()));
        let temp_path = temp_location.as_path();
        f(temp_path)?;
        std::fs::rename(temp_path, &data_path).unwrap();
    }
    Ok(data_path)
}

pub trait IdempotentPath {
    fn to_data_path(&self) -> PathBuf;
    fn to_temp_path(&self, temp_dir: PathBuf) -> PathBuf;
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

    fn to_temp_path(&self, temp_dir: PathBuf) -> PathBuf {
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

    fn to_temp_path(&self, temp_dir: PathBuf) -> PathBuf {
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

pub fn enumerate_arrays() -> Vec<EncodingRef> {
    println!("FOUND {:?}", ENCODINGS.iter().map(|e| e.id()).collect_vec());
    vec![
        &ALPEncoding,
        &DictEncoding,
        &BitPackedEncoding,
        &FoREncoding,
        &DateTimeEncoding,
        // &DeltaEncoding,  Blows up the search space too much.
        &REEEncoding,
        &RoaringBoolEncoding,
        // RoaringIntEncoding,
        // Doesn't offer anything more than FoR really
        // ZigZagEncoding,
    ]
}

pub fn compress_ctx() -> CompressCtx {
    let cfg = CompressConfig::new().with_enabled(enumerate_arrays());
    info!("Compression config {cfg:?}");
    CompressCtx::new(Arc::new(cfg))
}

pub fn compress_taxi_data() -> ArrayRef {
    let file = File::open(taxi_data_parquet()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let _mask = ProjectionMask::roots(builder.parquet_schema(), [1]);
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

    let ctx = compress_ctx();
    let schema = reader.schema();
    let mut uncompressed_size: usize = 0;
    let chunks = reader
        .into_iter()
        .map(|batch_result| batch_result.unwrap())
        .map(|batch| batch.into_array())
        .map(|array| {
            uncompressed_size += array.nbytes();
            ctx.clone().compress(&array, None).unwrap()
        })
        .collect_vec();

    chunks_to_array(schema, uncompressed_size, chunks)
}

fn chunks_to_array(schema: SchemaRef, uncompressed_size: usize, chunks: Vec<ArrayRef>) -> ArrayRef {
    let dtype = DType::from_arrow(schema.clone());
    let compressed = ChunkedArray::new(chunks.clone(), dtype).into_array();

    warn!("Compressed array {}", display_tree(compressed.as_ref()));

    let mut field_bytes = vec![0; schema.fields().len()];
    for chunk in chunks {
        let str = chunk.as_struct();
        for (i, field) in str.fields().iter().enumerate() {
            field_bytes[i] += field.nbytes();
        }
    }
    field_bytes.iter().enumerate().for_each(|(i, &nbytes)| {
        println!("{},{}", schema.field(i).name(), nbytes);
    });
    println!(
        "{}, Bytes: {}, Ratio {}",
        humansize::format_size(compressed.nbytes(), DECIMAL),
        compressed.nbytes(),
        compressed.nbytes() as f32 / uncompressed_size as f32
    );

    compressed
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::ops::Deref;
    use std::sync::Arc;

    use arrow_array::{ArrayRef as ArrowArrayRef, StructArray as ArrowStructArray};
    use log::LevelFilter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use vortex::array::ArrayRef;
    use vortex::compute::as_arrow::as_arrow;
    use vortex::encode::FromArrowArray;
    use vortex::serde::{ReadCtx, WriteCtx};

    use crate::taxi_data::taxi_data_parquet;
    use crate::{compress_ctx, compress_taxi_data, setup_logger};

    #[ignore]
    #[test]
    fn compression_ratio() {
        setup_logger(LevelFilter::Debug);
        _ = compress_taxi_data();
    }

    #[ignore]
    #[test]
    fn round_trip_serde() {
        let file = File::open(taxi_data_parquet()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.with_limit(1).build().unwrap();

        for record_batch in reader.map(|batch_result| batch_result.unwrap()) {
            let struct_arrow: ArrowStructArray = record_batch.into();
            let arrow_array: ArrowArrayRef = Arc::new(struct_arrow);
            let vortex_array = ArrayRef::from_arrow(arrow_array.clone(), false);

            let mut buf = Vec::<u8>::new();
            let mut write_ctx = WriteCtx::new(&mut buf);
            write_ctx.write(vortex_array.as_ref()).unwrap();

            let mut read = buf.as_slice();
            let mut read_ctx = ReadCtx::new(vortex_array.dtype(), &mut read);
            read_ctx.read().unwrap();
        }
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
            let vortex_array = ArrayRef::from_arrow(arrow_array.clone(), false);
            let vortex_as_arrow = as_arrow(vortex_array.as_ref()).unwrap();
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

        let ctx = compress_ctx();
        for record_batch in reader.map(|batch_result| batch_result.unwrap()) {
            let struct_arrow: ArrowStructArray = record_batch.into();
            let arrow_array: ArrowArrayRef = Arc::new(struct_arrow);
            let vortex_array = ArrayRef::from_arrow(arrow_array.clone(), false);

            let compressed = ctx.clone().compress(vortex_array.as_ref(), None).unwrap();
            let compressed_as_arrow = as_arrow(compressed.as_ref()).unwrap();
            assert_eq!(compressed_as_arrow.deref(), arrow_array.deref());
        }
    }
}
