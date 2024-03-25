use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatchReader;
use itertools::Itertools;
use log::{info, warn, LevelFilter};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};

use vortex::array::chunked::ChunkedArray;
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::IntoArray;
use vortex::array::{Array, ArrayRef, EncodingRef, ENCODINGS};
use vortex::arrow::FromArrowType;
use vortex::compress::{CompressConfig, CompressCtx};
use vortex::formatter::display_tree;
use vortex_alp::ALPEncoding;
use vortex_datetime::DateTimeEncoding;
use vortex_dict::DictEncoding;
use vortex_fastlanes::{BitPackedEncoding, DeltaEncoding, FoREncoding};
use vortex_ree::REEEncoding;
use vortex_roaring::RoaringBoolEncoding;
use vortex_schema::DType;

pub mod taxi_data;

pub fn idempotent(name: &str, f: impl FnOnce(&mut File)) -> PathBuf {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join(name);
    // if !path.exists() {
    create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = File::create(&path).unwrap();
    f(&mut file);
    // }
    path.to_path_buf()
}

#[allow(dead_code)]
fn setup_logger(level: LevelFilter) {
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
        &DeltaEncoding,
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
    let file = File::open(taxi_data::download_taxi_data()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let _mask = ProjectionMask::roots(builder.parquet_schema(), [1]);
    let _no_datetime_mask = ProjectionMask::roots(
        builder.parquet_schema(),
        [0, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
    );
    let reader = builder
        .with_projection(_mask)
        //.with_projection(no_datetime_mask)
        .with_batch_size(65_536)
        // .with_batch_size(5_000_000)
        // .with_limit(100_000)
        .build()
        .unwrap();

    let ctx = compress_ctx();
    let schema = reader.schema();
    let mut uncompressed_size = 0;
    let chunks = reader
        .into_iter()
        //.skip(39)
        //.take(1)
        .map(|batch_result| batch_result.unwrap())
        .map(|batch| batch.into_array())
        .map(|array| {
            uncompressed_size += array.nbytes();
            ctx.clone().compress(&array, None).unwrap()
        })
        .collect_vec();

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
        "NBytes {}, Ratio {}",
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

    use crate::taxi_data::download_taxi_data;
    use crate::{compress_ctx, compress_taxi_data, setup_logger};

    #[test]
    fn compression_ratio() {
        setup_logger(LevelFilter::Debug);
        _ = compress_taxi_data();
    }

    #[ignore]
    #[test]
    fn round_trip_serde() {
        let file = File::open(download_taxi_data()).unwrap();
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
        let file = File::open(download_taxi_data()).unwrap();
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
        let file = File::open(download_taxi_data()).unwrap();
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
