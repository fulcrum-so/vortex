use arrow_array::RecordBatchReader;
use itertools::Itertools;
use log::info;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use vortex::array::bool::BoolEncoding;
use vortex::array::chunked::{ChunkedArray, ChunkedEncoding};
use vortex::array::constant::ConstantEncoding;

use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveEncoding;
use vortex::array::sparse::SparseEncoding;
use vortex::array::struct_::StructEncoding;
use vortex::array::typed::TypedEncoding;
use vortex::array::varbin::VarBinEncoding;
use vortex::array::varbinview::VarBinViewEncoding;
use vortex::array::{Array, ArrayRef, Encoding};
use vortex::compress::{CompressConfig, CompressCtx};
use vortex::dtype::DType;
use vortex::formatter::display_tree;
use vortex_alp::ALPEncoding;
use vortex_dict::DictEncoding;
use vortex_fastlanes::{BitPackedEncoding, DeltaEncoding, FoREncoding};
use vortex_ree::REEEncoding;
use vortex_roaring::RoaringBoolEncoding;

pub fn enumerate_arrays() -> Vec<&'static dyn Encoding> {
    vec![
        // TODO(ngates): fix https://github.com/fulcrum-so/vortex/issues/35
        // Builtins
        &BoolEncoding,
        &ChunkedEncoding,
        &ConstantEncoding,
        &PrimitiveEncoding,
        &SparseEncoding,
        &StructEncoding,
        &TypedEncoding,
        &VarBinEncoding,
        &VarBinViewEncoding,
        // Encodings
        &ALPEncoding,
        &DictEncoding,
        &BitPackedEncoding,
        &FoREncoding,
        &DeltaEncoding,
        // &FFoREncoding,
        &REEEncoding,
        &RoaringBoolEncoding,
        // &RoaringIntEncoding,
        // Doesn't offer anything more than FoR really
        //&ZigZagEncoding,
    ]
}

pub fn download_taxi_data() -> PathBuf {
    let download_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("data/yellow-tripdata-2023-11.parquet");
    if download_path.exists() {
        return download_path;
    }

    create_dir_all(download_path.parent().unwrap()).unwrap();
    let mut download_file = File::create(&download_path).unwrap();
    reqwest::blocking::get(
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet",
    )
    .unwrap()
    .copy_to(&mut download_file)
    .unwrap();

    download_path
}

pub fn compress_taxi_data() -> ArrayRef {
    let file = File::open(download_taxi_data()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mask = ProjectionMask::roots(builder.parquet_schema(), [16]);
    let _no_datetime_mask = ProjectionMask::roots(
        builder.parquet_schema(),
        [0, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
    );
    let reader = builder
        .with_projection(mask)
        //.with_projection(no_datetime_mask)
        .with_batch_size(65_536)
        // .with_batch_size(5_000_000)
        //.with_limit(100_000)
        .build()
        .unwrap();

    // let array = ArrayRef::try_from((&mut reader) as &mut dyn RecordBatchReader).unwrap();
    let cfg = CompressConfig::new(
        HashSet::from_iter(enumerate_arrays().iter().map(|e| (*e).id())),
        HashSet::default(),
    );
    println!("Compression config {cfg:?}");
    let ctx = CompressCtx::new(Arc::new(cfg));

    let schema = reader.schema();
    let mut uncompressed_size = 0;
    let chunks = reader
        .into_iter()
        //.skip(39)
        //.take(1)
        .map(|batch_result| batch_result.unwrap())
        .map(ArrayRef::from)
        .map(|array| {
            uncompressed_size += array.nbytes();
            ctx.compress(array.as_ref(), None).unwrap()
        })
        .collect_vec();

    let dtype: DType = schema.clone().try_into().unwrap();
    let compressed = ChunkedArray::new(chunks.clone(), dtype).boxed();

    info!("Compressed array {}", display_tree(compressed.as_ref()));
    info!(
        "NBytes {}, Ratio {}",
        compressed.nbytes(),
        compressed.nbytes() as f32 / uncompressed_size as f32
    );

    let mut field_bytes = vec![0; schema.fields().len()];
    for chunk in chunks {
        let str = chunk.as_struct();
        for (i, field) in str.fields().iter().enumerate() {
            field_bytes[i] += field.nbytes();
        }
    }
    field_bytes.iter().enumerate().for_each(|(i, &nbytes)| {
        info!("{},{}", schema.field(i).name(), nbytes);
    });

    compressed
}

#[cfg(test)]
mod test {
    use log::LevelFilter;
    use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};

    use crate::compress_taxi_data;

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

    #[test]
    fn compression_ratio() {
        setup_logger(LevelFilter::Debug);
        _ = compress_taxi_data();
    }
}
