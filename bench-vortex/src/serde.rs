use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, RecordBatchReader, StructArray as ArrowStructArray};
use itertools::Itertools;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use vortex::array::chunked::ChunkedArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::ArrayRef;
use vortex::arrow::FromArrowType;
use vortex::compute::take::take;
use vortex::encode::FromArrowArray;
use vortex::ptype::PType;
use vortex::serde::{ReadCtx, WriteCtx};
use vortex_schema::DType;

use crate::taxi_data::download_taxi_data;
use crate::{compress_ctx, idempotent};

pub fn write_taxi_data() -> PathBuf {
    idempotent("taxi.spiral", |write| {
        let taxi_pq = File::open(download_taxi_data()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(taxi_pq).unwrap();

        // FIXME(ngates): the compressor should handle batch size.
        let reader = builder.with_batch_size(65_536).build().unwrap();

        let dtype = DType::from_arrow(reader.schema());
        let ctx = compress_ctx();

        let chunks = reader
            .map(|batch_result| batch_result.unwrap())
            .map(|record_batch| {
                let struct_arrow: ArrowStructArray = record_batch.into();
                let arrow_array: ArrowArrayRef = Arc::new(struct_arrow);
                let vortex_array = ArrayRef::from_arrow(arrow_array.clone(), false);
                ctx.compress(&vortex_array, None).unwrap()
            })
            .collect_vec();
        let chunked = ChunkedArray::new(chunks, dtype.clone());

        let mut write_ctx = WriteCtx::new(write);
        write_ctx.dtype(&dtype).unwrap();
        write_ctx.write(&chunked).unwrap();
    })
}

pub fn take_taxi_data(path: &Path, indices: &[u64]) -> ArrayRef {
    let chunked = {
        let mut file = File::open(path).unwrap();
        let dummy_dtype: DType = PType::U8.into();
        let mut read_ctx = ReadCtx::new(&dummy_dtype, &mut file);
        let dtype = read_ctx.dtype().unwrap();
        read_ctx.with_schema(&dtype).read().unwrap()
    };
    take(&chunked, &PrimitiveArray::from(indices.to_vec())).unwrap()
}

#[cfg(test)]
mod test {

    use log::LevelFilter;
    use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};

    use crate::serde::{take_taxi_data, write_taxi_data};

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

    #[ignore]
    #[test]
    fn round_trip_serde() {
        let taxi_spiral = write_taxi_data();
        let rows = take_taxi_data(&taxi_spiral, &[10, 11, 12, 13, 100_000, 3_000_000]);
        println!("TAKE TAXI DATA: {:?}", rows);
    }
}
