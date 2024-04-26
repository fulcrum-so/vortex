use std::io::Cursor;
use std::sync::Arc;

use arrow::ipc::reader::StreamReader as ArrowStreamReader;
use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter as ArrowStreamWriter};
use arrow_ipc::MetadataVersion;
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::take::take;
use vortex::{IntoArray, SerdeContext};
use vortex_ipc::iter::FallibleLendingIterator;
use vortex_ipc::reader::StreamReader;
use vortex_ipc::writer::StreamWriter;

fn ipc_take(c: &mut Criterion) {
    let mut group = c.benchmark_group("ipc_take");
    let indices = Int32Array::from(vec![10, 11, 12, 13, 100_000, 2_999_999]);
    group.bench_function("arrow", |b| {
        let mut buffer = vec![];
        {
            let field = Field::new("uid", DataType::Int32, true);
            let schema = Schema::new(vec![field]);
            let options = IpcWriteOptions::try_new(32, false, MetadataVersion::V5).unwrap();
            let mut writer =
                ArrowStreamWriter::try_new_with_options(&mut buffer, &schema, options).unwrap();
            let array = Int32Array::from(vec![5i32; 3_000_000]);

            let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
            writer.write(&batch).unwrap();
        }

        b.iter(|| {
            let mut cursor = Cursor::new(&buffer);
            let mut reader = ArrowStreamReader::try_new(&mut cursor, None).unwrap();
            let batch = reader.next().unwrap().unwrap();
            let array_from_batch = batch.column(0);
            let array = array_from_batch
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            black_box(arrow_select::take::take(array, &indices, None).unwrap());
        });
    });

    group.bench_function("vortex", |b| {
        let indices = PrimitiveArray::from(vec![10, 11, 12, 13, 100_000, 2_999_999]).into_array();
        let data = PrimitiveArray::from(vec![5; 3_000_000]).into_array();

        // Try running take over an ArrayView.
        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            let mut writer = StreamWriter::try_new(&mut cursor, SerdeContext::default()).unwrap();
            writer.write_array(&data).unwrap();
        }
        b.iter(|| {
            let mut cursor = Cursor::new(&buffer);
            let mut reader = StreamReader::try_new(&mut cursor).unwrap();
            let mut array_reader = reader.next().unwrap().unwrap();
            let array_view = array_reader.next().unwrap().unwrap();
            black_box(take(&array_view, &indices))
        });
    });
}

criterion_group!(benches, ipc_take);
criterion_main!(benches);
