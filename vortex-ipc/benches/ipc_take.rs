use std::io::Cursor;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::take::take;
use vortex::{IntoArray, SerdeContext};
use vortex_ipc::iter::FallibleLendingIterator;
use vortex_ipc::reader::StreamReader;
use vortex_ipc::writer::StreamWriter;

// 100 record batches, 100k rows each
// take from the first 20 batches and last batch
// compare with arrow
fn ipc_take(c: &mut Criterion) {
    let indices = PrimitiveArray::from(vec![10, 11, 12, 13, 100_000, 2_999_999]).into_array();
    let data = PrimitiveArray::from(vec![5; 3_000_000]).into_array();

    // Try running take over an ArrayView.
    let mut buffer = vec![];
    {
        let mut cursor = Cursor::new(&mut buffer);
        let mut writer = StreamWriter::try_new(&mut cursor, SerdeContext::default()).unwrap();
        writer.write_array(&data).unwrap();
    }

    c.bench_function("take_view", |b| {
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
