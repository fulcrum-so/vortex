use criterion::{black_box, criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use vortex::array::chunked::ChunkedArray;
use vortex::IntoArray;
use vortex_error::VortexError;

fn scalar_subtract(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalar_subtract");

    let mut rng = thread_rng();
    let range = Uniform::new(0i64, 100_000_000);
    let data1 = (0..10_000_000)
        .map(|_| rng.sample(range))
        .collect_vec()
        .into_array();
    let data2 = (0..10_000_000)
        .map(|_| rng.sample(range))
        .collect_vec()
        .into_array();

    let to_subtract = -1i64;

    let chunked = ChunkedArray::from_iter(vec![data1.clone(), data2]).into_array();

    group.bench_function("vortex", |b| {
        b.iter(|| {
            let array =
                vortex::compute::scalar_subtract::scalar_subtract(&chunked, to_subtract).unwrap();

            let chunked = ChunkedArray::try_from(array).unwrap();
            let mut chunks_out = chunked.chunks();
            let results = chunks_out
                .next()
                .unwrap()
                .flatten_primitive()?
                .typed_data::<i64>()
                .to_vec();
            black_box(results);
            let results = chunks_out
                .next()
                .unwrap()
                .flatten_primitive()?
                .typed_data::<i64>()
                .to_vec();
            black_box(results);
            Ok::<(), VortexError>(())
        });
    });
}

criterion_group!(benches, scalar_subtract);
criterion_main!(benches);
