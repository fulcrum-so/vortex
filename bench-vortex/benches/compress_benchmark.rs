use criterion::{black_box, criterion_group, criterion_main, Criterion};

use bench_vortex::compress_taxi_data;
use bench_vortex::taxi_data::download_taxi_data;

fn vortex_compress(c: &mut Criterion) {
    download_taxi_data();
    let mut group = c.benchmark_group("end to end");
    group.sample_size(10);
    group.bench_function("compress", |b| b.iter(|| black_box(compress_taxi_data())));
    group.finish()
}

criterion_group!(benches, vortex_compress);
criterion_main!(benches);
