use criterion::{black_box, criterion_group, criterion_main, Criterion};
use itertools::Itertools;

use bench_vortex::serde::{take_taxi_data, write_taxi_data};
use vortex::array::ENCODINGS;

fn random_access(c: &mut Criterion) {
    let taxi_spiral = write_taxi_data();
    let indices = [10, 11, 12, 13, 100_000, 3_000_000];
    println!(
        "ENCODINGS {:?}",
        ENCODINGS.iter().map(|e| e.id()).collect_vec()
    );
    c.bench_function("random access", |b| {
        b.iter(|| black_box(take_taxi_data(&taxi_spiral, &indices)))
    });
}

criterion_group!(benches, random_access);
criterion_main!(benches);
