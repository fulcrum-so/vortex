use std::hint::black_box;

use bench_vortex::reader::{take_parquet, take_vortex};
use bench_vortex::taxi_data::{taxi_data_parquet, taxi_data_vortex};
use divan::Bencher;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

static INDICES: [u64; 6] = [10, 11, 12, 13, 100_000, 3_000_000];

#[divan::bench]
fn vortex(bencher: Bencher) {
    let taxi_vortex = taxi_data_vortex();
    bencher.bench(|| black_box(take_vortex(&taxi_vortex, &INDICES)));
}

#[divan::bench]
fn arrow(bencher: Bencher) {
    let taxi_parquet = taxi_data_parquet();
    bencher.bench(|| black_box(take_parquet(&taxi_parquet, &INDICES)));
}

fn main() {
    divan::main();
}
