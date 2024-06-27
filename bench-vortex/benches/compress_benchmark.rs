use std::hint::black_box;

use bench_vortex::compress_taxi_data;
use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::Medicare1;
use bench_vortex::taxi_data::taxi_data_parquet;
use divan::Bencher;

#[divan::bench]
fn vortex_compress_taxi(bencher: Bencher) {
    taxi_data_parquet();
    bencher.bench(|| black_box(compress_taxi_data()));
}

#[divan::bench]
fn vortex_compress_medicare1(bencher: Bencher) {
    let dataset = BenchmarkDatasets::PBI(Medicare1);
    dataset.as_uncompressed();
    bencher.bench(|| black_box(dataset.compress_to_vortex()));
}

fn main() {
    divan::main();
}
