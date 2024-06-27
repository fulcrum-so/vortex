use std::sync::Arc;

use arrow_array::builder::{StringBuilder, UInt32Builder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use criterion::measurement::Measurement;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkGroup, Criterion};
use datafusion::common::Result as DFResult;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::memory_pool::human_readable_size;
use datafusion::logical_expr::lit;
use datafusion::prelude::{col, count_distinct, DataFrame, SessionContext};
use divan::Bencher;
use lazy_static::lazy_static;
use vortex::compress::Compressor;
use vortex::encoding::EncodingRef;
use vortex::{Array, Context, IntoArray, ToArrayData};
use vortex_datafusion::{VortexMemTable, VortexMemTableOptions};
use vortex_dict::DictEncoding;
use vortex_fastlanes::{BitPackedEncoding, DeltaEncoding, FoREncoding};

lazy_static! {
    pub static ref CTX: Context = Context::default().with_encodings([
        &BitPackedEncoding as EncodingRef,
        &DictEncoding,
        &FoREncoding,
        &DeltaEncoding,
    ]);
}

fn toy_dataset_arrow() -> RecordBatch {
    // 64,000 rows of string and numeric data.
    // 8,000 values of first string, second string, third string, etc.

    let names = [
        "Alexander",
        "Anastasia",
        "Archibald",
        "Bartholomew",
        "Benjamin",
        "Christopher",
        "Elizabeth",
        "Gabriella",
    ];

    let mut col1 = StringBuilder::with_capacity(640_000, 64_000_000);
    let mut col2 = UInt32Builder::with_capacity(640_000);
    for i in 0..640_000 {
        col1.append_value(names[i % 8]);
        col2.append_value(u32::try_from(i).unwrap());
    }

    let col1 = col1.finish();
    let col2 = col2.finish();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("names", DataType::Utf8, false),
            Field::new("scores", DataType::UInt32, false),
        ])),
        vec![Arc::new(col1), Arc::new(col2)],
    )
    .unwrap()
}

fn toy_dataset_vortex(compress: bool) -> Array {
    let uncompressed = toy_dataset_arrow().to_array_data().into_array();

    if !compress {
        return uncompressed;
    }

    println!(
        "uncompressed size: {:?}",
        human_readable_size(uncompressed.nbytes())
    );
    let compressor = Compressor::new(&CTX);
    let compressed = compressor.compress(&uncompressed, None).unwrap();
    println!(
        "vortex compressed size: {:?}",
        human_readable_size(compressed.nbytes())
    );
    compressed
}

fn filter_agg_query(df: DataFrame) -> DFResult<DataFrame> {
    // SELECT SUM(scores) FROM table WHERE scores >= 3000 AND scores <= 4000
    df.filter(col("scores").gt_eq(lit(3_000)))?
        .filter(col("scores").lt_eq(lit(4_000)))?
        .aggregate(vec![], vec![count_distinct(col("names"))])
}

fn measure_provider<M: Measurement>(bencher: Bencher, table: Arc<dyn TableProvider>) {
    bencher.bench(|| {
        tokio::sync::Runtime::new()
            .unwrap()
            .block_on(async {
                let session = Arc::new(SessionContext::new());
                let df = session.read_table(table.clone()).unwrap();
                filter_agg_query(df).unwrap().collect().await.unwrap();
            })
        // Force physical planner to execute on our TableProvider.
        filter_agg_query(black_box(session).read_table(table.clone()).unwrap())
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
    });

    group.bench_function("exec", |b| {
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        )
        .iter(|| async {
            // Force full query execution with .collect()
            filter_agg_query(black_box(session).read_table(table.clone()).unwrap())
                .unwrap()
                .collect()
                .await
                .unwrap();
        });
    });
}

fn bench_arrow<M: Measurement>(mut group: BenchmarkGroup<M>, session: &SessionContext) {
    let arrow_dataset = toy_dataset_arrow();
    let arrow_table =
        Arc::new(MemTable::try_new(arrow_dataset.schema(), vec![vec![arrow_dataset]]).unwrap());

    measure_provider(&mut group, session, arrow_table);
}

fn bench_vortex(bencher: Bencher, disable_pushdown: bool, compress: bool) {
    let vortex_dataset = toy_dataset_vortex(compress);
    let vortex_table = Arc::new(
        VortexMemTable::try_new(
            vortex_dataset,
            VortexMemTableOptions::default().with_disable_pushdown(disable_pushdown),
        )
        .unwrap(),
    );

    measure_provider(&mut group, session, vortex_table);
}

#[divan::bench]
fn vortex_pushdown_compressed(bencher: Bencher) {
    // compress=true, pushdown enabled
    bench_vortex(bencher, false, true);
}

#[divan::bench]
fn vortex_pushdown_uncompressed(bencher: Bencher) {
    // compress=true, pushdown enabled
    bench_vortex(bencher, false, false);
}

#[divan::bench]
fn vortex_nopushdown_compressed(bencher: Bencher) {
    // compress=true, pushdown enabled
    bench_vortex(bencher, true, true);
}

#[divan::bench]
fn vortex_nopushdown_uncompressed(bencher: Bencher) {
    // compress=true, pushdown enabled
    bench_vortex(bencher, true, false);
}

fn main() {
    divan::main();
}
