use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use vortex::array::r#struct::StructArray;
use vortex::validity::Validity;
use vortex::IntoArray;
use vortex_dtype::field_paths::{field, FieldPath};
use vortex_error::VortexError;
use vortex_expr::expressions::{lit, Conjunction, Disjunction};
use vortex_expr::field_paths::FieldPathOperations;
use vortex_expr::operators::{field_comparison, Operator};

fn filter_indices_primitive(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_indices_primitive");

    let mut rng = thread_rng();
    let range = Uniform::new(0i64, 100_000_000);
    let arr = (0..10_000_000)
        .map(|_| rng.sample(range))
        .collect_vec()
        .into_array();

    let predicate = Disjunction {
        conjunctions: vec![Conjunction {
            predicates: vec![FieldPath::builder().build().lt(lit(50_000_000i64))],
        }],
    };

    group.bench_function("vortex", |b| {
        b.iter(|| {
            let indices =
                vortex::compute::filter_indices::filter_indices(&arr, &predicate).unwrap();
            black_box(indices);
            Ok::<(), VortexError>(())
        });
    });
}

fn filter_indices_struct(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_indices_struct");

    let mut rng = thread_rng();
    let range = Uniform::new(0i64, 100_000_000);
    let arr = (0..10_000_000)
        .map(|_| rng.sample(range))
        .collect_vec()
        .into_array();
    let arr2 = (0..10_000_000)
        .map(|_| rng.sample(range))
        .collect_vec()
        .into_array();

    let structs = StructArray::try_new(
        Arc::new([Arc::from("field_a"), Arc::from("field_b")]),
        vec![arr, arr2.clone()],
        arr2.len(),
        Validity::AllValid,
    )
    .unwrap()
    .into_array();
    let predicate = field_comparison(Operator::LessThan, field("field_a"), field("field_b"));

    group.bench_function("vortex", |b| {
        b.iter(|| {
            let indices =
                vortex::compute::filter_indices::filter_indices(&structs, &predicate).unwrap();
            black_box(indices);
            Ok::<(), VortexError>(())
        });
    });
}

criterion_group!(benches, filter_indices_primitive, filter_indices_struct);
criterion_main!(benches);
