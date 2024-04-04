use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::distributions::{Alphanumeric, Uniform};
use rand::prelude::SliceRandom;
use rand::{thread_rng, Rng};
use vortex::array::primitive::{PrimitiveArray, PrimitiveTrait};
use vortex::array::varbin::VarBinArray;
use vortex::array::Array;
use vortex::match_each_native_ptype;
use vortex_dict::dict_encode_typed_primitive;

fn gen_primitive_dict(len: usize, uniqueness: f64) -> PrimitiveArray {
    let mut rng = thread_rng();
    let value_range = len as f64 * uniqueness;
    let range = Uniform::new(-(value_range / 2.0) as i32, (value_range / 2.0) as i32);
    let data: Vec<i32> = (0..len).map(|_| rng.sample(range)).collect();

    PrimitiveArray::from(data)
}

fn gen_varbin_dict(len: usize, uniqueness: f64) -> VarBinArray {
    let mut rng = thread_rng();
    let uniq_cnt = (len as f64 * uniqueness) as usize;
    let dict: Vec<String> = (0..uniq_cnt)
        .map(|_| {
            (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(16)
                .map(char::from)
                .collect()
        })
        .collect();
    let words: Vec<&str> = (0..len)
        .map(|_| dict.choose(&mut rng).unwrap().as_str())
        .collect();
    VarBinArray::from(words)
}

fn dict_encode_primitive(arr: &PrimitiveArray) -> usize {
    let (codes, values) = match_each_native_ptype!(arr.ptype(), |$P| {
        dict_encode_typed_primitive::<$P>(arr)
    });
    (codes.nbytes() + values.nbytes()) / arr.nbytes()
}

fn dict_encode_varbin(arr: &VarBinArray) -> usize {
    let (codes, values) = vortex_dict::dict_encode_varbin(arr);
    (codes.nbytes() + values.nbytes()) / arr.nbytes()
}

fn dict_encode(c: &mut Criterion) {
    let primitive_arr = gen_primitive_dict(1_000_000, 0.00005);
    let varbin_arr = gen_varbin_dict(1_000_000, 0.00005);

    c.bench_function("dict_encode_primitives", |b| {
        b.iter(|| black_box(dict_encode_primitive(&primitive_arr)));
    });
    c.bench_function("dict_encode_varbin", |b| {
        b.iter(|| black_box(dict_encode_varbin(&varbin_arr)));
    });
}

criterion_group!(benches, dict_encode);
criterion_main!(benches);
