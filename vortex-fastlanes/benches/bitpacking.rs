use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fastlanez::TryBitPack;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use vortex::array::primitive::PrimitiveArray;
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::take::take;
use vortex::encoding::EncodingRef;
use vortex_fastlanes::{
    bitpack_primitive, unpack_primitive, unpack_single_primitive, BitPackedEncoding,
};

fn values(len: usize, bits: usize) -> Vec<u32> {
    let rng = thread_rng();
    let range = Uniform::new(0_u32, 2_u32.pow(bits as u32));
    rng.sample_iter(range).take(len).collect()
}

fn unpack_singles(packed: &[u8], bit_width: usize, length: usize) -> Vec<u32> {
    let mut output = Vec::with_capacity(length);
    for i in 0..length {
        unsafe {
            output.push(unpack_single_primitive(packed, bit_width, i).unwrap());
        }
    }
    output
}

fn pack_unpack(c: &mut Criterion) {
    let bits: usize = 8;
    let values = values(1_000_000, bits);

    c.bench_function("bitpack_1M", |b| {
        b.iter(|| black_box(bitpack_primitive(&values, bits)));
    });

    let packed = bitpack_primitive(&values, bits);
    let unpacked = unpack_primitive::<u32>(&packed, bits, values.len());
    assert_eq!(unpacked, values);

    c.bench_function("unpack_1M", |b| {
        b.iter(|| black_box(unpack_primitive::<u32>(&packed, bits, values.len())));
    });

    c.bench_function("unpack_1M_singles", |b| {
        b.iter(|| black_box(unpack_singles(&packed, 8, values.len())));
    });

    // 1024 elements pack into `128 * bits` bytes
    let packed_1024 = &packed[0..128 * bits];
    c.bench_function("unpack_1024_alloc", |b| {
        b.iter(|| black_box(unpack_primitive::<u32>(&packed, bits, values.len())));
    });

    let mut output: Vec<u32> = Vec::with_capacity(1024);
    c.bench_function("unpack_1024_noalloc", |b| {
        b.iter(|| {
            output.clear();
            TryBitPack::try_unpack_into(packed_1024, bits, &mut output).unwrap();
            black_box(output[0])
        })
    });

    c.bench_function("unpack_single", |b| {
        b.iter(|| black_box(unsafe { unpack_single_primitive::<u32>(packed_1024, 8, 0) }));
    });
}

fn bench_take(c: &mut Criterion) {
    let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
    let ctx = CompressCtx::new(Arc::new(cfg));

    let values = values(1_000_000, 8);
    let uncompressed = PrimitiveArray::from(values.clone());
    let packed = BitPackedEncoding {}
        .compress(&uncompressed, None, ctx)
        .unwrap();

    let scattered_indices: PrimitiveArray = (0..10).map(|i| i * 10_000).collect::<Vec<_>>().into();
    c.bench_function("take_10", |b| {
        b.iter(|| black_box(take(&packed, &scattered_indices).unwrap()));
    });
}

criterion_group!(benches, pack_unpack, bench_take);
criterion_main!(benches);
