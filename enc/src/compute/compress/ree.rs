use half::f16;

use codecz::AlignedAllocator;

use crate::array::primitive::PrimitiveArray;
use crate::array::ree::REEArray;
use crate::array::ree::REEEncoding;
use crate::array::stats::Stat;
use crate::array::{Array, ArrayEncoding, Encoding};
use crate::compute::compress::{
    compress, CompressConfig, CompressCtx, CompressedEncoding, Compressor,
};
use crate::types::{match_each_native_ptype, PType};

impl CompressedEncoding for REEEncoding {
    fn compressor(&self, array: &Array, config: &CompressConfig) -> Option<&'static Compressor> {
        if !config.is_enabled(self.id()) {
            return None;
        }

        if let Array::Primitive(_) = array {
            if array.len() as f32
                / array
                    .stats()
                    .get_or_compute_or::<usize>(array.len(), &Stat::RunCount)
                    as f32
                >= config.ree_average_run_threshold
            {
                return Some(&(ree_compress as Compressor));
            }
        }

        None
    }
}

fn ree_compress(array: &Array, opts: CompressCtx) -> Array {
    match array {
        Array::Primitive(p) => ree_compress_primitive_array(p, opts),
        _ => unimplemented!(),
    }
}
fn ree_compress_primitive_array(array: &PrimitiveArray, ctx: CompressCtx) -> Array {
    match_each_native_ptype!(array.ptype(), |$P| {
        let (values, runs) = codecz::ree::encode(array.buffer().typed_data::<$P>()).unwrap();
        let compressed_values = compress(&Array::Primitive(PrimitiveArray::from_vec_in::<$P, AlignedAllocator>(values)), ctx.next_level());
        let compressed_ends = compress(&Array::Primitive(PrimitiveArray::from_vec_in::<u32, AlignedAllocator>(runs)), ctx.next_level());
        Array::REE(REEArray::new(compressed_ends, compressed_values))
    })
}
