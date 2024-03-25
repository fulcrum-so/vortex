#![allow(unused_imports)]
#![allow(dead_code)]

mod gen_batch {
    include!(concat!(env!("OUT_DIR"), "/flatbuffers/batch_generated.rs"));
}
pub use gen_batch::vortex::batch;

mod gen_encoding {
    include!(concat!(
        env!("OUT_DIR"),
        "/flatbuffers/encoding_generated.rs"
    ));
}
pub use gen_encoding::vortex::encoding;

mod gen_flat {
    include!(concat!(env!("OUT_DIR"), "/flatbuffers/flat_generated.rs"));
}
pub use gen_flat::vortex::flat;

mod gen_ree {
    include!(concat!(env!("OUT_DIR"), "/flatbuffers/ree_generated.rs"));
}
pub use gen_ree::vortex::ree;
