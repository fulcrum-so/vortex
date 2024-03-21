#![allow(unused_imports)]
#![allow(dead_code)]

mod gen_batch {
    include!(concat!(env!("OUT_DIR"), "/flatbuffers/batch_generated.rs"));
}
pub use gen_batch::vortex::batch;

mod gen_column {
    include!(concat!(env!("OUT_DIR"), "/flatbuffers/column_generated.rs"));
}
pub use gen_column::vortex::column;

mod gen_primitive {
    include!(concat!(
        env!("OUT_DIR"),
        "/flatbuffers/primitive_generated.rs"
    ));
}
pub use gen_primitive::vortex::primitive;

mod gen_ree {
    include!(concat!(env!("OUT_DIR"), "/flatbuffers/ree_generated.rs"));
}
pub use gen_ree::vortex::ree;
