use linkme::distributed_slice;

pub use array::*;
pub use typed::*;
use vortex_schema::CompositeID;

mod array;
mod compress;
mod compute;
mod serde;
mod typed;

#[distributed_slice]
pub static COMPOSITE_EXTENSIONS: [&'static dyn CompositeExtension] = [..];

pub fn find_extension(id: &str) -> Option<&'static dyn CompositeExtension> {
    COMPOSITE_EXTENSIONS
        .iter()
        .find(|ext| ext.id().0 == id)
        .copied()
}

pub fn find_extension_id(id: &str) -> Option<CompositeID> {
    find_extension(id).map(|e| e.id())
}
