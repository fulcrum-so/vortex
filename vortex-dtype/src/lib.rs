use std::fmt::{Display, Formatter};

pub use half;

pub use deserialize::*;
pub use dtype::*;
pub use ptype::*;

mod deserialize;
mod dtype;
mod ptype;
mod serde;
mod serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct CompositeID(pub &'static str);

impl Display for CompositeID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub mod flatbuffers {
    pub use generated::vortex::dtype::*;

    #[allow(clippy::all)]
    #[allow(clippy::unwrap_used)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(unsafe_op_in_unsafe_fn)]
    #[allow(unused_imports)]
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/dtype.rs"));
    }
}
