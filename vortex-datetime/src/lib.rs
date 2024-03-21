use linkme::distributed_slice;

pub use datetime::*;
use vortex::array::{EncodingRef, ENCODINGS};

mod compress;
mod datetime;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_DATETIME: EncodingRef = &DateTimeEncoding;
