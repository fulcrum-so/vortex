#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

use linkme::distributed_slice;

pub use bitpacking::*;
pub use delta::*;
pub use ffor::*;
pub use r#for::*;
use vortex::array::{EncodingRef, ENCODINGS};

mod bitpacking;
mod delta;
mod ffor;
mod r#for;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_BITPACKING: EncodingRef = &BitPackedEncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_DELTA: EncodingRef = &DeltaEncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_FOR: EncodingRef = &FoREncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_FL_FFOR: EncodingRef = &FFoREncoding;
