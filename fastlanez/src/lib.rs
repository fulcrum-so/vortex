#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

pub use bitpack::*;
pub use delta::*;
pub use fl::*;
pub use transpose::*;

mod bitpack;
mod delta;
mod fl;
mod transpose;

pub struct Pred<const B: bool>;

pub trait Satisfied {}

impl Satisfied for Pred<true> {}
