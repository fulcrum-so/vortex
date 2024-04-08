#![allow(dead_code)]

pub mod array;
pub mod compute;
mod context;
mod data;
pub mod encoding;
mod implementation;
mod metadata;
mod tree;
mod validity;
mod view;
mod visitor;

use std::fmt::{Debug, Display, Formatter};

use arrow_buffer::Buffer;
pub use context::*;
pub use data::*;
pub use implementation::*;
pub use metadata::*;
pub use view::*;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::validity::ArrayValidity;
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};

#[derive(Debug, Clone)]
pub enum Array<'v> {
    Data(ArrayData),
    DataRef(&'v ArrayData),
    View(ArrayView<'v>),
}

impl Array<'_> {
    pub fn encoding(&self) -> EncodingRef {
        match self {
            Array::Data(d) => d.encoding(),
            Array::DataRef(d) => d.encoding(),
            Array::View(v) => v.encoding(),
        }
    }

    pub fn dtype(&self) -> &DType {
        match self {
            Array::Data(d) => d.dtype(),
            Array::DataRef(d) => d.dtype(),
            Array::View(v) => v.dtype(),
        }
    }

    pub fn len(&self) -> usize {
        self.with_array(|a| a.len())
    }
}

pub trait ToArray {
    fn to_array(&self) -> Array;
}

pub trait IntoArray<'a> {
    fn into_array(self) -> Array<'a>;
}

pub trait ToArrayData {
    fn to_array_data(&self) -> ArrayData;
}

pub trait WithArray {
    fn with_array<R, F: FnMut(&dyn ArrayTrait) -> R>(&self, f: F) -> R;
}

pub trait ArrayParts<'a> {
    fn dtype(&'a self) -> &'a DType;
    fn buffer(&'a self, idx: usize) -> Option<&'a Buffer>;
    fn child(&'a self, idx: usize, dtype: &'a DType) -> Option<Array<'a>>;
}

pub trait TryFromArrayParts<'v, M: ArrayMetadata>: Sized + 'v {
    fn try_from_parts(parts: &'v dyn ArrayParts<'v>, metadata: &'v M) -> VortexResult<Self>;
}

/// Collects together the behaviour of an array.
pub trait ArrayTrait: ArrayCompute + ArrayValidity + AcceptArrayVisitor + ToArrayData {
    fn dtype(&self) -> &DType;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        // TODO(ngates): remove this default impl to encourage explicit implementation
        self.len() == 0
    }

    fn nbytes(&self) -> usize {
        let mut visitor = NBytesVisitor(0);
        self.accept(&mut visitor).unwrap();
        visitor.0
    }
}

struct NBytesVisitor(usize);
impl ArrayVisitor for NBytesVisitor {
    fn visit_array(&mut self, _name: &str, array: &Array) -> VortexResult<()> {
        self.0 += array.with_array(|a| a.nbytes());
        Ok(())
    }

    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()> {
        self.0 += buffer.len();
        Ok(())
    }
}

impl ToArrayData for Array<'_> {
    fn to_array_data(&self) -> ArrayData {
        match self {
            Array::Data(d) => d.encoding().with_data(d, |a| a.to_array_data()),
            Array::DataRef(d) => d.encoding().with_data(d, |a| a.to_array_data()),
            Array::View(v) => v.encoding().with_view(v, |a| a.to_array_data()),
        }
    }
}

impl WithArray for Array<'_> {
    fn with_array<R, F: FnMut(&dyn ArrayTrait) -> R>(&self, f: F) -> R {
        match self {
            Array::Data(d) => d.encoding().with_data(d, f),
            Array::DataRef(d) => d.encoding().with_data(d, f),
            Array::View(v) => v.encoding().with_view(v, f),
        }
    }
}

impl Display for Array<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let prefix = match self {
            Array::Data(_) => "",
            Array::DataRef(_) => "&",
            Array::View(_) => "$",
        };
        write!(
            f,
            "{}{}({}, len={})",
            prefix,
            self.encoding().id(),
            self.dtype(),
            self.len()
        )
    }
}
