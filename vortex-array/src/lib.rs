pub mod accessor;
pub mod array;
pub mod arrow;
pub mod buffer;
pub mod compress;
pub mod compute;
mod context;
mod data;
pub mod encoding;
mod flatten;
mod implementation;
mod metadata;
pub mod ptype;
mod sampling;
pub mod scalar;
pub mod stats;
mod tree;
mod typed;
pub mod validity;
mod view;
pub mod visitor;

use std::fmt::{Debug, Display, Formatter};

pub use context::*;
pub use data::*;
pub use flatten::*;
pub use implementation::*;
pub use linkme;
pub use metadata::*;
pub use typed::*;
pub use view::*;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::buffer::Buffer;
use crate::compute::ArrayCompute;
use crate::encoding::{ArrayEncodingRef, EncodingRef};
use crate::stats::{ArrayStatistics, ArrayStatisticsCompute};
use crate::validity::ArrayValidity;
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};

pub mod flatbuffers {
    pub use gen_array::vortex::*;
    pub use gen_scalar::vortex::*;

    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(clippy::all)]
    mod gen_array {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/array.rs"));
    }

    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(clippy::all)]
    mod gen_scalar {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/scalar.rs"));
    }

    mod deps {
        pub mod dtype {
            #[allow(unused_imports)]
            pub use vortex_schema::flatbuffers as dtype;
        }
    }
}

#[derive(Debug, Clone)]
pub enum Array<'v> {
    Data(ArrayData),
    DataRef(&'v ArrayData),
    View(ArrayView<'v>),
}

pub type OwnedArray = Array<'static>;

impl Array<'_> {
    pub fn encoding(&self) -> EncodingRef {
        match self {
            Array::Data(d) => d.encoding(),
            Array::DataRef(d) => d.encoding(),
            Array::View(v) => v.encoding(),
        }
    }

    pub fn len(&self) -> usize {
        self.with_dyn(|a| a.len())
    }

    pub fn nbytes(&self) -> usize {
        self.with_dyn(|a| a.nbytes())
    }

    pub fn is_empty(&self) -> bool {
        self.with_dyn(|a| a.is_empty())
    }

    pub fn child<'a>(&'a self, idx: usize, dtype: &'a DType) -> Option<Array<'a>> {
        match self {
            Array::Data(d) => d.child(idx, dtype).map(Array::DataRef),
            Array::DataRef(d) => d.child(idx, dtype).map(Array::DataRef),
            Array::View(v) => v.child(idx, dtype).map(Array::View),
        }
    }

    pub fn buffer(&self, idx: usize) -> Option<&Buffer> {
        match self {
            Array::Data(d) => d.buffers().get(idx),
            Array::DataRef(d) => d.buffers().get(idx),
            Array::View(v) => v.buffers().get(idx),
        }
    }
}

impl<'a> Array<'a> {
    pub fn into_buffer(self, idx: usize) -> Option<Buffer<'a>> {
        match self {
            Array::Data(d) => {
                let mut buffers = d.into_buffers();
                if idx >= buffers.len() {
                    None
                } else {
                    Some(buffers.remove(idx))
                }
            }
            Array::DataRef(d) => d.buffers().get(idx).cloned(),
            Array::View(v) => v.buffers().get(idx).map(|b| b.to_static()),
        }
    }
}

impl ToStatic for Array<'_> {
    type Static = OwnedArray;

    fn to_static(&self) -> Self::Static {
        Array::Data(self.to_array_data())
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

pub trait IntoArrayData {
    fn into_array_data(self) -> ArrayData;
}

pub trait ToStatic {
    type Static;

    fn to_static(&self) -> Self::Static;
}

pub trait AsArray {
    fn as_array_ref(&self) -> &Array;
}

/// Collects together the behaviour of an array.
pub trait ArrayTrait:
    ArrayEncodingRef
    + ArrayCompute
    + ArrayDType
    + ArrayFlatten
    + ArrayValidity
    + AcceptArrayVisitor
    + ArrayStatistics
    + ArrayStatisticsCompute
    + ToArrayData
{
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

pub trait ArrayDType {
    // TODO(ngates): move into ArrayTrait?
    fn dtype(&self) -> &DType;
}

struct NBytesVisitor(usize);
impl ArrayVisitor for NBytesVisitor {
    fn visit_child(&mut self, _name: &str, array: &Array) -> VortexResult<()> {
        self.0 += array.with_dyn(|a| a.nbytes());
        Ok(())
    }

    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()> {
        self.0 += buffer.len();
        Ok(())
    }
}

impl<'a> Array<'a> {
    pub fn with_dyn<R, F>(&'a self, mut f: F) -> R
    where
        F: FnMut(&dyn ArrayTrait) -> R,
    {
        let mut result = None;

        self.encoding()
            .with_dyn(self, &mut |array| {
                result = Some(f(array));
                Ok(())
            })
            .unwrap();

        // Now we unwrap the optional, which we know to be populated by the closure.
        result.unwrap()
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

impl IntoArrayData for Array<'_> {
    fn into_array_data(self) -> ArrayData {
        match self {
            Array::Data(d) => d,
            Array::DataRef(d) => d.clone(),
            Array::View(_) => self.with_dyn(|a| a.to_array_data()),
        }
    }
}
