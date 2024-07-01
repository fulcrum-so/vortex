//! Vortex crate containing core logic for encoding and memory representation of [arrays](Array).
//!
//! At the heart of Vortex are [arrays](Array) and [encodings](crate::encoding::EncodingCompression).
//! Arrays are typed views of memory buffers that hold [scalars](vortex_scalar::Scalar). These
//! buffers can be held in a number of physical encodings to perform lightweight compression that
//! exploits the particular data distribution of the array's values.
//!
//! Every data type recognized by Vortex also has a canonical physical encoding format, which
//! arrays can be [canonicalized](Canonical) into for ease of access in compute functions.
//!
use std::fmt::{Debug, Display, Formatter};
use std::future::ready;

pub use ::paste;
pub use canonical::*;
pub use context::*;
pub use data::*;
pub use implementation::*;
use itertools::Itertools;
pub use metadata::*;
pub use typed::*;
pub use view::*;
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::compute::ArrayCompute;
use crate::encoding::{ArrayEncodingRef, EncodingRef};
use crate::iter::{ArrayIterator, ArrayIteratorAdapter};
use crate::stats::{ArrayStatistics, ArrayStatisticsCompute};
use crate::stream::{ArrayStream, ArrayStreamAdapter};
use crate::validity::ArrayValidity;
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};

pub mod accessor;
pub mod array;
pub mod arrow;
mod canonical;
pub mod compress;
pub mod compute;
mod context;
mod data;
pub mod encoding;
mod implementation;
pub mod iter;
mod metadata;
pub mod stats;
pub mod stream;
mod tree;
mod typed;
pub mod validity;
pub mod vendored;
mod view;
pub mod visitor;

pub mod flatbuffers {
    pub use generated::vortex::array::*;

    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(clippy::all)]
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/array.rs"));
    }

    mod deps {
        pub mod dtype {
            #[allow(unused_imports)]
            pub use vortex_dtype::flatbuffers as dtype;
        }

        pub mod scalar {
            #[allow(unused_imports)]
            pub use vortex_scalar::flatbuffers as scalar;
        }
    }
}

#[derive(Debug, Clone)]
pub enum Array {
    Data(ArrayData),
    View(ArrayView),
}

impl Array {
    pub fn encoding(&self) -> EncodingRef {
        match self {
            Self::Data(d) => d.encoding(),
            Self::View(v) => v.encoding(),
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

    pub fn child<'a>(&'a self, idx: usize, dtype: &'a DType) -> Option<Self> {
        match self {
            Self::Data(d) => d.child(idx, dtype).cloned(),
            Self::View(v) => v.child(idx, dtype).map(Array::View),
        }
    }

    pub fn children(&self) -> Vec<Array> {
        match self {
            Array::Data(d) => d.children().iter().cloned().collect_vec(),
            Array::View(v) => v.children(),
        }
    }

    pub fn nchildren(&self) -> usize {
        match self {
            Self::Data(d) => d.nchildren(),
            Self::View(v) => v.nchildren(),
        }
    }

    pub fn depth_first_traversal(&self) -> ArrayChildrenIterator {
        ArrayChildrenIterator::new(self.clone())
    }

    /// Return the buffer offsets and the total length of all buffers, assuming the given alignment.
    /// This includes all child buffers.
    pub fn all_buffer_offsets(&self, alignment: usize) -> Vec<u64> {
        let mut offsets = vec![];
        let mut offset = 0;

        for col_data in self.depth_first_traversal() {
            if let Some(buffer) = col_data.buffer() {
                offsets.push(offset as u64);

                let buffer_size = buffer.len();
                let aligned_size = (buffer_size + (alignment - 1)) & !(alignment - 1);
                offset += aligned_size;
            }
        }
        offsets.push(offset as u64);

        offsets
    }

    pub fn buffer(&self) -> Option<&Buffer> {
        match self {
            Self::Data(d) => d.buffer(),
            Self::View(v) => v.buffer(),
        }
    }

    pub fn into_buffer(self) -> Option<Buffer> {
        match self {
            Self::Data(d) => d.into_buffer(),
            Self::View(v) => v.buffer().cloned(),
        }
    }

    pub fn into_array_iterator(self) -> impl ArrayIterator {
        ArrayIteratorAdapter::new(self.dtype().clone(), std::iter::once(Ok(self)))
    }

    pub fn into_array_stream(self) -> impl ArrayStream {
        ArrayStreamAdapter::new(
            self.dtype().clone(),
            futures_util::stream::once(ready(Ok(self))),
        )
    }
}

/// A depth-first pre-order iterator over a ArrayData.
pub struct ArrayChildrenIterator {
    stack: Vec<Array>,
}

impl ArrayChildrenIterator {
    pub fn new(array: Array) -> Self {
        Self { stack: vec![array] }
    }
}

impl Iterator for ArrayChildrenIterator {
    type Item = Array;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.stack.pop()?;
        for child in next.children().into_iter().rev() {
            self.stack.push(child);
        }
        Some(next)
    }
}

pub trait ToArray {
    fn to_array(&self) -> Array;
}

pub trait IntoArray {
    fn into_array(self) -> Array;
}

pub trait ToArrayData {
    fn to_array_data(&self) -> ArrayData;
}

pub trait IntoArrayData {
    fn into_array_data(self) -> ArrayData;
}

pub trait AsArray {
    fn as_array_ref(&self) -> &Array;
}

/// Collects together the behaviour of an array.
pub trait ArrayTrait:
    ArrayEncodingRef
    + ArrayCompute
    + ArrayDType
    + IntoCanonical
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

impl Array {
    #[inline]
    pub fn with_dyn<R, F>(&self, mut f: F) -> R
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

impl Display for Array {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let prefix = match self {
            Self::Data(_) => "",
            Self::View(_) => "$",
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

impl IntoArrayData for Array {
    fn into_array_data(self) -> ArrayData {
        match self {
            Self::Data(d) => d,
            Self::View(_) => self.with_dyn(|a| a.to_array_data()),
        }
    }
}
