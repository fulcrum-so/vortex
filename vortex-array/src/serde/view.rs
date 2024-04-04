use std::fmt::{Debug, Formatter};

use arrow_buffer::Buffer;
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_schema::DType;

use crate::array::Array;
use crate::encoding::EncodingRef;
use crate::flatbuffers::array as fb;
use crate::serde::context::SerdeContext;
use crate::serde::{EncodingSerde, WithArray};

#[derive(Clone)]
pub struct ArrayView<'a> {
    encoding: EncodingRef,
    dtype: &'a DType,
    array: fb::Array<'a>,
    buffers: &'a [Buffer],
    ctx: &'a SerdeContext,
}

impl<'a> Debug for ArrayView<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayView")
            .field("encoding", &self.encoding)
            .field("dtype", &self.dtype)
            // .field("array", &self.array)
            .field("buffers", &self.buffers)
            .field("ctx", &self.ctx)
            .finish()
    }
}

impl<'a> ArrayView<'a> {
    pub fn try_new(
        ctx: &'a SerdeContext,
        dtype: &'a DType,
        array: fb::Array<'a>,
        buffers: &'a [Buffer],
    ) -> VortexResult<Self> {
        let encoding = ctx
            .find_encoding(array.encoding())
            .ok_or_else(|| vortex_err!(InvalidSerde: "Encoding ID out of bounds"))?;
        let _vtable = encoding.serde().ok_or_else(|| {
            // TODO(ngates): we could fall-back to heap-allocating?
            vortex_err!(InvalidSerde: "Encoding {} does not support serde", encoding)
        })?;

        if buffers.len() != Self::cumulative_nbuffers(array) {
            vortex_bail!(InvalidSerde:
                "Incorrect number of buffers {}, expected {}",
                buffers.len(),
                Self::cumulative_nbuffers(array)
            )
        }

        Ok(Self {
            encoding,
            dtype,
            array,
            buffers,
            ctx,
        })
    }

    pub fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    pub fn vtable(&self) -> &dyn EncodingSerde {
        self.encoding.serde().unwrap()
    }

    pub fn dtype(&self) -> &DType {
        self.dtype
    }

    pub fn metadata(&self) -> Option<&'a [u8]> {
        self.array.metadata().map(|m| m.bytes())
    }

    pub fn nchildren(&self) -> usize {
        self.array.children().map(|c| c.len()).unwrap_or_default()
    }

    pub fn child(&self, idx: usize, dtype: &'a vortex_schema::DType) -> Option<ArrayView<'a>> {
        let child = self.array_child(idx)?;

        // Figure out how many buffers to skip...
        // We store them depth-first.
        let buffer_offset = self
            .array
            .children()?
            .iter()
            .take(idx)
            .map(|child| Self::cumulative_nbuffers(child))
            .sum();
        let buffer_count = Self::cumulative_nbuffers(child);

        Some(
            Self::try_new(
                self.ctx,
                dtype,
                child,
                &self.buffers[buffer_offset..][0..buffer_count],
            )
            .unwrap(),
        )
    }

    fn array_child(&self, idx: usize) -> Option<fb::Array<'a>> {
        let children = self.array.children()?;
        if idx < children.len() {
            Some(children.get(idx))
        } else {
            None
        }
    }

    /// The number of buffers used by the current Array.
    pub fn nbuffers(&self) -> usize {
        self.array.nbuffers() as usize
    }

    /// The number of buffers used by the current Array and all its children.
    fn cumulative_nbuffers(array: fb::Array) -> usize {
        let mut nbuffers = array.nbuffers() as usize;
        for child in array.children().unwrap_or_default() {
            nbuffers += Self::cumulative_nbuffers(child);
        }
        nbuffers
    }

    pub fn buffers(&self) -> &'a [Buffer] {
        // This is only true for the immediate current node?
        &self.buffers[0..self.nbuffers()]
    }

    pub fn with_array<R, F: Fn(&dyn Array) -> VortexResult<R>>(&self, f: F) -> VortexResult<R> {
        self.encoding()
            .serde()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Encoding does not support serde"))?
            .with_array(self, f)
    }
}
