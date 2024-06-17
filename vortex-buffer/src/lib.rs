use std::cmp::Ordering;
use std::ops::{Deref, Range};

use arrow_buffer::{ArrowNativeType, Buffer as ArrowBuffer};
pub use string::*;

mod flexbuffers;
pub mod io_buf;
mod string;

#[derive(Debug, Clone)]
pub enum Buffer {
    // TODO(ngates): we could add Aligned(Arc<AVec>) from aligned-vec package
    Arrow(ArrowBuffer),
    Bytes(bytes::Bytes),
}

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Buffer {
    pub fn len(&self) -> usize {
        match self {
            Self::Arrow(b) => b.len(),
            Self::Bytes(b) => b.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Arrow(b) => b.is_empty(),
            Self::Bytes(b) => b.is_empty(),
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        match self {
            Self::Arrow(b) => {
                Self::Arrow(b.slice_with_length(range.start, range.end - range.start))
            }
            Self::Bytes(b) => Self::Bytes(b.slice(range)),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Arrow(b) => b.as_ref(),
            Self::Bytes(b) => b.as_ref(),
        }
    }

    pub fn into_vec<T: ArrowNativeType>(self) -> Result<Vec<T>, Self> {
        match self {
            Self::Arrow(buffer) => buffer.into_vec::<T>().map_err(Buffer::Arrow),
            // Cannot convert bytes into a mutable vec
            Self::Bytes(_) => Err(self),
        }
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<&[u8]> for Buffer {
    fn from(value: &[u8]) -> Self {
        // We prefer Arrow since it retains mutability
        Self::Arrow(ArrowBuffer::from(value))
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(value: Vec<u8>) -> Self {
        // We prefer Arrow since it retains mutability
        Self::Arrow(ArrowBuffer::from_vec(value))
    }
}

impl From<bytes::Bytes> for Buffer {
    fn from(value: bytes::Bytes) -> Self {
        Self::Bytes(value)
    }
}

impl From<ArrowBuffer> for Buffer {
    fn from(value: ArrowBuffer) -> Self {
        Self::Arrow(value)
    }
}

impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl Eq for Buffer {}

impl PartialOrd for Buffer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}
