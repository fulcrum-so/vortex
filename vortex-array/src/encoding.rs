use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

use linkme::distributed_slice;
use vortex_error::VortexResult;

use crate::compress::EncodingCompression;
use crate::flatten::{ArrayFlatten, Flattened};
use crate::ArrayDef;
use crate::{Array, ArrayTrait};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct EncodingId(&'static str);

impl EncodingId {
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }
}

impl Display for EncodingId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.0, f)
    }
}

impl AsRef<str> for EncodingId {
    fn as_ref(&self) -> &str {
        self.0
    }
}

#[distributed_slice]
pub static VORTEX_ENCODINGS: [EncodingRef] = [..];

pub type EncodingRef = &'static dyn ArrayEncoding;

pub fn find_encoding(id: &str) -> Option<EncodingRef> {
    VORTEX_ENCODINGS
        .iter()
        .find(|&x| x.id().as_ref() == id)
        .cloned()
}

/// Object-safe encoding trait for an array.
pub trait ArrayEncoding: 'static + Sync + Send + Debug {
    fn as_any(&self) -> &dyn Any;

    fn id(&self) -> EncodingId;

    /// Flatten the given array.
    fn flatten<'a>(&self, array: Array<'a>) -> VortexResult<Flattened<'a>>;

    /// Unwrap the provided array into an implementation of ArrayTrait
    fn with_dyn<'a>(
        &self,
        array: &'a Array<'a>,
        f: &mut dyn for<'b> FnMut(&'b (dyn ArrayTrait + 'a)) -> VortexResult<()>,
    ) -> VortexResult<()>;

    /// Return a compressor for this encoding.
    fn compression(&self) -> &dyn EncodingCompression;
}

impl PartialEq for dyn ArrayEncoding + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
impl Eq for dyn ArrayEncoding + '_ {}
impl Hash for dyn ArrayEncoding + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state)
    }
}

/// Non-object-safe extensions to the ArrayEncoding trait.
pub trait ArrayEncodingExt {
    type D: ArrayDef;

    fn flatten<'a>(array: Array<'a>) -> VortexResult<Flattened<'a>>
    where
        <Self as ArrayEncodingExt>::D: 'a,
    {
        let typed = <<Self::D as ArrayDef>::Array<'a> as TryFrom<Array>>::try_from(array)?;
        ArrayFlatten::flatten(typed)
    }

    fn with_dyn<'a, R, F>(array: &'a Array<'a>, mut f: F) -> R
    where
        F: for<'b> FnMut(&'b (dyn ArrayTrait + 'a)) -> R,
        <Self as ArrayEncodingExt>::D: 'a,
    {
        let typed =
            <<Self::D as ArrayDef>::Array<'a> as TryFrom<Array>>::try_from(array.clone()).unwrap();
        f(&typed)
    }
}

pub trait ArrayEncodingRef {
    fn encoding(&self) -> EncodingRef;
}
