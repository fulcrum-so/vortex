use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

use vortex_error::VortexResult;

use crate::canonical::{Canonical, IntoCanonical};
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

pub type EncodingRef = &'static dyn ArrayEncoding;

/// Object-safe encoding trait for an array.
pub trait ArrayEncoding: 'static + Sync + Send + Debug {
    fn id(&self) -> EncodingId;

    /// Flatten the given array.
    fn canonicalize(&self, array: Array) -> VortexResult<Canonical>;

    /// Unwrap the provided array into an implementation of ArrayTrait
    fn with_dyn(
        &self,
        array: &Array,
        f: &mut dyn for<'b> FnMut(&'b (dyn ArrayTrait + 'b)) -> VortexResult<()>,
    ) -> VortexResult<()>;
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

    fn into_canonical(array: Array) -> VortexResult<Canonical> {
        let typed = <<Self::D as ArrayDef>::Array as TryFrom<Array>>::try_from(array)?;
        IntoCanonical::into_canonical(typed)
    }

    #[inline]
    fn with_dyn<R, F>(array: &Array, mut f: F) -> R
    where
        F: for<'b> FnMut(&'b (dyn ArrayTrait + 'b)) -> R,
    {
        let typed =
            <<Self::D as ArrayDef>::Array as TryFrom<Array>>::try_from(array.clone()).unwrap();
        f(&typed)
    }
}

pub trait ArrayEncodingRef {
    fn encoding(&self) -> EncodingRef;
}
