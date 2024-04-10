use std::any::Any;
use std::fmt::{Debug, Formatter};

use linkme::distributed_slice;
pub use vortex::encoding::EncodingId;
use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::ArrayView;
use crate::{ArrayData, ArrayTrait};

#[distributed_slice]
pub static VORTEX_ENCODINGS: [EncodingRef] = [..];

pub type EncodingRef = &'static dyn ArrayEncoding;

pub fn find_encoding(id: &str) -> Option<EncodingRef> {
    VORTEX_ENCODINGS
        .iter()
        .find(|&x| x.id().name() == id)
        .cloned()
}

/// Dynamic trait representing an array type.
#[allow(dead_code)]
pub trait ArrayEncoding: 'static + Sync + Send {
    fn as_any(&self) -> &dyn Any;

    fn id(&self) -> EncodingId;

    fn with_view_mut<'v>(
        &self,
        view: &'v ArrayView<'v>,
        f: &mut dyn FnMut(&dyn ArrayTrait) -> VortexResult<()>,
    ) -> VortexResult<()>;

    fn with_data_mut(
        &self,
        data: &ArrayData,
        f: &mut dyn FnMut(&dyn ArrayTrait) -> VortexResult<()>,
    ) -> VortexResult<()>;
}

pub trait WithEncodedArray<'v, A: ArrayTrait> {
    fn with_view_mut(
        &'v self,
        view: &'v ArrayView<'v>,
        f: &mut dyn FnMut(&A) -> VortexResult<()>,
    ) -> VortexResult<()>;

    fn with_data_mut(
        &self,
        data: &'v ArrayData,
        f: &mut dyn FnMut(&A) -> VortexResult<()>,
    ) -> VortexResult<()>;
}

impl Debug for dyn ArrayEncoding + '_ {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.id(), f)
    }
}

impl dyn ArrayEncoding {
    pub(crate) fn with_view<'v, R, F: FnMut(&dyn ArrayTrait) -> R>(
        &self,
        view: &'v ArrayView<'v>,
        mut f: F,
    ) -> R {
        let mut result = None;

        // Unwrap the result. This is safe since we validate that encoding against the
        // ArrayData during ArrayData::try_new.
        self.with_view_mut(view, &mut |array| {
            result = Some(f(array));
            Ok(())
        })
        .unwrap();

        // Now we unwrap the optional, which we know to be populated in the closure.
        result.unwrap()
    }

    pub(crate) fn with_data<R, F: FnMut(&dyn ArrayTrait) -> R>(
        &self,
        data: &ArrayData,
        mut f: F,
    ) -> R {
        let mut result = None;

        // Unwrap the result. This is safe since we validate that encoding against the
        // ArrayData during ArrayData::try_new.
        self.with_data_mut(data, &mut |array| {
            result = Some(f(array));
            Ok(())
        })
        .unwrap();

        // Now we unwrap the optional, which we know to be populated in the closure.
        result.unwrap()
    }
}
