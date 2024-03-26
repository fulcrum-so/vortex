use vortex_error::{VortexError, VortexResult};

use crate::array::Array;
use crate::scalar::Scalar;

pub enum SearchSortedSide {
    Left,
    Right,
}

pub trait SearchSortedFn {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<usize>;
}

pub fn search_sorted<T: Into<Scalar>>(
    array: &dyn Array,
    target: T,
    side: SearchSortedSide,
) -> VortexResult<usize> {
    let scalar = target.into().cast(array.dtype())?;
    array
        .search_sorted()
        .map(|f| f.search_sorted(&scalar, side))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "search_sorted",
                array.encoding().id().name(),
            ))
        })
}
