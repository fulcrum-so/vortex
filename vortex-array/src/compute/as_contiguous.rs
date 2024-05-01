use itertools::Itertools;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::{Array, ArrayDType, OwnedArray};

pub trait AsContiguousFn {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<OwnedArray>;
}

pub fn as_contiguous(arrays: &[Array]) -> VortexResult<OwnedArray> {
    if arrays.is_empty() {
        vortex_bail!(ComputeError: "No arrays to concatenate");
    }
    if !arrays.iter().map(|chunk| chunk.encoding().id()).all_equal() {
        vortex_bail!(ComputeError:
            "Chunks have differing encodings",
        );
    }
    if !arrays.iter().map(|chunk| chunk.dtype()).all_equal() {
        vortex_bail!(ComputeError:
            "Chunks have differing dtypes",
        );
    }

    let first = arrays.first().unwrap();
    first.with_dyn(|a| {
        a.as_contiguous()
            .map(|f| f.as_contiguous(arrays))
            .unwrap_or_else(|| {
                Err(vortex_err!(
                    NotImplemented: "as_contiguous",
                    first.encoding().id()
                ))
            })
    })
}
