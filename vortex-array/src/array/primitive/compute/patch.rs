use itertools::Itertools;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::primitive::PrimitiveArray;
use crate::array::sparse::{SparseArray, SparseEncoding};
use crate::array::{Array, ArrayRef};
use crate::compute;
use crate::compute::patch::PatchFn;
use crate::ptype::NativePType;
use crate::view::ToOwnedView;

impl<T: NativePType> PatchFn for &dyn PrimitiveTrait<T> {
    fn patch(&self, patch: &dyn Array) -> VortexResult<ArrayRef> {
        match patch.encoding().id() {
            SparseEncoding::ID => patch_with_sparse(*self, patch.as_sparse()),
            // TODO(ngates): support a default implementation based on iter_arrow?
            _ => Err(vortex_err!(NotImplemented: "patch", patch.encoding().id().name())),
        }
    }
}

fn patch_with_sparse<T: NativePType>(
    array: &dyn PrimitiveTrait<T>,
    patch: &SparseArray,
) -> VortexResult<ArrayRef> {
    let patch_indices = patch.resolved_indices();
    let mut values = Vec::from(array.typed_data());
    let patch_values = compute::flatten::flatten_primitive(patch.values())?;

    if array.ptype() != patch_values.ptype() {
        vortex_bail!(MismatchedTypes: array.dtype(), Array::dtype(&patch_values))
    }

    for (idx, value) in patch_indices
        .iter()
        .zip_eq(patch_values.typed_data().iter())
    {
        values[*idx] = *value;
    }

    Ok(PrimitiveArray::from_nullable(
        values,
        // TODO(ngates): if patch values has null, we need to patch into the validity buffer
        array.validity().map(|v| v.to_owned_view()),
    )
    .to_array_data())
}
