use vortex::array::{Array, ArrayRef};
use vortex::compute::flatten::{flatten, FlattenFn, FlattenedArray};
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::DictArray;

impl ArrayCompute for DictArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl FlattenFn for DictArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        flatten(&take(self.values(), self.codes())?)
    }
}

impl ScalarAtFn for DictArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let dict_index: usize = scalar_at(self.codes(), index)?.try_into()?;
        scalar_at(self.values(), dict_index)
    }
}

impl TakeFn for DictArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let codes = take(self.codes(), indices)?;
        // TODO(ngates): Add function to remove unused entries from dictionary
        Ok(DictArray::new(codes, self.values().clone()).to_array())
    }
}

#[cfg(test)]
mod test {
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::{PrimitiveArray, PrimitiveTrait};
    use vortex::array::varbin::VarBinArray;
    use vortex::array::Array;
    use vortex::compute::flatten::{flatten_primitive, flatten_varbin};
    use vortex_schema::{DType, Nullability};

    use crate::{dict_encode_typed_primitive, dict_encode_varbin, DictArray};

    #[test]
    fn flatten_nullable_primitive() {
        let reference =
            PrimitiveArray::from_iter(vec![Some(42), Some(-9), None, Some(42), None, Some(-9)]);
        let (codes, values) = dict_encode_typed_primitive::<i32>(&reference);
        let dict = DictArray::new(codes.to_array(), values.to_array());
        let flattened_dict = flatten_primitive(&dict).unwrap();
        assert_eq!(flattened_dict.buffer(), reference.buffer());
    }

    #[test]
    fn flatten_nullable_varbin() {
        let reference = VarBinArray::from_iter(
            vec![Some("a"), Some("b"), None, Some("a"), None, Some("b")],
            DType::Utf8(Nullability::Nullable),
        );
        let (codes, values) = dict_encode_varbin(&reference);
        let dict = DictArray::new(codes.to_array(), values.to_array());
        let flattened_dict = flatten_varbin(&dict).unwrap();
        assert_eq!(
            flattened_dict.offsets().as_primitive().buffer(),
            reference.offsets().as_primitive().buffer()
        );
        assert_eq!(
            flattened_dict.bytes().as_primitive().buffer(),
            reference.bytes().as_primitive().buffer()
        );
    }
}
