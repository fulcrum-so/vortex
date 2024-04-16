use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::DictArray;

impl ArrayCompute for DictArray<'_> {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for DictArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let dict_index: usize = scalar_at(self.codes(), index)?.try_into()?;
        scalar_at(self.values(), dict_index)
    }
}

impl TakeFn for DictArray<'_> {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        // Dict
        //   codes: 0 0 1
        //   dict: a b c d e f g h
        let codes = take(self.codes(), indices)?;
        Ok(DictArray::new(codes, self.values().clone()).into_array())
    }
}

impl SliceFn for DictArray<'_> {
    // TODO(robert): Add function to trim the dictionary
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(DictArray::new(slice(self.codes(), start, stop)?, self.values().clone()).into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::PrimitiveArray;
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
        let dict = DictArray::new(codes.into_array(), values.into_array());
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
        let dict = DictArray::new(codes.into_array(), values.into_array());
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
