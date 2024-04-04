use arrow_buffer::BooleanBuffer;
use num_traits::AsPrimitive;
use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveTrait;
use crate::array::primitive::TypedPrimitiveTrait;
use crate::array::{Array, ArrayRef};
use crate::compute::flatten::flatten_primitive;
use crate::compute::take::TakeFn;
use crate::match_each_integer_ptype;

impl TakeFn for BoolArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let validity = self
            .validity()
            .map(|v| v.as_view().take(indices))
            .transpose()?;
        let indices = flatten_primitive(indices)?;
        match_each_integer_ptype!(indices.ptype(), |$I| {
            Ok(BoolArray::from_nullable(
                take_bool(self.buffer(), indices.typed_data::<$I>()),
                validity,
            ).into_array())
        })
    }
}

fn take_bool<I: AsPrimitive<usize>>(bools: &BooleanBuffer, indices: &[I]) -> Vec<bool> {
    indices.iter().map(|&idx| bools.value(idx.as_())).collect()
}

#[cfg(test)]
mod test {
    use crate::array::bool::BoolArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::compute::take::take;

    #[test]
    fn take_nullable() {
        let reference = BoolArray::from_iter(vec![
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(false),
        ]);
        let res = take(&reference, &PrimitiveArray::from(vec![0, 3, 4])).unwrap();
        assert_eq!(
            res.as_bool().buffer(),
            BoolArray::from_iter(vec![Some(false), None, Some(false)]).buffer()
        );
    }
}
