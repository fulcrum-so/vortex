use itertools::Itertools;

use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::match_each_native_ptype;
use crate::scalar::Scalar;

impl ArrayCompute for ConstantArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

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

impl AsContiguousFn for ConstantArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        let chunks = arrays.iter().map(|a| a.as_constant().clone()).collect_vec();
        if chunks.iter().map(|c| c.scalar()).all_equal() {
            Ok(ConstantArray::new(
                chunks.first().unwrap().scalar().clone(),
                chunks.iter().map(|c| c.len()).sum(),
            )
            .into_array())
        } else {
            // TODO(ngates): we need to flatten the constant arrays and then concatenate them
            Err("Cannot concatenate constant arrays with differing scalars".into())
        }
    }
}

impl FlattenFn for ConstantArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(match self.scalar() {
            Scalar::Bool(b) => {
                if let Some(bv) = b.value() {
                    FlattenedArray::Bool(BoolArray::from(vec![bv; self.len()]))
                } else {
                    FlattenedArray::Bool(BoolArray::null(self.len()))
                }
            }
            Scalar::Primitive(p) => {
                if let Some(ps) = p.value() {
                    match_each_native_ptype!(ps.ptype(), |$P| {
                        FlattenedArray::Primitive(PrimitiveArray::from_value::<$P>(
                            $P::try_from(self.scalar())?,
                            self.len(),
                        ))
                    })
                } else {
                    match_each_native_ptype!(p.ptype(), |$P| {
                        FlattenedArray::Primitive(PrimitiveArray::null::<$P>(self.len()))
                    })
                }
            }
            _ => panic!("Unsupported scalar type {}", self.dtype()),
        })
    }
}

impl ScalarAtFn for ConstantArray {
    fn scalar_at(&self, _index: usize) -> VortexResult<Scalar> {
        Ok(self.scalar().clone())
    }
}

impl TakeFn for ConstantArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        Ok(ConstantArray::new(self.scalar().clone(), indices.len()).into_array())
    }
}
