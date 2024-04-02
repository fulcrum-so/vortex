use crate::array::primitive::{PrimitiveArray, PrimitiveEncoding};
use crate::array::{Array, ArrayRef};
use crate::compute::flatten::flatten_primitive;
use crate::compute::{ComputeVTable, VTakeFn};
use crate::match_each_integer_ptype;
use crate::ptype::{NativePType, PType};
use crate::validity::Validity;
use num_traits::PrimInt;
use vortex_error::VortexResult;

// A trait that can be used to implement compute over both ArrayView and PrimitiveArray.
// TODO(ngates): decide whether to put T: NativePType in this trait.
#[allow(dead_code)]
pub trait PrimitiveTrait<T: NativePType> {
    fn t_ptype(&self) -> PType;
    fn t_typed_data(&self) -> &[T];
    fn t_validity(&self) -> Option<Validity>;
}

impl<T: NativePType> PrimitiveTrait<T> for PrimitiveArray {
    fn t_ptype(&self) -> PType {
        todo!()
    }

    fn t_typed_data(&self) -> &[T] {
        todo!()
    }

    fn t_validity(&self) -> Option<Validity> {
        todo!()
    }
}

impl<T: NativePType> ComputeVTable<&dyn PrimitiveTrait<T>> for PrimitiveEncoding {}

impl<T: NativePType> VTakeFn<&dyn PrimitiveTrait<T>> for PrimitiveEncoding {
    fn take(&self, array: &dyn PrimitiveTrait<T>, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let validity = array.t_validity().map(|v| v.take(indices)).transpose()?;
        let indices = flatten_primitive(indices)?;
        match_each_integer_ptype!(indices.ptype(), |$I| {
            Ok(PrimitiveArray::from_nullable(
                take_primitive(array.t_typed_data(), indices.typed_data::<$I>()),
                validity,
            ).into_array())
        })
    }
}

fn take_primitive<T: NativePType, I: NativePType + PrimInt>(array: &[T], indices: &[I]) -> Vec<T> {
    indices
        .iter()
        .map(|&idx| array[idx.to_usize().unwrap()])
        .collect()
}

// Need some way to downcast compute...
impl<'a> ComputeVTable<&'a dyn Array> for PrimitiveEncoding {
    fn take(&self) -> Option<&dyn VTakeFn<&'a dyn Array>> {
        Some(self)
    }
}

impl<'a> VTakeFn<&'a dyn Array> for PrimitiveEncoding {
    fn take(&self, array: &'a dyn Array, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let primitive_array = array.as_any().downcast_ref::<PrimitiveArray>().unwrap();
        //match_each_native_ptype!(primitive_array.ptype(), |$T| {
        let primitive_array_trait = primitive_array.as_trait::<u16>();
        VTakeFn::take(self, primitive_array_trait, indices)
        //})
    }
}
