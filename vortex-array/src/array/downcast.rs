use crate::array::bool::BoolArray;
use crate::array::chunked::ChunkedArray;
use crate::array::composite::CompositeArray;
use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::sparse::SparseArray;
use crate::array::struct_::StructArray;
use crate::array::varbin::VarBinArray;
use crate::array::varbinview::VarBinViewArray;
use crate::array::{Array, ArrayRef};

mod private {
    pub trait Sealed {}
}

pub trait DowncastArrayBuiltin: private::Sealed {
    fn maybe_primitive(&self) -> Option<&PrimitiveArray>;

    fn as_primitive(&self) -> &PrimitiveArray {
        self.maybe_primitive().unwrap()
    }

    fn maybe_bool(&self) -> Option<&BoolArray>;

    fn as_bool(&self) -> &BoolArray {
        self.maybe_bool().unwrap()
    }

    fn maybe_varbin(&self) -> Option<&VarBinArray>;

    fn as_varbin(&self) -> &VarBinArray {
        self.maybe_varbin().unwrap()
    }

    fn maybe_varbinview(&self) -> Option<&VarBinViewArray>;

    fn as_varbinview(&self) -> &VarBinViewArray {
        self.maybe_varbinview().unwrap()
    }

    fn maybe_composite(&self) -> Option<&CompositeArray>;

    fn as_composite(&self) -> &CompositeArray {
        self.maybe_composite().unwrap()
    }

    fn maybe_struct(&self) -> Option<&StructArray>;

    fn as_struct(&self) -> &StructArray {
        self.maybe_struct().unwrap()
    }

    fn maybe_sparse(&self) -> Option<&SparseArray>;

    fn as_sparse(&self) -> &SparseArray {
        self.maybe_sparse().unwrap()
    }

    fn maybe_constant(&self) -> Option<&ConstantArray>;

    fn as_constant(&self) -> &ConstantArray {
        self.maybe_constant().unwrap()
    }

    fn maybe_chunked(&self) -> Option<&ChunkedArray>;

    fn as_chunked(&self) -> &ChunkedArray {
        self.maybe_chunked().unwrap()
    }
}

impl private::Sealed for dyn Array + '_ {}

impl DowncastArrayBuiltin for dyn Array + '_ {
    fn maybe_primitive(&self) -> Option<&PrimitiveArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_bool(&self) -> Option<&BoolArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_varbin(&self) -> Option<&VarBinArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_varbinview(&self) -> Option<&VarBinViewArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_composite(&self) -> Option<&CompositeArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_struct(&self) -> Option<&StructArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_sparse(&self) -> Option<&SparseArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_constant(&self) -> Option<&ConstantArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_chunked(&self) -> Option<&ChunkedArray> {
        self.as_any().downcast_ref()
    }
}

impl private::Sealed for ArrayRef {}

impl DowncastArrayBuiltin for ArrayRef {
    fn maybe_primitive(&self) -> Option<&PrimitiveArray> {
        self.as_ref().maybe_primitive()
    }

    fn maybe_bool(&self) -> Option<&BoolArray> {
        self.as_ref().maybe_bool()
    }

    fn maybe_varbin(&self) -> Option<&VarBinArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_varbinview(&self) -> Option<&VarBinViewArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_composite(&self) -> Option<&CompositeArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_struct(&self) -> Option<&StructArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_sparse(&self) -> Option<&SparseArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_constant(&self) -> Option<&ConstantArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_chunked(&self) -> Option<&ChunkedArray> {
        self.as_any().downcast_ref()
    }
}
