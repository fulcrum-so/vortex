use vortex::array::{Array, ArrayRef};

use crate::DictArray;

mod private {
    pub trait Sealed {}
}

pub trait DowncastDict: private::Sealed {
    fn maybe_dict(&self) -> Option<&DictArray>;

    fn as_dict(&self) -> &DictArray {
        self.maybe_dict().unwrap()
    }
}

impl private::Sealed for dyn Array + '_ {}

impl DowncastDict for dyn Array + '_ {
    fn maybe_dict(&self) -> Option<&DictArray> {
        self.as_any().downcast_ref()
    }
}

impl private::Sealed for ArrayRef {}

impl DowncastDict for ArrayRef {
    fn maybe_dict(&self) -> Option<&DictArray> {
        self.as_any().downcast_ref()
    }
}
