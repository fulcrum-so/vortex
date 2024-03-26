use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, BinaryViewArray, StringViewArray};
use arrow_buffer::ScalarBuffer;
use itertools::Itertools;

use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

use crate::array::varbinview::VarBinViewArray;
use crate::array::Array;
use crate::arrow::wrappers::as_nulls;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::flatten::{flatten, flatten_primitive, FlattenFn, FlattenedArray};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::ptype::PType;
use crate::scalar::Scalar;
use crate::validity::ArrayValidity;

impl ArrayCompute for VarBinViewArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for VarBinViewArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            self.bytes_at(index).map(|bytes| {
                if matches!(self.dtype, DType::Utf8(_)) {
                    unsafe { String::from_utf8_unchecked(bytes) }.into()
                } else {
                    bytes.into()
                }
            })
        } else {
            Ok(Scalar::null(self.dtype()))
        }
    }
}

impl FlattenFn for VarBinViewArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        let views = flatten(self.views())?.into_array();
        let data = self
            .data()
            .iter()
            .map(|d| flatten(d.as_ref()).unwrap().into_array())
            .collect::<Vec<_>>();
        Ok(FlattenedArray::VarBinView(VarBinViewArray::new(
            views,
            data,
            self.dtype.clone(),
            self.validity(),
        )))
    }
}

impl AsArrowArray for VarBinViewArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // Views should be buffer of u8
        let views = flatten_primitive(self.views())?;
        assert_eq!(views.ptype(), PType::U8);
        let nulls = as_nulls(self.validity())?;

        let data = self
            .data()
            .iter()
            .map(|d| flatten_primitive(d.as_ref()).unwrap())
            .collect::<Vec<_>>();
        if !data.is_empty() {
            assert_eq!(data[0].ptype(), PType::U8);
            assert!(data.iter().map(|d| d.ptype()).all_equal());
        }

        let data = data
            .iter()
            .map(|p| p.buffer().to_owned())
            .collect::<Vec<_>>();

        // Switch on Arrow DType.
        Ok(match self.dtype() {
            DType::Binary(_) => Arc::new(BinaryViewArray::new(
                ScalarBuffer::<u128>::from(views.buffer().clone()),
                data,
                nulls,
            )),
            DType::Utf8(_) => Arc::new(StringViewArray::new(
                ScalarBuffer::<u128>::from(views.buffer().clone()),
                data,
                nulls,
            )),
            _ => return Err(VortexError::InvalidDType(self.dtype().clone())),
        })
    }
}
