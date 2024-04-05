mod compute;

use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::DType;

use crate::array2::ArrayView;
use crate::array2::TypedArrayView;
use crate::array2::{Array, ArrayEncoding, ArrayMetadata, FromArrayMetadata};
use crate::array2::{ArrayData, TypedArrayData};
use crate::impl_encoding;

impl_encoding!("vortex.ree", REE);

#[derive(Clone, Debug)]
pub struct REEMetadata {
    length: usize,
    ends_dtype: DType,
}

impl REEMetadata {
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn ends_dtype(&self) -> &DType {
        &self.ends_dtype
    }
}

pub trait REEArray {
    fn run_ends(&self) -> Array;
    fn values(&self) -> Array;
}

impl REEData {
    pub fn new(ends: ArrayData, values: ArrayData, length: usize) -> Self {
        ArrayData::try_new(
            &REEEncoding,
            values.dtype().clone(),
            REEMetadata {
                length,
                ends_dtype: ends.dtype().clone(),
            }
            .into_arc(),
            vec![].into(),
            vec![ends, values].into(),
        )
        .unwrap()
        .as_typed()
    }
}

impl REEArray for REEData {
    fn run_ends(&self) -> Array {
        Array::DataRef(self.data().children().first().unwrap())
    }

    fn values(&self) -> Array {
        Array::DataRef(self.data().children().get(1).unwrap())
    }
}

impl REEArray for REEView<'_> {
    fn run_ends(&self) -> Array {
        Array::View(self.view().child(0, self.metadata().ends_dtype()).unwrap())
    }

    fn values(&self) -> Array {
        Array::View(self.view().child(1, self.view().dtype()).unwrap())
    }
}

impl FromArrayMetadata for REEMetadata {
    fn try_from(metadata: Option<&[u8]>) -> VortexResult<Self> {
        let Some(bytes) = metadata else {
            vortex_bail!("REE metadata is missing")
        };
        todo!()
    }
}

impl FromArrayView for REEView<'_> {
    fn try_from(view: &ArrayView) -> VortexResult<Self> {
        todo!()
    }
}

impl FromArrayData for REEData {
    fn try_from(data: &ArrayData) -> VortexResult<Self> {
        todo!()
    }
}

impl ArrayTrait for &dyn REEArray {
    fn dtype(&self) -> &DType {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }
}
