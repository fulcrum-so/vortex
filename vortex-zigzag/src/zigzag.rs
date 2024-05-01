use serde::{Deserialize, Serialize};
use vortex::array::primitive::PrimitiveArray;
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, ArrayFlatten, IntoArrayData};
use vortex_dtype::PType;
use vortex_error::{vortex_bail, vortex_err};

use crate::compress::zigzag_encode;

impl_encoding!("vortex.zigzag", ZigZag);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZigZagMetadata {
    encoded_dtype: DType,
}

impl ZigZagArray<'_> {
    pub fn new(encoded: Array) -> Self {
        Self::try_new(encoded).unwrap()
    }

    pub fn try_new(encoded: Array) -> VortexResult<Self> {
        let encoded_dtype = encoded.dtype().clone();
        let dtype = if encoded_dtype.is_unsigned_int() {
            DType::from(PType::try_from(&encoded_dtype).unwrap().to_signed())
                .with_nullability(encoded_dtype.nullability())
        } else {
            vortex_bail!(MismatchedTypes: "unsigned int", encoded_dtype)
        };

        let children = vec![encoded.into_array_data()];

        let metadata = ZigZagMetadata { encoded_dtype };
        Self::try_from_parts(dtype, metadata, children.into(), StatsSet::new())
    }

    pub fn encode<'a>(array: &'a Array<'a>) -> VortexResult<Array<'a>> {
        PrimitiveArray::try_from(array)
            .map_err(|_| vortex_err!("ZigZag can only encoding primitive arrays"))
            .map(|parray| zigzag_encode(&parray))?
            .map(|encoded| encoded.into_array())
    }

    pub fn encoded(&self) -> Array {
        self.array()
            .child(0, &self.metadata().encoded_dtype)
            .expect("Missing encoded array")
    }
}

impl ArrayValidity for ZigZagArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.encoded().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.encoded().with_dyn(|a| a.logical_validity())
    }
}

impl AcceptArrayVisitor for ZigZagArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("encoded", &self.encoded())
    }
}

impl ArrayStatisticsCompute for ZigZagArray<'_> {}

impl ArrayFlatten for ZigZagArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        todo!("ZigZagArray::flatten")
    }
}

impl ArrayTrait for ZigZagArray<'_> {
    fn len(&self) -> usize {
        self.encoded().len()
    }
}
