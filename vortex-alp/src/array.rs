use serde::{Deserialize, Serialize};
use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::compute::patch::PatchFn;
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, ArrayFlatten, OwnedArray, ToArrayData, IntoArrayData};
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{IntWidth, Signedness};

use crate::alp::Exponents;
use crate::compress::{alp_encode, decompress};

impl_encoding!("vortex.alp", ALP);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ALPMetadata {
    exponents: Exponents,
    has_patches: bool,
    dtype: DType,
}

impl ALPArray<'_> {
    pub fn try_new(
        encoded: Array,
        exponents: Exponents,
        patches: Option<Array>,
    ) -> VortexResult<Self> {
        let d_type = encoded.dtype().clone();
        let dtype = match encoded.dtype() {
            DType::Int(IntWidth::_32, Signedness::Signed, nullability) => {
                DType::Float(32.into(), *nullability)
            }
            DType::Int(IntWidth::_64, Signedness::Signed, nullability) => {
                DType::Float(64.into(), *nullability)
            }
            d => vortex_bail!(MismatchedTypes: "int32 or int64", d),
        };
        // let d2 = dtype.clone();

        let mut children = vec![];
        children.push(encoded.into_array_data());
        patches.iter().for_each(|patch| {
            children.push(patch.to_array_data());
        });


        Self::try_from_parts(
            dtype,
            ALPMetadata {
                exponents,
                has_patches: patches.is_some(),
                dtype: d_type,
            },
            vec![].into(),
            children.into(),
            Default::default(),
        )
    }

    pub fn encode(array: Array<'_>) -> VortexResult<OwnedArray> {
        if array.encoding().id() == Primitive::ID {
            Ok(alp_encode(&PrimitiveArray::try_from(array)?)?.into_array())
        } else {
            vortex_bail!("ALP can only encode primitive arrays");
        }
    }

    pub fn encoded(&self) -> Array {
        self.array()
            .child(0, &self.metadata().dtype)
            .expect("Missing encoded array")
    }

    pub fn exponents(&self) -> &Exponents {
        &self.metadata().exponents
    }

    pub fn patches(&self) -> Option<Array> {
        self.metadata().has_patches.then(|| {
            self.array()
                .child(1, self.dtype())
                .expect("Missing patches with present metadata flag")
        })
    }
}

impl ArrayValidity for ALPArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.encoded().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.encoded().with_dyn(|a| a.logical_validity())
    }
}

impl ArrayFlatten for ALPArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        decompress(self).map(Flattened::Primitive)
    }
}

impl AcceptArrayVisitor for ALPArray<'_> {
    fn accept(&self, _visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        todo!()
    }
}

impl ArrayStatisticsCompute for ALPArray<'_> {}

impl ArrayTrait for ALPArray<'_> {
    fn len(&self) -> usize {
        todo!()
    }
}

impl PatchFn for ALPArray<'_> {
    fn patch(&self, _patch: &Array) -> VortexResult<Array<'static>> {
        todo!()
    }
}
