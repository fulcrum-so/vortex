use serde::{Deserialize, Serialize};
use vortex::array::primitive::PrimitiveArray;
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, ArrayFlatten, IntoArrayData, OwnedArray, ToArrayData};
use vortex_dtype::PType;
use vortex_error::vortex_bail;

use crate::alp::Exponents;
use crate::compress::{alp_encode, decompress};

impl_encoding!("vortex.alp", ALP);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ALPMetadata {
    exponents: Exponents,
    encoded_dtype: DType,
    patches_dtype: Option<DType>,
}

impl ALPArray<'_> {
    pub fn try_new(
        encoded: Array,
        exponents: Exponents,
        patches: Option<Array>,
    ) -> VortexResult<Self> {
        let encoded_dtype = encoded.dtype().clone();
        let dtype = match encoded.dtype() {
            DType::Primitive(PType::I32, nullability) => DType::Primitive(PType::F32, *nullability),
            DType::Primitive(PType::I64, nullability) => DType::Primitive(PType::F64, *nullability),
            d => vortex_bail!(MismatchedTypes: "int32 or int64", d),
        };

        let mut children = Vec::with_capacity(2);
        children.push(encoded.into_array_data());
        if let Some(ref patch) = patches {
            children.push(patch.to_array_data());
        }

        Self::try_from_parts(
            dtype,
            ALPMetadata {
                exponents,
                encoded_dtype,
                patches_dtype: patches.map(|a| a.dtype().as_nullable()),
            },
            children.into(),
            Default::default(),
        )
    }

    pub fn encode(array: Array<'_>) -> VortexResult<OwnedArray> {
        if let Ok(parray) = PrimitiveArray::try_from(array) {
            Ok(alp_encode(&parray)?.into_array())
        } else {
            vortex_bail!("ALP can only encode primitive arrays");
        }
    }

    pub fn encoded(&self) -> Array {
        self.array()
            .child(0, &self.metadata().encoded_dtype)
            .expect("Missing encoded array")
    }

    pub fn exponents(&self) -> &Exponents {
        &self.metadata().exponents
    }

    pub fn patches(&self) -> Option<Array> {
        self.metadata().patches_dtype.as_ref().map(|dt| {
            self.array()
                .child(1, dt)
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
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("encoded", &self.encoded())?;
        if self.patches().is_some() {
            visitor.visit_child(
                "patches",
                &self.patches().expect("Expected patches to be present "),
            )?;
        }
        Ok(())
    }
}

impl ArrayStatisticsCompute for ALPArray<'_> {}

impl ArrayTrait for ALPArray<'_> {
    fn len(&self) -> usize {
        self.encoded().len()
    }
}
