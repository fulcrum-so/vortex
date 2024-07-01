use serde::{Deserialize, Serialize};
use vortex::array::primitive::PrimitiveArray;
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, Canonical, IntoCanonical};
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

impl ALPArray {
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

        let patches_dtype = patches.as_ref().map(|a| a.dtype().as_nullable());
        let mut children = Vec::with_capacity(2);
        children.push(encoded);
        if let Some(patch) = patches {
            children.push(patch);
        }

        Self::try_from_parts(
            dtype,
            ALPMetadata {
                exponents,
                encoded_dtype,
                patches_dtype,
            },
            children.into(),
            Default::default(),
        )
    }

    pub fn encode(array: Array) -> VortexResult<Array> {
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

    #[inline]
    pub fn exponents(&self) -> Exponents {
        self.metadata().exponents
    }

    pub fn patches(&self) -> Option<Array> {
        self.metadata().patches_dtype.as_ref().map(|dt| {
            self.array()
                .child(1, dt)
                .expect("Missing patches with present metadata flag")
        })
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.dtype().try_into().unwrap()
    }
}

impl ArrayValidity for ALPArray {
    fn is_valid(&self, index: usize) -> bool {
        self.encoded().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.encoded().with_dyn(|a| a.logical_validity())
    }
}

impl IntoCanonical for ALPArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        decompress(self).map(Canonical::Primitive)
    }
}

impl AcceptArrayVisitor for ALPArray {
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

impl ArrayStatisticsCompute for ALPArray {}

impl ArrayTrait for ALPArray {
    fn len(&self) -> usize {
        self.encoded().len()
    }
}
