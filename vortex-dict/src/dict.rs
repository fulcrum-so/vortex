use serde::{Deserialize, Serialize};
use vortex::accessor::ArrayAccessor;
use vortex::array::bool::BoolArray;
use vortex::compute::scalar_at::scalar_at;
use vortex::compute::take::take;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::IntoArrayData;
use vortex::{impl_encoding, ArrayDType, ArrayFlatten, ToArrayData};
use vortex_dtype::match_each_integer_ptype;
use vortex_error::vortex_bail;

impl_encoding!("vortex.dict", Dict);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DictMetadata {
    codes_dtype: DType,
}

impl DictArray<'_> {
    pub fn try_new(codes: Array, values: Array) -> VortexResult<Self> {
        if !codes.dtype().is_unsigned_int() {
            vortex_bail!(MismatchedTypes: "unsigned int", codes.dtype());
        }
        Self::try_from_parts(
            values.dtype().clone(),
            DictMetadata {
                codes_dtype: codes.dtype().clone(),
            },
            [values.to_array_data(), codes.to_array_data()].into(),
            StatsSet::new(),
        )
    }

    #[inline]
    pub fn values(&self) -> Array {
        self.array().child(0, self.dtype()).expect("Missing values")
    }

    #[inline]
    pub fn codes(&self) -> Array {
        self.array()
            .child(1, &self.metadata().codes_dtype)
            .expect("Missing codes")
    }
}

impl ArrayFlatten for DictArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        take(&self.values(), &self.codes())?.flatten()
    }
}

impl ArrayValidity for DictArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        let values_index = scalar_at(&self.codes(), index).unwrap().try_into().unwrap();
        self.values().with_dyn(|a| a.is_valid(values_index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        if self.dtype().is_nullable() {
            let primitive_codes = self.codes().flatten_primitive().unwrap();
            match_each_integer_ptype!(primitive_codes.ptype(), |$P| {
                ArrayAccessor::<$P>::with_iterator(&primitive_codes, |iter| {
                    LogicalValidity::Array(
                        BoolArray::from(iter.flatten().map(|c| *c != 0).collect::<Vec<_>>())
                            .into_array_data(),
                    )
                })
                .unwrap()
            })
        } else {
            LogicalValidity::AllValid(self.len())
        }
    }
}

impl AcceptArrayVisitor for DictArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("values", &self.values())?;
        visitor.visit_child("codes", &self.codes())
    }
}

impl ArrayTrait for DictArray<'_> {
    fn len(&self) -> usize {
        self.codes().len()
    }
}
