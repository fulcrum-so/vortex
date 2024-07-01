use serde::{Deserialize, Serialize};
use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::compute::search_sorted::{search_sorted, SearchSortedSide};
use vortex::compute::unary::scalar_at::scalar_at;
use vortex::stats::{ArrayStatistics, ArrayStatisticsCompute};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, Canonical, IntoArrayVariant, IntoCanonical};
use vortex_error::vortex_bail;

use crate::compress::{runend_decode, runend_encode};

impl_encoding!("vortex.runend", RunEnd);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEndMetadata {
    validity: ValidityMetadata,
    ends_dtype: DType,
    offset: usize,
    length: usize,
}

impl RunEndArray {
    pub fn try_new(ends: Array, values: Array, validity: Validity) -> VortexResult<Self> {
        let length: usize = scalar_at(&ends, ends.len() - 1)?.as_ref().try_into()?;
        Self::with_offset_and_size(ends, values, validity, length, 0)
    }

    pub(crate) fn with_offset_and_size(
        ends: Array,
        values: Array,
        validity: Validity,
        length: usize,
        offset: usize,
    ) -> VortexResult<Self> {
        if values.dtype().is_nullable() == (validity == Validity::NonNullable) {
            vortex_bail!(
                "incorrect validity {:?} for dtype {}",
                validity,
                values.dtype()
            );
        }

        if !ends.statistics().compute_is_strict_sorted().unwrap_or(true) {
            vortex_bail!("Ends array must be strictly sorted",);
        }
        let dtype = values.dtype().clone();
        let metadata = RunEndMetadata {
            validity: validity.to_metadata(length)?,
            ends_dtype: ends.dtype().clone(),
            offset,
            length,
        };

        let mut children = Vec::with_capacity(3);
        children.push(ends);
        children.push(values);
        if let Some(a) = validity.into_array() {
            children.push(a)
        }

        Self::try_from_parts(dtype, metadata, children.into(), StatsSet::new())
    }

    pub fn find_physical_index(&self, index: usize) -> VortexResult<usize> {
        search_sorted(&self.ends(), index + self.offset(), SearchSortedSide::Right)
            .map(|s| s.to_index())
    }

    pub fn encode(array: Array) -> VortexResult<Self> {
        if array.encoding().id() == Primitive::ID {
            let primitive = PrimitiveArray::try_from(array)?;
            let (ends, values) = runend_encode(&primitive);
            Self::try_new(ends.into_array(), values.into_array(), primitive.validity())
        } else {
            vortex_bail!("REE can only encode primitive arrays")
        }
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(2, &Validity::DTYPE))
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.metadata().offset
    }

    #[inline]
    pub fn ends(&self) -> Array {
        self.array()
            .child(0, &self.metadata().ends_dtype)
            .expect("missing ends")
    }

    #[inline]
    pub fn values(&self) -> Array {
        self.array().child(1, self.dtype()).expect("missing values")
    }
}

impl ArrayValidity for RunEndArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl IntoCanonical for RunEndArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let pends = self.ends().into_primitive()?;
        let pvalues = self.values().into_primitive()?;
        runend_decode(&pends, &pvalues, self.validity(), self.offset(), self.len())
            .map(Canonical::Primitive)
    }
}

impl AcceptArrayVisitor for RunEndArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("ends", &self.ends())?;
        visitor.visit_child("values", &self.values())?;
        visitor.visit_validity(&self.validity())
    }
}

impl ArrayStatisticsCompute for RunEndArray {}

impl ArrayTrait for RunEndArray {
    fn len(&self) -> usize {
        self.metadata().length
    }
}

#[cfg(test)]
mod test {
    use vortex::compute::slice::slice;
    use vortex::compute::unary::scalar_at::scalar_at;
    use vortex::validity::Validity;
    use vortex::{ArrayDType, ArrayTrait, IntoArray, IntoCanonical};
    use vortex_dtype::{DType, Nullability, PType};

    use crate::RunEndArray;

    #[test]
    fn new() {
        let arr = RunEndArray::try_new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            Validity::NonNullable,
        )
        .unwrap();
        assert_eq!(arr.len(), 10);
        assert_eq!(
            arr.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );

        // 0, 1 => 1
        // 2, 3, 4 => 2
        // 5, 6, 7, 8, 9 => 3
        assert_eq!(scalar_at(arr.array(), 0).unwrap(), 1.into());
        assert_eq!(scalar_at(arr.array(), 2).unwrap(), 2.into());
        assert_eq!(scalar_at(arr.array(), 5).unwrap(), 3.into());
        assert_eq!(scalar_at(arr.array(), 9).unwrap(), 3.into());
    }

    #[test]
    fn slice_array() {
        let arr = slice(
            RunEndArray::try_new(
                vec![2u32, 5, 10].into_array(),
                vec![1i32, 2, 3].into_array(),
                Validity::NonNullable,
            )
            .unwrap()
            .array(),
            3,
            8,
        )
        .unwrap();
        assert_eq!(
            arr.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );
        assert_eq!(arr.len(), 5);

        assert_eq!(
            arr.into_canonical()
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            vec![2, 2, 3, 3, 3]
        );
    }

    #[test]
    fn flatten() {
        let arr = RunEndArray::try_new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            Validity::NonNullable,
        )
        .unwrap();

        assert_eq!(
            arr.into_canonical()
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            vec![1, 1, 2, 2, 2, 3, 3, 3, 3, 3]
        );
    }
}
